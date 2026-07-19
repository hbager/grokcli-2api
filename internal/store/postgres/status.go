package postgres

import (
	"context"
	"strings"
)

type KeyStats struct {
	Total         int64 `json:"total"`
	Enabled       int64 `json:"enabled"`
	Disabled      int64 `json:"disabled"`
	TotalRequests int64 `json:"total_requests"`
}

type PoolSummary struct {
	Mode          string `json:"mode,omitempty"`
	Total         int64  `json:"total"`
	Live          int64  `json:"live"`
	Rotatable     int64  `json:"rotatable"`
	Enabled       int64  `json:"enabled"`
	InCooldown    int64  `json:"in_cooldown"`
	QuotaDisabled int64  `json:"quota_disabled"`
	ModelBlocked  int64  `json:"model_blocked"`
	Expired       int64  `json:"expired"`
	Disabled      int64  `json:"disabled"`
	Source        string `json:"source"`
}

func (c *Connector) CountAccounts(ctx context.Context) (int64, error) {
	return countQuery(ctx, c, "SELECT COUNT(*) FROM accounts")
}

func (c *Connector) CountModels(ctx context.Context, includeHidden bool) (int64, error) {
	if includeHidden {
		return countQuery(ctx, c, "SELECT COUNT(*) FROM models")
	}
	return countQuery(ctx, c, "SELECT COUNT(*) FROM models WHERE hidden = false")
}

func (c *Connector) KeyStats(ctx context.Context, legacyEnvKey bool, authRequired bool) (map[string]any, error) {
	var stats KeyStats
	err := c.Pool.QueryRow(ctx, `
		SELECT COUNT(*),
		       COUNT(*) FILTER (WHERE enabled = true),
		       COUNT(*) FILTER (WHERE enabled = false),
		       COALESCE(SUM(request_count), 0)
		FROM api_keys`,
	).Scan(&stats.Total, &stats.Enabled, &stats.Disabled, &stats.TotalRequests)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"total":          stats.Total,
		"enabled":        stats.Enabled,
		"disabled":       stats.Disabled,
		"total_requests": stats.TotalRequests,
		"auth_required":  authRequired,
		"legacy_env_key": legacyEnvKey,
	}, nil
}

// activeModelBlockSQL is shared by PoolSummary + list filters. A row is model-blocked
// only when blocked_models has at least one currently active entry.
const activeModelBlockSQL = `
EXISTS (
  SELECT 1
  FROM jsonb_each(COALESCE(ap.blocked_models, '{}'::jsonb)) AS e(model, value)
  WHERE
    (jsonb_typeof(e.value) = 'boolean' AND e.value = 'true'::jsonb)
    OR (jsonb_typeof(e.value) = 'number' AND (e.value #>> '{}')::double precision > EXTRACT(EPOCH FROM now()))
    OR (
      jsonb_typeof(e.value) = 'object'
      AND (
        (e.value ? 'until' AND COALESCE((e.value->>'until')::double precision, 0) > EXTRACT(EPOCH FROM now()))
        OR (NOT (e.value ? 'until'))
        OR ((e.value ? 'blocked') AND (e.value->>'blocked') IN ('true','1'))
      )
    )
)
`

// PoolSummary returns mutually exclusive account-pool buckets from PostgreSQL.
// Priority (matches list filters + admin tags):
//
//	expired > quota_disabled > disabled > cooldown > model_blocked > live
//
// live == rotatable; sum(live+cooldown+model_blocked+expired+quota_disabled+disabled) == total.
func (c *Connector) PoolSummary(ctx context.Context) (PoolSummary, error) {
	var summary PoolSummary
	err := c.Pool.QueryRow(ctx, `
		WITH classified AS (
		  SELECT
		    CASE
		      WHEN (a.expires_at IS NOT NULL AND a.expires_at <= now())
		        OR COALESCE(ap.pool_status, '') = 'expired'
		        THEN 'expired'
		      WHEN COALESCE(ap.disabled_for_quota, false) = true
		        OR COALESCE(ap.pool_status, '') = 'quota_disabled'
		        THEN 'quota_disabled'
		      WHEN COALESCE(ap.enabled, true) = false
		        OR COALESCE(ap.pool_status, '') = 'disabled'
		        THEN 'disabled'
		      WHEN ap.cooldown_until IS NOT NULL AND ap.cooldown_until > now()
		        THEN 'cooldown'
		      WHEN `+activeModelBlockSQL+`
		        THEN 'model_blocked'
		      ELSE 'live'
		    END AS bucket,
		    COALESCE(ap.enabled, true) AS is_enabled
		  FROM accounts a
		  LEFT JOIN account_pool ap ON ap.account_id = a.id
		)
		SELECT
		  COUNT(*)::bigint AS total,
		  COUNT(*) FILTER (WHERE is_enabled)::bigint AS enabled,
		  COUNT(*) FILTER (WHERE bucket = 'live')::bigint AS live,
		  COUNT(*) FILTER (WHERE bucket = 'live')::bigint AS rotatable,
		  COUNT(*) FILTER (WHERE bucket = 'cooldown')::bigint AS in_cooldown,
		  COUNT(*) FILTER (WHERE bucket = 'quota_disabled')::bigint AS quota_disabled,
		  COUNT(*) FILTER (WHERE bucket = 'model_blocked')::bigint AS model_blocked,
		  COUNT(*) FILTER (WHERE bucket = 'expired')::bigint AS expired,
		  COUNT(*) FILTER (WHERE bucket = 'disabled')::bigint AS disabled
		FROM classified`,
	).Scan(
		&summary.Total,
		&summary.Enabled,
		&summary.Live,
		&summary.Rotatable,
		&summary.InCooldown,
		&summary.QuotaDisabled,
		&summary.ModelBlocked,
		&summary.Expired,
		&summary.Disabled,
	)
	if err != nil {
		return summary, err
	}
	// Prefer configured account mode when present.
	if modeVal, err := c.GetSetting(ctx, "account_mode"); err == nil {
		switch v := modeVal.(type) {
		case string:
			if strings.TrimSpace(v) != "" {
				summary.Mode = strings.TrimSpace(v)
			}
		}
	}
	if summary.Mode == "" {
		summary.Mode = "round_robin"
	}
	summary.Source = "postgres"
	return summary, nil
}

// RepairFreeUsageModelBlocks moves free-usage mis-tagged "模型封禁" rows back into
// the cooldown pool: clear blocked_models, ensure cooldown_until, pool_status=cooldown.
// Returns the number of account_pool rows updated.
func (c *Connector) RepairFreeUsageModelBlocks(ctx context.Context) (int64, error) {
	if c == nil || c.Pool == nil {
		return 0, nil
	}
	// Default cool window when free-usage was blocked but cooldown already elapsed:
	// 2h matches freeUsageCooldownDuration default for a 24h rolling window.
	tag, err := c.Pool.Exec(ctx, `
		UPDATE account_pool ap
		SET
		  blocked_models = '{}'::jsonb,
		  cooldown_until = CASE
		    WHEN ap.cooldown_until IS NOT NULL AND ap.cooldown_until > now() THEN ap.cooldown_until
		    ELSE now() + interval '2 hours'
		  END,
		  cooldown_reason = COALESCE(
		    NULLIF(btrim(ap.cooldown_reason), ''),
		    NULLIF(btrim(ap.last_error), ''),
		    'free usage exhausted'
		  ),
		  cooldown_code = COALESCE(
		    NULLIF(btrim(ap.cooldown_code), ''),
		    'subscription:free-usage-exhausted'
		  ),
		  pool_status = CASE
		    WHEN COALESCE(ap.enabled, true) = false OR COALESCE(ap.disabled_for_quota, false) = true THEN 'disabled'
		    ELSE 'cooldown'
		  END,
		  updated_at = now()
		WHERE
		  -- still looks like free-usage exhaustion
		  (
		    COALESCE(ap.cooldown_code, '') ILIKE '%free-usage%'
		    OR COALESCE(ap.cooldown_reason, '') ILIKE '%free usage%'
		    OR COALESCE(ap.cooldown_reason, '') ILIKE '%额度用完%'
		    OR COALESCE(ap.cooldown_reason, '') ILIKE '%免费额度%'
		    OR COALESCE(ap.last_error, '') ILIKE '%free-usage-exhausted%'
		    OR COALESCE(ap.last_error, '') ILIKE '%included free usage%'
		    OR COALESCE(ap.last_error, '') ILIKE '%额度用完%'
		    OR COALESCE(ap.last_error, '') ILIKE '%免费额度%'
		    OR COALESCE(ap.extra #>> '{cooldown_detail,failure_class}', '') ILIKE '%free-usage%'
		    OR COALESCE(ap.extra #>> '{cooldown_detail,code}', '') ILIKE '%free-usage%'
		  )
		  AND (
		    COALESCE(ap.blocked_models, '{}'::jsonb) <> '{}'::jsonb
		    OR COALESCE(ap.pool_status, '') = 'model_blocked'
		  )
	`)
	if err != nil {
		return 0, err
	}
	n := tag.RowsAffected()
	if n > 0 {
		c.InvalidateCandidateCache()
	}
	// Heal stale pool_status=model_blocked with no active blocked_models entry.
	_, _ = c.Pool.Exec(ctx, `
		UPDATE account_pool ap
		SET
		  blocked_models = '{}'::jsonb,
		  pool_status = CASE
		    WHEN COALESCE(ap.enabled, true) = false OR COALESCE(ap.disabled_for_quota, false) = true THEN 'disabled'
		    WHEN ap.cooldown_until IS NOT NULL AND ap.cooldown_until > now() THEN 'cooldown'
		    ELSE 'normal'
		  END,
		  updated_at = now()
		WHERE COALESCE(ap.pool_status, '') = 'model_blocked'
		  AND NOT (`+activeModelBlockSQL+`)
		  AND (ap.cooldown_until IS NULL OR ap.cooldown_until <= now())
	`)
	return n, nil
}

func countQuery(ctx context.Context, c *Connector, sql string) (int64, error) {
	var count int64
	if err := c.Pool.QueryRow(ctx, sql).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
