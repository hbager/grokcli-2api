package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hm2899/grokcli-2api/internal/admin"
	"github.com/hm2899/grokcli-2api/internal/auth"
	"github.com/hm2899/grokcli-2api/internal/buildinfo"
	"github.com/hm2899/grokcli-2api/internal/config"
	"github.com/hm2899/grokcli-2api/internal/maintainer"
	"github.com/hm2899/grokcli-2api/internal/modelhealth"
	"github.com/hm2899/grokcli-2api/internal/models"
	"github.com/hm2899/grokcli-2api/internal/protocol/historycompact"
	"github.com/hm2899/grokcli-2api/internal/quota"
	appruntime "github.com/hm2899/grokcli-2api/internal/runtime"
	"github.com/hm2899/grokcli-2api/internal/server"
	"github.com/hm2899/grokcli-2api/internal/store/postgres"
	"github.com/hm2899/grokcli-2api/internal/store/redis"
	"github.com/hm2899/grokcli-2api/internal/upstream/console"
	"github.com/hm2899/grokcli-2api/internal/upstream/grok"
	"github.com/hm2899/grokcli-2api/internal/upstream/oidc"
	"github.com/hm2899/grokcli-2api/internal/upstream/outboundproxy"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(2)
	}

	stop, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	readiness := appruntime.StartReadinessProbe(stop, cfg, "migrations")
	var store *postgres.Connector
	var adminSessions admin.SessionVerifier
	var redisClient *redis.Client
	var leader *redis.Leader
	var maintSvc *maintainer.Service
	var healthSvc *modelhealth.Service

	if cfg.GoAdminRead || cfg.GoAdminWrite || cfg.GoChat || cfg.GoMessages || cfg.GoResponses || cfg.GoMaintainer {
		redisClient = redis.New(cfg.RedisURL, cfg.RedisPrefix)
	}
	if cfg.GoAdminRead || cfg.GoAdminWrite {
		adminSessions = redisClient
	}
	if redisClient != nil {
		leader = redis.NewLeader(redisClient, cfg.MaintainerLeader, cfg.Workers, cfg.MaintainerLeaderTTL, cfg.MaintainerLeaderRenew)
	}

	if cfg.GoPublicRead || cfg.GoAdminRead || cfg.GoAdminWrite || cfg.GoChat || cfg.GoMessages || cfg.GoResponses || cfg.GoMaintainer {
		ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
		opened, err := postgres.Open(ctx, cfg.DatabaseURL)
		done()
		if err != nil {
			slog.Warn("read-only store unavailable; staged routes remain fail-closed", "error", err)
		} else {
			store = opened
			defer store.Close()
			if adminSessions == nil {
				adminSessions = store
			}
			// One-shot: free-usage mis-tags (模型封禁) → durable cooldown only.
			repairCtx, repairDone := context.WithTimeout(context.Background(), 30*time.Second)
			if n, err := store.RepairFreeUsageModelBlocks(repairCtx); err != nil {
				slog.Warn("repair free-usage model blocks failed", "error", err)
			} else if n > 0 {
				slog.Info("repaired free-usage model blocks into cooldown", "accounts", n)
			}
			repairDone()
		}
	}

	// Live config snapshots let admin settings writes hot-reload without races.
	runtimeCfg := config.NewRuntimeConfig(cfg)
	var bootSettings map[string]any
	if store != nil {
		if settings, err := store.PublicSettings(context.Background()); err == nil {
			bootSettings = settings
			runtimeCfg.ApplyStoreSettings(settings)
			opts := historycompact.ConfigureOpts{}
			if v, ok := settings["history_compact_enabled"].(bool); ok {
				opts.Enabled = &v
			}
			if v, ok := intSetting(settings["history_compact_auto_chars"]); ok {
				opts.AutoChars = &v
			}
			if v, ok := intSetting(settings["history_keep_tool_rounds"]); ok {
				opts.KeepToolRounds = &v
			}
			if v, ok := intSetting(settings["history_max_tool_result_chars"]); ok {
				opts.MaxToolResultChars = &v
			}
			historycompact.ConfigureFull(opts)
			loaded := runtimeCfg.Load()
			snap := historycompact.Snapshot()
			slog.Info("loaded durable settings into runtime config",
				"default_model", loaded.DefaultModel,
				"sse_keepalive", loaded.SSEKeepalive.String(),
				"outbound_max_tools", loaded.OutboundMaxTools,
				"outbound_proxy_enabled", loaded.OutboundProxyConfigured && loaded.OutboundProxyEnabled,
				"history_compact_enabled", snap["enabled"],
				"history_compact_auto_chars", snap["auto_chars"],
			)
		} else {
			slog.Warn("failed to load durable settings at boot", "error", err)
		}
	}
	proxySelector := outboundproxy.New(runtimeCfg.Load)
	chatClient := &grok.Client{BaseURL: cfg.UpstreamBase, HTTP: grok.NewHTTPClient(proxySelector.Proxy)}
	consoleClient := &console.Client{HTTP: chatClient.HTTP, Proxy: proxySelector.Proxy}
	quotaSvc := quota.New(store, cfg.UpstreamBase)
	quotaSvc.SetProxy(proxySelector.Proxy)

	if store != nil {
		oidcTransport := http.DefaultTransport.(*http.Transport).Clone()
		oidcTransport.Proxy = proxySelector.Proxy
		oidcClient := &oidc.Client{HTTP: &http.Client{Timeout: 30 * time.Second, Transport: oidcTransport}}
		maintSvc = maintainer.New(store, redisClient, oidcClient)
		healthSvc = modelhealth.New(store, redisClient, cfg.UpstreamBase, []string{runtimeCfg.Load().DefaultModel})
		healthSvc.SetProxy(proxySelector.Proxy)
		healthSvc.SetConsoleClient(consoleClient)
		if bootSettings != nil {
			var intervalSec float64
			switch v := bootSettings["model_health_interval_sec"].(type) {
			case float64:
				intervalSec = v
			case int:
				intervalSec = float64(v)
			case int64:
				intervalSec = float64(v)
			}
			batch, _ := intSetting(bootSettings["model_health_probe_batch"])
			workers, _ := intSetting(bootSettings["model_health_probe_workers"])
			healthSvc.Configure(intervalSec, batch, workers)
		}
		if leader != nil {
			maintSvc.IsLeader = leader.IsLeader
			healthSvc.IsLeader = leader.IsLeader
		} else {
			maintSvc.IsLeader = func() bool { return true }
			healthSvc.IsLeader = func() bool { return true }
		}
		maintSvc.Enabled = func() bool {
			if !cfg.GoMaintainer {
				return false
			}
			settings, err := store.PublicSettings(context.Background())
			if err != nil {
				return true
			}
			v, _ := settings["token_maintain_enabled"].(bool)
			return v
		}
		healthSvc.Enabled = func() bool {
			if !cfg.GoMaintainer {
				return false
			}
			settings, err := store.PublicSettings(context.Background())
			if err != nil {
				return true
			}
			v, _ := settings["model_health_enabled"].(bool)
			return v
		}
	}

	if cfg.GoMaintainer {
		if leader != nil {
			if leader.ShouldStartMaintainers(context.Background()) {
				slog.Info("go maintainer leadership acquired", "leader_id", leader.Status(context.Background())["leader_id"])
			} else {
				slog.Info("go maintainer waiting for leadership")
			}
		}
		if maintSvc != nil {
			maintSvc.Start()
		}
		if healthSvc != nil {
			healthSvc.Start()
		}
	}

	handler := server.NewMux(server.Options{
		Ready:             readiness.Ready,
		Reason:            readiness.Reason,
		StaticDir:         cfg.StaticDir,
		PublicReadEnabled: cfg.GoPublicRead,
		AdminReadEnabled:  cfg.GoAdminRead,
		AdminWriteEnabled: cfg.GoAdminWrite,
		ChatEnabled:       cfg.GoChat,
		MessagesEnabled:   cfg.GoMessages,
		ResponsesEnabled:  cfg.GoResponses,
		APIKeys:           auth.NewAPIKeyVerifier(cfg, store),
		Models:            models.NewDynamicCatalog(runtimeCfg.Load, store),
		Store:             store,
		AdminSessions:     adminSessions,
		PickObserver:      redis.NewPickObserver(redisClient),
		AffinityStore:     redis.NewChatAffinity(redisClient, 24*time.Hour),
		Upstream:          chatClient,
		Console:           consoleClient,
		Redis:             redisClient,
		Leader:            leader,
		Maintainer:        maintSvc,
		ModelHealth:       healthSvc,
		Quota:             quotaSvc,
		Config:            cfg,
		RuntimeConfig:     runtimeCfg,
		RegistrationURL:   cfg.RegistrationServiceURL,
		RegistrationToken: cfg.RegistrationToken,
	})
	httpServer := &http.Server{
		Addr:              cfg.Address(),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       0,                 // 不设置 ReadTimeout，避免影响流式响应
		WriteTimeout:      0,                 // 不设置 WriteTimeout，流式响应需要持续写入
		IdleTimeout:       120 * time.Second, // 增加空闲超时，支持长连接
		MaxHeaderBytes:    1 << 20,           // 1MB header limit
	}

	go func() {
		<-stop.Done()
		ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		if maintSvc != nil {
			maintSvc.Stop()
		}
		if healthSvc != nil {
			healthSvc.Stop()
		}
		if leader != nil {
			leader.Release(ctx)
		}
		if err := httpServer.Shutdown(ctx); err != nil {
			slog.Error("graceful shutdown failed", "error", err)
		}
	}()

	slog.Info("starting migration-safe Go probe server",
		"address", cfg.Address(),
		"implementation", buildinfo.Implementation,
		"version", buildinfo.Version,
	)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func intSetting(value any) (int, bool) {
	switch v := value.(type) {
	case float64:
		return int(v), true
	case int:
		return v, true
	case int64:
		return int(v), true
	default:
		return 0, false
	}
}
