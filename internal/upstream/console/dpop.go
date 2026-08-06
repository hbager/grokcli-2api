package console

import (
	"bytes"
	"container/list"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hm2899/grokcli-2api/internal/pool"
	"github.com/hm2899/grokcli-2api/internal/upstream/grok"
)

const (
	dpopSessionCacheLimit = 4096
	dpopRefreshSkew       = 20 * time.Second
	maxDPoPTokenLifetime  = time.Hour
)

type dpopJWK struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

type dpopSession struct {
	accessToken string
	privateKey  *ecdsa.PrivateKey
	publicJWK   dpopJWK
	expiresAt   time.Time
	// clockSkew is server time minus local time, learned from the mint Date header.
	clockSkew time.Duration
}

type dpopSessionEntry struct {
	key     string
	session dpopSession
	element *list.Element
}

type dpopLoad struct {
	done    chan struct{}
	session dpopSession
	err     error
	waiters int
}

type dpopSessionManager struct {
	mu       sync.Mutex
	sessions map[string]*dpopSessionEntry
	lru      list.List
	loads    map[string]*dpopLoad
	now      func() time.Time
}

func (m *dpopSessionManager) get(ctx context.Context, key string, fetch func(context.Context) (dpopSession, error)) (dpopSession, error) {
	for {
		m.mu.Lock()
		m.initLocked()
		if session, ok := m.cachedLocked(key); ok {
			m.mu.Unlock()
			return session, nil
		}
		if load := m.loads[key]; load != nil {
			load.waiters++
			m.mu.Unlock()
			select {
			case <-ctx.Done():
				m.mu.Lock()
				load.waiters--
				m.mu.Unlock()
				return dpopSession{}, ctx.Err()
			case <-load.done:
				m.mu.Lock()
				load.waiters--
				m.mu.Unlock()
				if (errors.Is(load.err, context.Canceled) || errors.Is(load.err, context.DeadlineExceeded)) && ctx.Err() == nil {
					continue
				}
				return load.session, load.err
			}
		}
		load := &dpopLoad{done: make(chan struct{})}
		m.loads[key] = load
		m.mu.Unlock()

		session, err := fetch(ctx)

		m.mu.Lock()
		if err == nil {
			m.storeLocked(key, session)
		}
		load.session = session
		load.err = err
		delete(m.loads, key)
		close(load.done)
		m.mu.Unlock()
		return session, err
	}
}

func (m *dpopSessionManager) invalidate(key, accessToken string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sessions == nil {
		return
	}
	entry := m.sessions[key]
	if entry == nil || (accessToken != "" && entry.session.accessToken != accessToken) {
		return
	}
	m.removeLocked(entry)
	// An in-flight replacement remains coalesced. A late 401 for an older token
	// cannot remove that replacement because accessToken is compared above.
}

func (m *dpopSessionManager) initLocked() {
	if m.sessions == nil {
		m.sessions = make(map[string]*dpopSessionEntry)
	}
	if m.loads == nil {
		m.loads = make(map[string]*dpopLoad)
	}
	if m.now == nil {
		m.now = time.Now
	}
}

func (m *dpopSessionManager) cachedLocked(key string) (dpopSession, bool) {
	entry := m.sessions[key]
	if entry == nil {
		return dpopSession{}, false
	}
	if !entry.session.expiresAt.After(m.now().UTC().Add(dpopRefreshSkew)) {
		m.removeLocked(entry)
		return dpopSession{}, false
	}
	m.lru.MoveToFront(entry.element)
	return entry.session, true
}

func (m *dpopSessionManager) storeLocked(key string, session dpopSession) {
	if entry := m.sessions[key]; entry != nil {
		entry.session = session
		m.lru.MoveToFront(entry.element)
		return
	}
	if len(m.sessions) >= dpopSessionCacheLimit {
		if oldest := m.lru.Back(); oldest != nil {
			m.removeLocked(oldest.Value.(*dpopSessionEntry))
		}
	}
	entry := &dpopSessionEntry{key: key, session: session}
	entry.element = m.lru.PushFront(entry)
	m.sessions[key] = entry
}

func (m *dpopSessionManager) removeLocked(entry *dpopSessionEntry) {
	if entry == nil {
		return
	}
	delete(m.sessions, entry.key)
	if entry.element != nil {
		m.lru.Remove(entry.element)
		entry.element = nil
	}
}

func (c *Client) dpopCacheKey(account pool.ConsoleAccount, egressIdentity ...string) string {
	identity := "direct"
	if len(egressIdentity) > 0 && strings.TrimSpace(egressIdentity[0]) != "" {
		identity = strings.TrimSpace(egressIdentity[0])
	}
	accountDigest := sha256.Sum256([]byte(account.SSO))
	egressDigest := sha256.Sum256([]byte(identity))
	return c.base() + "|" + strings.TrimSpace(account.ID) + "|" + hex.EncodeToString(accountDigest[:]) + "|" + hex.EncodeToString(egressDigest[:])
}

func (c *Client) mintDPoPSession(ctx context.Context, transport doer, account pool.ConsoleAccount) (dpopSession, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return dpopSession{}, fmt.Errorf("generate Console DPoP key: %w", err)
	}
	publicJWK := publicDPoPJWK(&privateKey.PublicKey)
	payload, err := json.Marshal(map[string]any{"jwk": publicJWK})
	if err != nil {
		return dpopSession{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.dpopTokenEndpoint(), bytes.NewReader(payload))
	if err != nil {
		return dpopSession{}, err
	}
	for key, value := range c.headers(account.SSO, account.Cookies) {
		req.Header.Set(key, value)
	}
	localBefore := time.Now().UTC()
	resp, err := transport.Do(req)
	if err != nil {
		return dpopSession{}, err
	}
	localAfter := time.Now().UTC()
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return dpopSession{}, err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return dpopSession{}, &grok.UpstreamError{
			Status:     resp.StatusCode,
			Body:       string(data),
			RetryAfter: resp.Header.Get("Retry-After"),
		}
	}
	var tokenResponse struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(data, &tokenResponse); err != nil {
		return dpopSession{}, fmt.Errorf("parse Console DPoP token response: %w", err)
	}
	tokenResponse.AccessToken = strings.TrimSpace(tokenResponse.AccessToken)
	if tokenResponse.AccessToken == "" || !strings.EqualFold(strings.TrimSpace(tokenResponse.TokenType), "DPoP") {
		return dpopSession{}, errors.New("invalid Console DPoP token response")
	}
	if tokenResponse.ExpiresIn <= 0 || tokenResponse.ExpiresIn > int64(maxDPoPTokenLifetime/time.Second) {
		return dpopSession{}, errors.New("invalid Console DPoP token lifetime")
	}
	thumbprint, err := dpopJWKThumbprint(publicJWK)
	if err != nil {
		return dpopSession{}, err
	}
	tokenExpiry, tokenThumbprint, err := parseDPoPAccessToken(tokenResponse.AccessToken)
	if err != nil {
		return dpopSession{}, err
	}
	if tokenThumbprint != thumbprint {
		return dpopSession{}, errors.New("Console DPoP token is not bound to the generated key")
	}
	now := time.Now().UTC()
	expiresAt := now.Add(time.Duration(tokenResponse.ExpiresIn) * time.Second)
	if tokenExpiry.Before(expiresAt) {
		expiresAt = tokenExpiry
	}
	if !expiresAt.After(now.Add(dpopRefreshSkew)) {
		return dpopSession{}, errors.New("Console DPoP token is expired or too close to expiry")
	}
	return dpopSession{
		accessToken: tokenResponse.AccessToken,
		privateKey:  privateKey,
		publicJWK:   publicJWK,
		expiresAt:   expiresAt,
		clockSkew:   dpopClockSkewFromDateHeader(resp.Header.Get("Date"), localBefore, localAfter),
	}, nil
}

func publicDPoPJWK(key *ecdsa.PublicKey) dpopJWK {
	return dpopJWK{
		Kty: "EC",
		Crv: "P-256",
		X:   base64.RawURLEncoding.EncodeToString(key.X.FillBytes(make([]byte, 32))),
		Y:   base64.RawURLEncoding.EncodeToString(key.Y.FillBytes(make([]byte, 32))),
	}
}

func dpopJWKThumbprint(jwk dpopJWK) (string, error) {
	canonical := struct {
		Crv string `json:"crv"`
		Kty string `json:"kty"`
		X   string `json:"x"`
		Y   string `json:"y"`
	}{Crv: jwk.Crv, Kty: jwk.Kty, X: jwk.X, Y: jwk.Y}
	data, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func parseDPoPAccessToken(value string) (time.Time, string, error) {
	parts := strings.Split(value, ".")
	if len(parts) != 3 {
		return time.Time{}, "", errors.New("invalid Console DPoP access token format")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, "", errors.New("invalid Console DPoP access token payload")
	}
	var claims struct {
		ExpiresAt int64 `json:"exp"`
		CNF       struct {
			JKT string `json:"jkt"`
		} `json:"cnf"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.ExpiresAt <= 0 || strings.TrimSpace(claims.CNF.JKT) == "" {
		return time.Time{}, "", errors.New("invalid Console DPoP access token claims")
	}
	return time.Unix(claims.ExpiresAt, 0).UTC(), strings.TrimSpace(claims.CNF.JKT), nil
}

func applyDPoPAuthorization(req *http.Request, session dpopSession) error {
	if req == nil || req.URL == nil || session.privateKey == nil || strings.TrimSpace(session.accessToken) == "" {
		return errors.New("invalid Console DPoP request parameters")
	}
	jti, err := newDPoPJTI()
	if err != nil {
		return fmt.Errorf("generate Console DPoP jti: %w", err)
	}
	digest := sha256.Sum256([]byte(session.accessToken))
	header := struct {
		Alg string  `json:"alg"`
		Typ string  `json:"typ"`
		JWK dpopJWK `json:"jwk"`
	}{Alg: "ES256", Typ: "dpop+jwt", JWK: session.publicJWK}
	claims := struct {
		JTI string `json:"jti"`
		HTM string `json:"htm"`
		HTU string `json:"htu"`
		IAT int64  `json:"iat"`
		ATH string `json:"ath"`
	}{
		JTI: jti,
		HTM: strings.ToUpper(req.Method),
		HTU: dpopHTU(req),
		IAT: dpopProofIAT(session, time.Now().UTC()),
		ATH: base64.RawURLEncoding.EncodeToString(digest[:]),
	}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return err
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return err
	}
	signingInput := base64.RawURLEncoding.EncodeToString(headerJSON) + "." + base64.RawURLEncoding.EncodeToString(claimsJSON)
	signingDigest := sha256.Sum256([]byte(signingInput))
	r, s, err := ecdsa.Sign(rand.Reader, session.privateKey, signingDigest[:])
	if err != nil {
		return fmt.Errorf("sign Console DPoP proof: %w", err)
	}
	signature := make([]byte, 64)
	r.FillBytes(signature[:32])
	s.FillBytes(signature[32:])
	proof := signingInput + "." + base64.RawURLEncoding.EncodeToString(signature)
	req.Header.Set("Authorization", "DPoP "+session.accessToken)
	req.Header.Set("DPoP", proof)
	return nil
}

func newDPoPJTI() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]), nil
}

func dpopProofIAT(session dpopSession, localNow time.Time) int64 {
	if localNow.IsZero() {
		localNow = time.Now().UTC()
	}
	return localNow.Add(session.clockSkew).UTC().Unix()
}

// dpopClockSkewFromDateHeader follows the Console frontend: estimate local time
// at receipt from the request window midpoint, then round skew to whole seconds.
func dpopClockSkewFromDateHeader(dateHeader string, localBefore, localAfter time.Time) time.Duration {
	dateHeader = strings.TrimSpace(dateHeader)
	if dateHeader == "" {
		return 0
	}
	serverTime, err := http.ParseTime(dateHeader)
	if err != nil {
		return 0
	}
	if localAfter.IsZero() || localAfter.Before(localBefore) {
		localAfter = localBefore
	}
	if localBefore.IsZero() {
		localBefore = time.Now().UTC()
		localAfter = localBefore
	}
	localMid := localBefore.Add(localAfter.Sub(localBefore) / 2)
	delta := serverTime.UTC().Sub(localMid.UTC())
	// time.Duration.Round rounds ties away from zero, matching the frontend's
	// intended whole-second correction on both sides of the boundary.
	return delta.Round(time.Second)
}

func dpopHTU(req *http.Request) string {
	path := req.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	return req.URL.Scheme + "://" + req.URL.Host + path
}
