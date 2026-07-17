# 公網匿名端點資訊最小化 Implementation Plan

> **For agentic workers:** Execute each task at its recorded risk level. Use `subagent-driven-development` only for strict plans with independently review-worthy tasks; otherwise use `executing-plans` inline.

**Goal:** 將公網匿名狀態與健康檢查限制為最小回應，阻止第三方金鑰、帳號、代理與基礎設施資訊外洩，同時維持登入、探活與管理設定功能。

**Architecture:** 匿名 `/admin/api/status` 與 `/health` 採固定允許清單輸出，不再序列化內部狀態物件；完整管理資料只由既有管理員驗證端點提供，且設定物件一律使用遮罩後秘密。診斷介面採明確保護：`/metrics` 必須驗證有效客戶端 API Key，FastAPI 文件預設停用並可透過單一環境變數明確啟用。

**Tech Stack:** Python 3.12、FastAPI、Pydantic v2、現有 assert-based regression scripts、原生 `unittest.mock`

## Global Constraints

- 匿名 `GET /admin/api/status` 僅回 `ok`、`setup_needed`、`version`。
- 匿名 `GET /health` 成功僅回 `status`、`version`；失敗維持 HTTP 503 且不回傳原始例外。
- 第三方 API Key、token、password、proxy password 不得出現在匿名回應。
- 管理設定 GET 與更新回應只使用遮罩值及 `*_set`／`has_*` 旗標；遮罩輸入不得覆寫既有秘密。
- `/metrics` 必須提供真正有效的既有客戶端 API Key，不得因 `GROK2API_REQUIRE_API_KEY=off/auto` 而匿名放行。
- `/docs`、`/redoc`、`/openapi.json` 預設停用，只有明確設定 `GROK2API_ENABLE_DOCS=1` 才啟用。
- CORS 不得使用萬用來源搭配 credentials；預設不允許跨來源，僅透過 `GROK2API_CORS_ORIGINS` 明確列出來源。
- HTTPS 或可信反向代理回報 HTTPS 時，管理 cookie 必須帶 `Secure`；本機 HTTP 仍須可用。
- 不重做管理 session 架構，不更動 `/v1` API Key 驗證語義，不新增依賴。
- 所有修改維持 UTF-8 與 LF，結束時執行 `git diff --check`。

---

## File Structure

- Modify: `grok2api/admin/admin_routes.py` — 匿名 status 契約、管理 cookie 安全屬性、設定路由的受保護邊界。
- Modify: `grok2api/admin/settings_store.py` — 管理設定回應統一遮罩秘密，保留內部執行時讀取完整秘密的介面。
- Modify: `grok2api/config.py` — 文件開關與明確 CORS 來源設定。
- Modify: `grok2api/app.py` — FastAPI 文件設定、CORS、最小 health、metrics 強制 Key 驗證。
- Modify: `static/js/auth.js` — 登入頁不再依賴匿名 health/status 的資料庫與帳號細節。
- Modify: `static/admin/login.html` — 登入畫面文案改為最小服務可用性，不宣稱匿名檢查資料庫連線。
- Regenerate: `static/dist/auth.<hash>.js`, `static/dist/manifest.json`, `static/admin/*.html` — 由既有 `scripts/build_admin_assets.py` 產生，禁止手動維護 hash。
- Create: `scripts/_test_public_endpoint_security.py` — 無外部服務的安全回歸測試，檢查端點契約、遮罩相容性、CORS、cookie、metrics 與文件開關。
- Modify: `README.md` — 記錄匿名端點契約、metrics 驗證、文件與 CORS 開關，以及已外洩秘密需自行輪替。

---

### Task 1: 鎖定匿名端點與秘密遮罩契約

**Why:** 先以可重現測試證明現行匿名回應會外洩，並鎖定修正後允許的欄位集合；後續所有安全修改依賴此契約。

**Risk:** strict — 涉及未授權資訊揭露、憑證與信任邊界。

**Files:**
- Create: `scripts/_test_public_endpoint_security.py`
- Inspect: `grok2api/admin/admin_routes.py:455-706`
- Inspect: `grok2api/admin/settings_store.py:2287-2354,3011-3021,3304-3379`
- Inspect: `grok2api/app.py:673-690,3777-3860`

**Interfaces:**
- Consumes: FastAPI `app`、`admin_status(request)`、`health()`、`get_public_settings()`、`get_registration_config()`、`get_outbound_proxy_config()`。
- Produces: 單一可直接執行的 regression script；退出碼 0 代表所有安全契約成立。

**Acceptance:**
- 使用 monkeypatch／`unittest.mock` 注入唯一測試秘密，不接觸真實 PostgreSQL、Redis、帳號或公網站點。
- 驗證匿名 status 的 JSON key 集合恰為 `{ok, setup_needed, version}`，且遞迴序列化內容不含注入秘密。
- 驗證 health 成功 key 集合恰為 `{status, version}`。
- 模擬 health 內部檢查失敗時驗證 HTTP 503，只回通用狀態與版本，不含原始例外文字。
- 驗證 `get_public_settings()` 對註冊與出站代理密碼只回遮罩／存在旗標，且測試秘密不在序列化結果。
- 驗證遮罩後 registration config 經 `resolve_registration_inputs()` 與 `set_registration_config()` 時不會把遮罩字串當成新秘密。
- 測試在實作前至少對現行外洩行為失敗，不能以弱化 assertion 讓舊行為通過。

**Constraints:**
- 遵循 `test-driven-development` 的 RED-GREEN 流程。
- 不讀寫 `.env`、`data/` 或外部供應商。
- 不把任何真實金鑰寫進測試、log 或文件。

**Proof:**
- Focused verification: `python scripts/_test_public_endpoint_security.py` — 修正前因匿名欄位／測試秘密外洩而失敗；完成 Task 2–4 後退出碼 0。
- Task boundary: `python -m compileall -q grok2api scripts/_test_public_endpoint_security.py` — 無語法錯誤。
- Strict risk evidence: 測試輸出逐項列出 status、health、settings secrecy 契約，且不列印秘密值。

---

### Task 2: 最小化 `/admin/api/status` 並遮罩管理設定回應

**Why:** 直接修正已證實的匿名註冊金鑰外洩根因，同時保留登入頁判斷首次設定與已登入管理台功能。

**Risk:** strict — 修改管理身份驗證邊界與秘密序列化。

**Files:**
- Modify: `grok2api/admin/admin_routes.py:613-706,764-812,2070-2118,4589-4640`
- Modify: `grok2api/admin/settings_store.py:2287-2354,3011-3021,3304-3379`
- Test: `scripts/_test_public_endpoint_security.py`

**Interfaces:**
- Consumes: `is_setup_needed()`、`APP_VERSION` 相容匯入、`get_public_settings()`、`get_registration_config(include_secrets=...)`、`get_outbound_proxy_config(include_secrets=...)`。
- Produces: `GET /admin/api/status -> {ok: bool, setup_needed: bool, version: str}`；`get_public_settings()` 只產生可安全傳給管理前端的遮罩設定。

**Acceptance:**
- `admin_status()` 不再查詢或回傳帳號、pool、store、settings、registration、usage、upstream、host/port、維護器或模型健康資料。
- 已登入 `/admin/api/dashboard`、`GET/PUT/PATCH /admin/api/settings` 仍回傳管理台需要的非秘密資料，但 registration 與 outbound proxy 的秘密只以遮罩及存在旗標表示。
- `get_public_settings()` 使用 `get_registration_config(include_secrets=False)` 與 `get_outbound_proxy_config(include_secrets=False)`。
- `get_outbound_proxy_config(include_secrets=False)` 明確提供 `proxy_password_set`，空密碼則為 false；不回完整密碼。
- `GET /admin/api/accounts/register-email/config` 改回遮罩設定；PUT 與啟動註冊仍能沿用既有秘密，遮罩 placeholder 不得落庫或送到供應商。
- 管理頁需要編輯新秘密時仍可輸入新值；空值與清除語義維持既有行為。
- 公開 status 不包含任何內部路徑，例如 `/app/data/auth.json`。

**Constraints:**
- 完整秘密只允許在後端內部執行路徑呼叫 `include_secrets=True`；HTTP GET/PUT/PATCH 回應不可使用完整秘密。
- 不改動 `set_registration_config()` 的明確清除語義。
- 不以黑名單從大型 payload 刪欄位；匿名 status 必須直接建立三欄 allowlist。

**Proof:**
- Focused verification: `python scripts/_test_public_endpoint_security.py` — status 與 settings 契約通過。
- Task boundary: `python -m compileall -q grok2api/admin` — 無語法錯誤。
- Strict risk evidence: 以注入秘密遞迴掃描 status、dashboard/settings、registration config 回應，完整秘密皆不存在；遮罩保留測試仍成功。

---

### Task 3: 最小化 `/health` 並保護診斷介面

**Why:** 關閉第二條匿名憑證／帳號／拓撲外洩路徑，並阻止 `/metrics` 與 API 文件成為替代枚舉面。

**Risk:** strict — 影響公網探活、診斷授權與服務可用性。

**Files:**
- Modify: `grok2api/config.py:35-67`
- Modify: `grok2api/app.py:673-690,3777-3860`
- Test: `scripts/_test_public_endpoint_security.py`

**Interfaces:**
- Consumes: `APP_VERSION`、`apikeys.verify_key(raw)`、新設定 `ENABLE_DOCS: bool` 與 `CORS_ORIGINS: list[str]`。
- Produces: `GET /health -> {status: "ok", version: str}` 或 HTTP 503 同形通用回應；`GET /metrics` 需 Bearer 或 `x-api-key` 的有效 Key；FastAPI 文件 URL 由 `ENABLE_DOCS` 控制。

**Acceptance:**
- health 不回 `email`、`expires_at`、`auth_key`、upstream、account counts、registration、store、leader、maintainer、model health 或 affinity。
- readiness 只執行有界且必要的內部檢查；任何失敗轉成 503 `{status: "unavailable", version: APP_VERSION}`，不包含例外、路徑或元件名稱。
- metrics 缺少或使用無效 Key 時回 401；有效 managed 或 legacy env Key 才回 Prometheus 文字。
- metrics 的強制驗證不呼叫 `auth_required()`，因此 Key 模式關閉也不會匿名放行。
- FastAPI 建構時 `GROK2API_ENABLE_DOCS` 未設或 false，`docs_url`、`redoc_url`、`openapi_url` 都為 `None`；true 時恢復 `/docs`、`/redoc`、`/openapi.json`。
- 不改動 compose healthcheck URL 或成功碼語義。

**Constraints:**
- health 不依賴外部 xAI/OIDC 網路，避免公網供應商抖動讓容器誤判死亡。
- metrics 可重用 header 解析規則，但必須直接呼叫 `apikeys.verify_key()`。
- 不新增第二套 metrics secret。

**Proof:**
- Focused verification: `python scripts/_test_public_endpoint_security.py` — health、metrics 與 docs 契約通過。
- Task boundary: `python -m compileall -q grok2api/app.py grok2api/config.py` — 無語法錯誤。
- Strict risk evidence: 匿名 TestClient 請求 health 僅兩欄、metrics 為 401、三個文件 URL 為 404；有效測試 Key 可讀 metrics。

---

### Task 4: 收緊 CORS 與管理 Cookie

**Why:** 匿名資訊最小化後仍需消除萬用 CORS 與非 Secure 管理 cookie，避免跨來源讀取和 HTTPS session 保護落差。

**Risk:** strict — 涉及瀏覽器同源政策與管理 session cookie。

**Files:**
- Modify: `grok2api/config.py:35-67`
- Modify: `grok2api/app.py:684-690`
- Modify: `grok2api/admin/admin_routes.py:461-475,709-733`
- Test: `scripts/_test_public_endpoint_security.py`

**Interfaces:**
- Consumes: `GROK2API_CORS_ORIGINS` 逗號分隔來源、登入／setup 的 `Request`、`X-Forwarded-Proto` 第一個值。
- Produces: 明確來源 CORS middleware；`_set_admin_cookie(response, token, request)` 依外部協定設定 `Secure`。

**Acceptance:**
- 未設定 CORS 來源時不回 `Access-Control-Allow-Origin`；設定多個明確來源時只回匹配來源，且 credentials 僅用於該 allowlist。
- 不接受 `*` 與 credentials 同時啟用；`GROK2API_CORS_ORIGINS` 含 `*` 時應在設定載入階段拋出清楚的 `ValueError` 並拒絕啟動。
- 直接 HTTPS 與 `X-Forwarded-Proto: https` 的 login/setup cookie 包含 `Secure; HttpOnly; SameSite=Lax`。
- 本機 HTTP cookie 保持 `HttpOnly; SameSite=Lax` 且不強制 `Secure`，確保本機開發仍可登入。
- 只信任部署反向代理提供的 forwarding header；README 明確提醒代理應覆寫而非附加客戶端 `X-Forwarded-Proto`。

**Constraints:**
- 不移除 HttpOnly、SameSite 或 cookie session fallback。
- 不在本次移除前端 localStorage token，以免擴張 session 遷移範圍。
- 所有 CORS origin 必須為完整 origin，不接受路徑。

**Proof:**
- Focused verification: `python scripts/_test_public_endpoint_security.py` — CORS 與 cookie header assertions 通過。
- Task boundary: `python -m compileall -q grok2api` — 無語法錯誤。
- Strict risk evidence: 測試分別覆蓋 HTTP、HTTPS、forwarded HTTPS、允許來源與惡意來源。

---

### Task 5: 更新登入前端以符合最小匿名資料

**Why:** status/health 不再提供 store、pool 與帳號資料後，登入頁必須停止依賴這些欄位，避免顯示誤導性的「資料庫異常」或連線清單。

**Risk:** standard — 本地前端行為變更，受安全端點契約約束但不新增信任決策。

**Files:**
- Modify: `static/js/auth.js:94-197`
- Modify: `static/admin/login.html:122-170`
- Regenerate: `static/dist/auth.<hash>.js`
- Regenerate: `static/dist/manifest.json`
- Regenerate references: `static/admin/*.html`
- Use: `scripts/build_admin_assets.py`

**Interfaces:**
- Consumes: 匿名 status `{ok, setup_needed, version}` 與 health `{status, version}`。
- Produces: 登入頁只顯示服務可達、首次設定或輸入密碼，不再呈現 PostgreSQL、Redis、帳號池或 Key 狀態。

**Acceptance:**
- `initLoginPage()` 不再呼叫 `renderConnList()` 解讀匿名 store/accounts/pool。
- 登入頁仍可分辨 setup 與 login，服務不可達時仍顯示錯誤並允許重試。
- health 請求只用於可達性或移除其重複請求；不得期待內部欄位。
- 登入成功、cookie/session 探測與 redirect 邏輯不變。
- 執行資產建置後所有管理 HTML 指向 manifest 中的新 hash，manifest 與實際檔案一致；既有未引用 hash 檔因刪除需另行確認，可保留但不得被 HTML 引用。

**Constraints:**
- 只改來源檔，再執行既有 builder；不得手動編輯 hash bundle。
- 不改視覺樣式或其他管理頁。
- 不刪除既有 hash 檔，除非另行取得刪除確認；未引用舊檔可留待既有清理流程。

**Proof:**
- Focused verification: `python scripts/build_admin_assets.py` — 輸出 OK，產生新 manifest 與 HTML 引用。
- Task boundary: `python -c "import json, pathlib; m=json.loads(pathlib.Path('static/dist/manifest.json').read_text()); assert all((pathlib.Path('static') / v.removeprefix('/static/')).is_file() for v in m.values())"` — 所有 manifest 資產存在。
- Standard evidence: 瀏覽器或本機服務實測未登入 login 頁可載入、setup/login 分支正確、Network 的 status/health 無秘密。

---

### Task 6: 文件、完整驗證與公網部署檢查

**Why:** 安全修正只有在部署後匿名實測與秘密輪替完成才真正可用；文件必須讓部署者知道新診斷介面與反向代理要求。

**Risk:** strict — 生產安全驗證與已外洩憑證處置。

**Files:**
- Modify: `README.md`（安全／部署段落）
- Verify: `docs/superpowers/specs/2026-07-17-public-endpoint-data-minimization-design.md`
- Verify: all modified files

**Interfaces:**
- Consumes: `GROK2API_ENABLE_DOCS`、`GROK2API_CORS_ORIGINS`、既有 client API Key header 格式。
- Produces: 可執行的部署說明與匿名端點驗證清單。

**Acceptance:**
- README 記錄 status/health 的最小 JSON、metrics Key 要求、文件預設關閉、CORS 明確 allowlist 與 HTTPS proxy forwarding 要求。
- README 明確寫出：任何曾出現在匿名回應、瀏覽器截圖或對話中的秘密必須在供應商端撤銷並重建，部署程式碼不能取代輪替。
- 所有既有 `scripts/_test_*.py` 與新安全測試通過；若既有 script 依賴外部服務或專用資料而不能執行，逐項記錄原因，不宣稱完整通過。
- `git diff --check` 無錯；touched text files 維持 LF。
- 本機啟動後無 cookie 的 status/health/metrics/docs 實測符合契約；有管理 session 時 dashboard/settings 仍可用且不回完整第三方秘密。
- 公網部署前確認沒有舊容器／舊進程；部署屬共享系統變更，執行前必須另取得使用者確認。
- 部署後從外部無 cookie 請求 `https://gapi.hbager.de/admin/api/status`、`/health`、`/metrics`、`/openapi.json`，只記錄狀態碼與欄位名稱，不輸出秘密內容。

**Constraints:**
- 不自動撤銷第三方金鑰、不自動重啟或部署公網服務，除非使用者明確授權。
- 不把 `.env`、憑證、API Key 或完整生產回應加入 git diff 或測試輸出。
- 完成程式修改不等於完成生產修復；未部署／未輪替時必須標示為待辦。

**Proof:**
- Focused verification: `python scripts/_test_public_endpoint_security.py` — 安全回歸全部通過。
- Task boundary: `for f in scripts/_test_*.py; do python "$f" || exit 1; done` — 可在本機執行的既有 regression scripts 全通過。
- Task boundary: `git diff --check && git status --short` — 無 whitespace 錯誤並列出預期檔案。
- Strict risk evidence: 本機匿名 HTTP 契約檢查通過；公網檢查在使用者授權部署後才執行，並驗證已洩漏供應商 Key 已輪替。
