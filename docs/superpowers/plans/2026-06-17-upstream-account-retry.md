# 上游錯誤自動換號重試 Implementation Plan

> **For agentic workers:** Execute each task at its recorded risk level. Use `subagent-driven-development` only for strict plans with independently review-worthy tasks; otherwise use `executing-plans` inline.

**Goal:** 新增可在系統設定調整的上游自動換號重試次數 `N`，讓 Go 與 Python 的三種 LLM API 在所有上游錯誤或空回覆時最多嘗試 `N+1` 個不同帳號。

**Architecture:** 設定層以 `upstream_retry_count` 儲存額外重試次數，並將舊 `max_failover_attempts` 總嘗試數換算為 `N`。Go 由每個請求即時解析有效設定後，把 `N+1` 傳入共用 `ChatService`；Python 由帳號池用相同換算建立候選鏈。兩個執行路徑都只在尚未提交有效模型輸出時換號，耗盡候選後由各協議回傳標準化 `503 / upstream_error / all_accounts_failed`。

**Tech Stack:** Go 1.22+、Python 3.12、FastAPI、httpx、PostgreSQL `app_settings`、原生 Go `testing/httptest`、現有 standalone Python regression scripts、原生 HTML/JavaScript 管理頁

## Global Constraints

- `upstream_retry_count` 預設為 `3`，合法範圍為 `0–63`，總嘗試上限固定為 `upstream_retry_count + 1`。
- 同一用戶請求不重複使用已失敗帳號；可用帳號不足時只嘗試所有不同帳號。
- 新設定優先；只有新設定不存在時，才以 `max(0, max_failover_attempts - 1)` 讀取舊設定。
- 管理頁只寫入 `upstream_retry_count`；舊 `max_failover_attempts` 僅保留讀取相容。
- 所有已送往 LLM 上游的非 `2xx`、連線錯誤、逾時、提前斷線、解析錯誤、空 Body、非模型回應及空模型輸出都可觸發換號。
- `output_tokens=0` 或 `completion_tokens=0` 只有在文字、推理與工具呼叫也全部不存在時才算空回覆。
- 客戶端取消、本地輸入驗證失敗及本地 API Key 驗證失敗不得換號。
- SSE keepalive、角色欄位與協議外殼不算有效模型輸出；有效文字、推理或工具呼叫一旦寫給客戶端，後續不得換號。
- 全部帳號失敗時不得洩漏 Token、帳號憑證或完整敏感上游回應。
- 不新增第三方依賴，不重構與此功能無關的代理、帳號池或管理頁程式。
- Windows + Git Bash 環境下使用 `python` 與既有專用工具；所有修改文字檔維持 UTF-8/LF。

---

### Task 1: 建立新舊設定的單一有效語義

**Why:** Go、Python、管理 API 與帳號池必須先共享相同的 `N` 與 `N+1` 定義，後續重試核心才能避免 off-by-one 及升級行為改變。

**Risk:** strict — 持久化設定相容與跨 Go/Python 執行路徑的公開管理 API 行為。

**Files:**
- Modify: `grok2api/admin/settings_store.py:62-106,2935-3004,3252-3343,3387-3494`
- Modify: `grok2api/admin/admin_routes.py:122-176`
- Modify: `grok2api/pool/account_pool.py:462-535,2967-2974`
- Modify: `internal/store/postgres/settings.go:14-80,224-398,418-428`
- Create: `internal/store/postgres/settings_retry_test.go`
- Create: `scripts/_test_upstream_account_retry.py`

**Interfaces:**
- Consumes: Python `_get_setting_value(key, default)`, `_set_setting_value(key, value)`, `account_pool.try_acquire_sequence(..., max_attempts=None)`；Go `Connector.GetSetting`, `Connector.PublicSettings`, `Connector.UpdateRuntimeSettings`。
- Produces: Python `account_pool.upstream_retry_count() -> int`，固定回傳 `0–63`；既有 `account_pool.max_failover_attempts() -> int` 改為回傳有效總嘗試數 `upstream_retry_count()+1`，供舊呼叫者相容使用。
- Produces: Go `Connector.UpstreamRetryCount(context.Context) (int, error)`，依「新值 → 舊值減一 → 預設 3」解析；`PublicSettings` 對外提供 `upstream_retry_count`，並可提供唯讀 `max_failover_attempts=upstream_retry_count+1` 相容值。
- Produces: 管理 PATCH/PUT 接受 `upstream_retry_count` 整數 `0–63`；Python Pydantic 欄位與 Go `fieldSpec` 使用相同範圍。

**Acceptance:**
- 新值不存在、舊值為 `4` 時，有效 `N=3`、總嘗試數為 `4`。
- 新值為 `0` 時，即使舊值存在也以 `0` 為準。
- 兩個值都不存在時，有效 `N=3`。
- 管理 API 寫入新值後，下一個請求可讀到新值，不需重啟；Python 的短期 policy cache 必須在寫入時失效，不能讓已儲存的新值繼續等待舊 TTL。
- `try_acquire_sequence` 在未傳 `max_attempts` 時最多回傳 `N+1` 個不同帳號；顯式 `max_attempts` 仍優先，供既有測試與內部呼叫使用。
- 舊 `max_failover_attempts` 不再是管理 API 的可寫欄位，但保留讀取相容，不刪除舊資料。
- 超出範圍的資料庫舊資料在讀取時夾到合法範圍；管理 API 沿用現有設定策略將數值夾到 `0–63`，非數字回傳驗證錯誤。

**Constraints:**
- 不新增資料庫 migration；`app_settings` 已支援任意鍵。
- Python `_PG_SCALAR_KEYS` 必須加入 `upstream_retry_count`，確保 PostgreSQL、多 worker 與 file backend 都可持久化。
- 不在新設定存在時回寫或覆蓋舊值，避免不必要資料遷移副作用。

**Proof:**
- Focused verification: `go test ./internal/store/postgres -run 'UpstreamRetry|RuntimeSettings' -count=1` — 新舊值優先序、預設值及範圍測試通過。
- Focused verification: `python -B scripts/_test_upstream_account_retry.py` — 設定相容與候選鏈長度案例通過。
- Task boundary: `go test ./internal/store/postgres ./internal/pool -count=1` — 設定與帳號挑選既有測試無回歸。
- Strict risk evidence: 測試明確證明舊值 `4` 升級後仍只產生四次總嘗試，且新值 `0` 不被「缺值 fallback」誤判。

---

### Task 2: 讓 Go 共用代理核心對所有上游失敗安全換號

**Why:** 三個 Go API 都依賴 `ChatService`；先在代理層統一候選上限、所有錯誤分類、空回覆判定及串流提交前驗證，才能避免各協議自行重作重試邏輯。

**Risk:** strict — 串流狀態、跨帳號故障切換與上游錯誤邊界會直接影響回應完整性。

**Files:**
- Modify: `internal/upstream/grok/client.go:35-43,72-107,135-220`
- Modify: `internal/upstream/grok/client_test.go`
- Modify: `internal/proxy/failover.go:13-58`
- Modify: `internal/proxy/failover_test.go`
- Modify: `internal/proxy/chat.go:21-27,68-91,118-248,312-340,342-429,789-901`
- Modify: `internal/proxy/chat_test.go`

**Interfaces:**
- Consumes: `pool.Chain(candidates, model, mode, now, max)`、`grok.Client.Open`、`grok.ReadSSE`、`ChatDelta`。
- Produces: `ChatService.MaxAttempts int`，代表包含首次在內的總嘗試數；`<=0` 僅供未注入設定的舊測試/內嵌呼叫使用預設 `4`，正式 server 必須傳入 `N+1 >= 1`。
- Produces: 可由 `errors.Is`/`errors.As` 辨識的「所有帳號失敗」錯誤，攜帶安全摘要、實際嘗試數與最後原因，但 `Error()` 不包含 Token 或完整敏感 Body。
- Produces: `ChatService.FailureObserver`（或同等最小 callback）在每個失敗帳號上報 `accountID`、錯誤及可取得的上游狀態，供 server 記錄帳號池狀態。
- Preserves: `CompleteWithResult`、`OpenStreamWithResult` 對 server 的既有成功結果形狀。

**Acceptance:**
- `MaxAttempts=4` 時只建立最多四個不同帳號的 chain；`MaxAttempts=1` 時不換號。
- `grok.Client.Open` 將所有非 `2xx` 回傳為 `UpstreamError`；代理對任何 `UpstreamError`、DNS/TCP/TLS/代理錯誤及讀取錯誤嘗試下一帳號。
- `context.Canceled` 或請求 context 結束立即停止，不換號；一般上游逾時/transport error 在客戶端仍連線時可換號。
- 非串流在完整收集前發生讀取中斷或解析失敗時，可安全換下一帳號，因為尚未向客戶端提交結果。
- HTTP 200 但無非空白文字、推理、工具呼叫或 function call 時視為空回覆並換號。
- `completion_tokens=0` 但有文字、推理或工具呼叫仍成功；usage 缺失或不準確不能覆蓋有效 payload 判定。
- 串流驗證不再使用固定 80ms 後無條件放行：必須等到第一個有效模型 payload、明確空結束、解析錯誤、讀取錯誤或 context 取消。成功時重播已緩衝 SSE；失敗時關閉該 Body 並換下一帳號。
- 角色、usage-only、finish-only 與其他控制 frame 不算有效 payload；EOF/[DONE] 前都沒有有效 payload 時算空回覆。
- 每個失敗帳號恰好觸發一次 failure observer；成功帳號不觸發失敗 observer。

**Constraints:**
- 不以 HTTP 狀態白名單限制換號。
- 串流第一個有效 payload 之後的讀取錯誤不由代理重新開啟另一帳號；server 只終止已提交串流。
- 緩衝只涵蓋第一個有效模型 payload 前的 frame，不緩衝完整正常回應，避免增加長回答記憶體使用。
- 保留 `PickObserver` mark/release 與 affinity 成功綁定語義；失敗帳號不得成為新的 affinity。

**Proof:**
- Focused verification: `go test ./internal/upstream/grok ./internal/proxy -run 'Retry|Failover|PrepareChain|Empty|SlowFirstToken|ReadError|ParseError|Cancel' -count=1` — 狀態碼、transport、空回覆、慢首 token 與取消案例通過。
- Task boundary: `go test ./internal/proxy ./internal/upstream/grok -count=1` — 代理與上游客戶端完整套件通過。
- Strict risk evidence: 測試使用不同 bearer token 記錄實際帳號順序，證明不重複帳號；另證明第一段有效模型內容後的中斷不會開啟下一帳號。

---

### Task 3: 將 Go 動態設定及標準錯誤接到三種 API

**Why:** 代理核心只有在 server 每次請求注入最新 `N+1` 並由三個協議一致處理最終錯誤時，才成為實際可用功能。

**Risk:** strict — 公開 API 錯誤契約、三協議串流封裝及持久化帳號失敗紀錄。

**Files:**
- Modify: `internal/server/server.go:43-73,449-537,711-843,920-1155,1238-1307,1331-1695,2245-2284`
- Modify: `internal/server/server_test.go`
- Modify: `internal/server/chat_responses_e2e_test.go`
- Modify: `internal/server/messages_e2e_test.go`
- Modify: `internal/server/admin_write_test.go`

**Interfaces:**
- Consumes: Task 1 `Connector.UpstreamRetryCount`、Task 2 `ChatService.MaxAttempts`、所有帳號失敗 typed error 與 failure observer。
- Produces: `Options.UpstreamRetryCount func(context.Context) int` 作為可選注入點；正式路徑未注入時每個請求呼叫 `Store.UpstreamRetryCount`，Store 不可用的測試模式回退預設 `3`。
- Produces: server helper 將 `N` 安全轉成 `MaxAttempts=N+1`，並在 chat/messages/responses 建立 `ChatService` 時統一注入。
- Produces: OpenAI Chat/Responses 非串流錯誤 `{error:{message,type:"upstream_error",code:"all_accounts_failed"}}`；Anthropic 非串流錯誤維持 `{type:"error",error:{type:"upstream_error",message,...}}` 並帶等價 code 欄位或既有契約允許的 metadata。

**Acceptance:**
- 管理 API 將 `upstream_retry_count` 從 `3` 改成 `0` 後，下一個 LLM 請求只嘗試一個帳號；改回 `3` 後下一個請求最多嘗試四個，無須重啟 handler。
- `/v1/chat/completions`、`/v1/messages`、`/v1/responses` 的非串流與串流入口都使用同一 `MaxAttempts`。
- `400`、`404`、`429`、`500`、`502`、`503` 及另一個非白名單錯誤（例如 `418`）都能從失敗 token 換到下一個成功 token。
- 全部帳號失敗時，非串流三協議都回 HTTP `503`、`upstream_error`、`all_accounts_failed`，且回應不含 bearer token 或原始敏感 Body。
- 串流在代理層尚未找到第一個有效 payload 前耗盡帳號時，使用 OpenAI Chat、Anthropic Messages 或 Responses 對應的 SSE error/failed 終端；已送有效 payload 後中斷只終止原串流，不換帳號。
- failure observer 為每個失敗帳號呼叫既有 pool failure/Redis 記錄；最終成功仍只以成功帳號記錄正常 usage 與 affinity，嘗試失敗事件不被錯記為整體成功。
- `X-Grok2API-Accounts` 反映實際候選鏈長度；成功後的 account/failover observation 指向最後成功帳號。

**Constraints:**
- `Options.UpstreamRetryCount` 只作設定讀取/測試 seam，不承載重試實作。
- 本地 request JSON、欄位或 API Key 錯誤仍沿用原 `400/401`，不得包成 `all_accounts_failed`。
- 不改變正常成功 payload、工具轉換與 usage 格式。

**Proof:**
- Focused verification: `go test ./internal/server -run 'UpstreamRetry|AllAccountsFailed|ChatAndResponsesE2E|AnthropicMessagesE2E|AdminSettings' -count=1` — 三協議、動態設定與標準錯誤案例通過。
- Task boundary: `go test ./internal/server ./internal/protocol/... -count=1` — server 與協議套件通過。
- Strict risk evidence: E2E fake upstream 依 Authorization token 回傳「第一帳號錯誤、第二帳號成功」及「全部失敗」，並斷言呼叫順序、次數、503 契約與無敏感字串。

---

### Task 4: 讓 Python 相容路徑具備相同的所有錯誤換號行為

**Why:** Python 仍是可選 runtime；若只修 Go，切換 runtime 後管理設定與公開 API 行為會分歧。

**Risk:** strict — FastAPI 公開 API、非同步串流狀態與跨帳號重試。

**Files:**
- Modify: `grok2api/app.py:2895-2919,3994-3995,4128-4150,4263-4487,4512-5368,5371-5505,5571-5912,6047-6930,7020-8100`
- Modify: `grok2api/protocol/openai_responses.py:1243-1300,1316-1486,2039-2170` only if needed to preserve one monotonic Responses terminal while delaying commit until valid output
- Modify: `scripts/_test_upstream_account_retry.py`
- Modify: `scripts/README.md:12-32`

**Interfaces:**
- Consumes: Task 1 `account_pool.max_failover_attempts()` 作為有效總嘗試數、`try_acquire_sequence`、現有 `_collect_completion`、`_report_upstream_failure`、`openai_error`、`anthropic_error`、`failed_responses_sse`。
- Produces: `_retryable_status(code)`（保留現有名稱以減少呼叫面變動）對任何非 `2xx` 上游狀態回傳 true；本地驗證不呼叫它。
- Produces: 共用安全的 chain-exhausted 訊息/常數，使 chat、messages、responses 都使用 `upstream_error` 與 `all_accounts_failed`，不直接回最後原始上游 Body。
- Preserves: `_collect_completion` 以文字、推理、工具呼叫判斷有效輸出；usage token 為 0 不覆蓋有效 payload。

**Acceptance:**
- `_pick_account_chain` 取得的不同帳號數等於最多 `N+1`，且依 Task 1 相容規則即時生效。
- OpenAI Chat、Anthropic Messages、OpenAI Responses 的非串流 loop 遇到任何 `httpx.HTTPStatusError` 都繼續下一帳號，不再因 `400/404/418` 提前 break。
- transport/timeout/read/parse/empty-output 例外在尚未提交時繼續下一帳號；`asyncio.CancelledError` 與已確認客戶端斷線不換號。
- Chat 與 Anthropic 串流使用「有效模型 payload 已送出」作為 commit；role/keepalive/envelope 不阻止下一帳號。
- Responses 串流不得把 `response.created` 當有效模型 payload。為避免換號後重複 `response.created` 或 sequence number 回退，應延後或單次管理協議 envelope，直到第一個有效文字、推理或工具 payload；空回覆或錯誤在未提交時可換號。
- 推理內容即使不對終端使用者顯示，也算有效模型輸出；不得因 usage 為 0 對已有 reasoning 的回應重試。
- reasoning-only 回應正常結束時沿用既有非可見 reasoning metadata/完成事件，不得為了製造可見輸出而把思考鏈轉成 `output_text`。
- 全部候選失敗後，非串流回 `503 / upstream_error / all_accounts_failed`；串流回各協議完整 error terminal 及 `[DONE]`，只表明已嘗試所有可用帳號。
- 每次帳號失敗都保留既有 `_report_upstream_failure`、失敗 usage 與 metrics；成功帳號綁定 affinity，失敗帳號不綁定。

**Constraints:**
- 不捕捉 FastAPI/Pydantic 在呼叫上游前產生的 `400/401/422` 作為可重試錯誤。
- 已送有效文字、推理或工具事件後，任何例外只關閉當前協議串流；不能重播另一帳號結果。
- Responses SSE 的 `sequence_number` 必須單調，且每個回應最多一個 created 與一個 completed/failed 終端。
- 不把 reasoning 降級成可見 `output_text` 來規避空回覆；沿用既有協議隱私政策。

**Proof:**
- Focused verification: `python -B scripts/_test_upstream_account_retry.py` — 以 mock/本地 HTTP transport 覆蓋三協議、`N=0/3`、所有狀態、空回覆、串流 commit 與最終錯誤。
- Task boundary: `python -B scripts/run_regressions.py` — 所有 tracked Python regression scripts 通過，新腳本也被 runner 自動納入。
- Strict risk evidence: regression 記錄每次 Authorization token，證明失敗帳號不重複；另解析 SSE，證明 Responses sequence 單調、只有一個 created/terminal，且有效輸出後沒有下一帳號請求。

---

### Task 5: 更新系統設定頁並完成跨 runtime 實機驗證

**Why:** 使用者必須能理解並修改 `N`；最終還需以實際 HTTP fake upstream 證明設定、換號與三協議行為形成完整閉環。

**Risk:** strict — 管理設定會直接改變正式流量的重試放大倍數，且驗收跨 UI、Go、Python 與假上游程序。

**Files:**
- Modify: `static/admin/settings.html:351-370`
- Modify: `static/js/core.js:6272-6284,6388-6399`
- Modify: `contracts/fake_upstream.py:33-69,83-143`
- Modify: `static/dist/manifest.json`（由資產建置器產生）
- Generate: `static/dist/core.<content-hash>.js`（由 `scripts/build_admin_assets.py` 依內容 hash 產生）
- Modify: `static/admin/accounts.html`, `static/admin/guide.html`, `static/admin/index.html`, `static/admin/keys.html`, `static/admin/login.html`, `static/admin/logs.html`, `static/admin/models.html`, `static/admin/rotation.html`, `static/admin/settings.html`, `static/admin/usage.html`（僅由資產建置器更新 hashed asset reference）

**Interfaces:**
- Consumes: GET `/admin/api/settings` 的 `upstream_retry_count`、PUT/PATCH `/admin/api/settings`。
- Produces: `#set-upstream-retry-count` number input，`min=0`、`max=63`、`step=1`；表單讀取/提交鍵固定為 `upstream_retry_count`。
- Produces: fake upstream 可依 bearer token 或明確 scenario 決定每個帳號的 `status-*`、`empty-200`、`html-200`、`truncate-before-output`、`truncate-after-output`、`normal` 行為，且不在日誌輸出完整 token。

**Acceptance:**
- 設定頁標籤為「上游報錯自動換號重試次數」，輔助文字明確寫「設為 3 時，最多嘗試 4 個不同帳號；0 表示不重試」。
- 表單載入缺少新值的舊後端回應時，可使用唯讀 `max_failover_attempts-1` 顯示相容值；提交永遠只送新鍵。
- 輸入值在瀏覽器與後端都限制 `0–63`，不出現「最多帳號數」與「額外重試數」混用文案。
- 執行資產建置後，manifest、hashed core bundle 與所有管理頁引用一致，來源檔與生成檔內容同步。
- 本地 fake upstream 實際接受 HTTP 請求；Go 與 Python 都完成：首帳號 `429/400/500/空回覆`、下一帳號成功；`N+1` 個帳號全失敗；有效串流輸出後截斷不換號。
- 三種 API 只回最後成功帳號的完整結果；全部失敗時回標準 `503/all_accounts_failed`。
- 所有 touched text files 為 LF，差異中沒有尾隨空白、衝突標記、秘密或無關重構。

**Constraints:**
- 不手動編輯 hashed bundle；只修改 `static/js/core.js` 後執行既有 `scripts/build_admin_assets.py`。
- fake upstream 的 token scenario 只解析測試用標記，不記錄或回顯完整 Authorization。
- 實機測試前確認沒有舊 fake upstream 或服務程序佔用測試 port；測試程序結束後正常關閉，不使用破壞性終止。

**Proof:**
- Focused verification: `python -B scripts/build_admin_assets.py` — 完成且 manifest 指向新 core hash。
- Focused verification: `python -B scripts/_test_upstream_account_retry.py` — Python 經本地 HTTP fake upstream 的端到端案例通過。
- Task boundary: `go test ./... -count=1` — 全部 Go 測試通過。
- Task boundary: `python -B scripts/run_regressions.py` — 全部 Python regression 通過。
- Task boundary: `git diff --check` — 無空白或衝突標記問題。
- Strict risk evidence: `git ls-files --eol` 檢查所有 touched Go、Python、HTML、JavaScript、Markdown、JSON 檔均為 `w/lf`；實機測試輸出列出每個案例的帳號嘗試數、最終狀態及協議終端，不輸出憑證。
