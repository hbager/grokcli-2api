# 公網匿名端點資訊最小化設計

日期：2026-07-17

## 背景

目前匿名 `GET /admin/api/status` 會透過 `get_public_settings()` 回傳完整註冊設定，其中包含第三方 API Key、代理位址及其他內部設定。匿名 `GET /health` 也回傳帳號識別、帳號池、儲存層、leader、註冊器與維護狀態等內部資料。這些資料超出匿名登入啟動與負載平衡探活所需範圍。

已公開的第三方金鑰必須在供應商端撤銷並輪替；程式修正無法讓舊金鑰恢復安全。

## 目標

1. 未登入使用者只能取得登入頁啟動所需的最小狀態。
2. 公開健康檢查只回傳可用性與版本，不回傳憑證、帳號、拓撲或內部設定。
3. 已登入管理員仍可使用既有管理台，但一般設定回應不得反覆傳送完整第三方秘密。
4. 檢查其他匿名診斷介面，避免相同資訊從 `/metrics` 或 API 文件繞出。

## 信任邊界與資料流

```text
匿名網路
  ├─ GET /admin/api/status ──> 最小登入啟動狀態
  ├─ GET /health           ──> 最小 readiness 狀態
  ├─ GET /metrics          ──> 不得匿名暴露內部指標
  └─ /docs /redoc /openapi.json ──> 不得在公網匿名暴露介面結構

已驗證管理員
  ├─ GET /admin/api/dashboard ──> 管理狀態與遮罩後設定
  └─ 專用設定端點             ──> 僅回遮罩或是否已設定
```

## 端點設計

### `GET /admin/api/status`

保持匿名，因登入頁需要判斷是否首次設定。固定只回：

```json
{
  "ok": true,
  "setup_needed": false,
  "version": "1.9.91"
}
```

不得回傳：

- 帳號或帳號池資料
- Key 統計與鑑權策略
- 系統設定或註冊設定
- API Key、token、password、proxy
- PostgreSQL、Redis、worker、leader 或容器資訊
- 上游 URL、內網 URL、主機、連接埠
- 維護、模型探測、用量或會話狀態

登入後的完整畫面資料繼續由既有受保護的 `/admin/api/dashboard` 與專用管理端點提供。

### `GET /health`

保持匿名供反向代理與容器探活。成功只回：

```json
{
  "status": "ok",
  "version": "1.9.91"
}
```

失敗維持 HTTP `503`，只回通用狀態與版本，不回原始例外或內部元件細節。

健康判斷可以在伺服器內部檢查必要依賴，但檢查結果不得直接序列化給匿名呼叫者。

### `/metrics` 與 API 文件

- `/metrics` 改為必須提供有效的既有客戶端 API Key，避免匿名取得內部容量、錯誤率或服務結構。
- `/docs`、`/redoc`、`/openapi.json` 在正式公網預設停用。若日後需要公開文件，應以明確設定重新啟用，不與本次修正綁定管理員 session。

## 秘密回應政策

- `get_public_settings()` 不得再呼叫 `get_registration_config(include_secrets=True)` 或 `get_outbound_proxy_config(include_secrets=True)`。
- 管理狀態與設定 GET 回應預設使用遮罩值及 `*_set`／`has_*` 布林值。
- 第三方 API Key、token、password、代理密碼不得出現在匿名回應。
- 既有用戶端 API Key 清單另有明文儲存與重複回傳風險，但不擴張本次端點外洩修正；本次至少確保其仍只存在於管理員驗證後端點。

## CORS 與 Cookie

- CORS 不再使用萬用來源搭配 credentials。
- 管理台使用同源請求；不需要為匿名跨來源網站開放管理 API。
- 公網 HTTPS 下管理 cookie 應標記 `Secure`。由於本專案也支援本機 HTTP，是否安全應依請求／轉發協定決定，而不是硬編碼永遠關閉。
- 管理 session 從 `localStorage` 遷移為純 HttpOnly cookie 屬獨立安全強化，避免本次修正範圍擴大；本次不得削弱現有 session 驗證。

## 測試

新增針對回應契約的測試：

1. 匿名 `/admin/api/status` 僅有允許的三個欄位。
2. status 回應遞迴掃描不得出現秘密欄位名稱或測試秘密值。
3. 匿名 `/health` 成功回應僅有 `status`、`version`。
4. health 失敗回應為 `503`，不包含內部例外訊息。
5. 匿名 `/metrics` 回傳 `401`；有效 API Key 可取得指標。
6. `/docs`、`/redoc`、`/openapi.json` 預設不可匿名使用。
7. 已登入 dashboard 仍能取得管理台需要的非秘密狀態，設定秘密只以遮罩／存在旗標表示。
8. 執行相關測試、完整既有測試，以及 `git diff --check`。

## 部署與驗證

1. 先撤銷並輪替已公開第三方金鑰。
2. 部署修正版並重啟應用；確認沒有舊容器仍服務舊映像。
3. 從無 cookie、無 Authorization 的外部請求驗證所有匿名端點。
4. 在瀏覽器無痕視窗確認登入流程正常。
5. 登入後確認 dashboard 與設定頁仍可使用，Network 回應中沒有第三方完整秘密。

## 非目標

- 不在本次重做整套管理 session 架構。
- 不新增新的權限角色或 OAuth 系統。
- 不更動 `/v1` 的既有 API Key 驗證語義。
- 不自動操作第三方供應商撤銷金鑰。
