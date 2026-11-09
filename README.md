# network-app-streaming-collaboration
# Network App — Step 2

Ứng dụng cộng tác mạng dùng custom application protocol với header cố định 20 byte Big-Endian, TCP handshake cho mốc Step 2 và nền tảng reliable UDP cho các bước tiếp theo.

Yêu cầu: Go 1.22+ trên WSL/Linux; không có dependency bên thứ ba.

Từ PowerShell tại workspace:

```powershell
wsl make -C network-app-streaming-collaboration check
wsl make -C network-app-streaming-collaboration smoke-tcp
wsl make -C network-app-streaming-collaboration smoke-udp
```

Chạy thủ công trong hai terminal WSL với `make server-tcp` và `make client-tcp`; dùng `make server-udp` và `make client-udp` cho RUDP. Các target khác: `help`, `fmt`, `vet`, `test`, `test-proto`, `test-tcp`, `test-rudp`, `test-int`, `build`, `clean`.

## Step 9 — Local HTTP UI

Terminal 1 chạy UDP backend:

```powershell
go run ./cmd/server --mode=step9-http-ui --addr=127.0.0.1:9000 --users=configs/users.json
```

Terminal 2 chạy Client Agent + embedded browser UI:

```powershell
go run ./cmd/client --mode=step9-http-ui --server=127.0.0.1:9000 --ui-addr=127.0.0.1:8081 --storage-dir=storage/client-8081
```

Mở `http://127.0.0.1:8081`. Client Agent chỉ bind loopback; browser gửi lệnh qua REST JSON/multipart và nhận message, presence, connection state và file progress qua SSE. Không có WebSocket, framework HTTP hoặc CDN frontend. Password chỉ nằm trong form/agent memory trong thời gian session; payload UDP được bảo vệ bởi X25519 + AES-256-GCM của Step 8.

Chạy nhiều client trên cùng máy bằng UI port và storage directory khác nhau, ví dụ `8082`/`storage/client-8082`. Giới hạn upload mặc định là 1 GiB và có thể đổi bằng `--max-upload-bytes`.

Kiểm thử riêng Step 9:

```powershell
go test -race ./internal/client/... ./web/...
go test -race -run TestStep9 ./tests/integration/...
go vet ./...
```
