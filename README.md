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
