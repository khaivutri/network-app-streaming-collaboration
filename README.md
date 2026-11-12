# Network Streaming Collaboration

A Go based collaboration app with a reliable UDP transport, authenticated and encrypted sessions, chat, file transfer, and a local browser interface for each client process.

## Contents

- [Features](#features)
- [Requirements](#requirements)
- [Quick start](#quick-start)
- [Run multiple clients](#run-multiple-clients)
- [Docker quick start](#docker-quick-start)
- [Use the app](#use-the-app)
- [Configuration](#configuration)
- [Network simulation](#network-simulation)
- [Tests](#tests)
- [Troubleshooting](#troubleshooting)
- [Demo accounts and security](#demo-accounts-and-security)

## Features

- Reliable UDP sessions with per-stream sequencing and retransmission.
- X25519 key agreement and AES-256-GCM authenticated encryption.
- User authentication, online presence, broadcast and direct messages.
- Relayed file transfer with progress, cancellation, and local downloads.
- A self-contained browser UI served by a loopback-only HTTP agent. The UI uses REST and Server-Sent Events.
- Multiple client agents on one computer, each with a separate browser port and storage directory.

## Requirements

- Go 1.22 or later.
- Windows PowerShell, Linux, or WSL. The commands below use Go directly and do not require Make.
- A modern browser for the web UI.

Run commands from the repository root, the directory containing `go.mod`.

## Quick start

Check Go and build both programs:

```powershell
go version
go build ./cmd/server ./cmd/client
```

Open a terminal in the repository root and start the server:

```powershell
go run ./cmd/server --mode full --addr 127.0.0.1:9000 --users configs/users.json
```

The server listens for UDP traffic on `127.0.0.1:9000`. Keep this terminal open.

In a second terminal, start one client agent:

```powershell
go run ./cmd/client --mode ui --server 127.0.0.1:9000 --ui-port 8081 --storage storage/client1
```

Open [http://127.0.0.1:8081](http://127.0.0.1:8081) and sign in with the `hung.vo` demo account. The agent prints its local UI address when it starts. Keep its terminal open while using the UI.

## Run multiple clients

Run one server and one client agent per terminal. Each client needs its own UI port and storage directory. Use a different demo account in each browser window because an account can have only one active session.

| Terminal | Command | Browser address | Demo user |
| --- | --- | --- | --- |
| 1 — Server | `go run ./cmd/server --mode full --addr 127.0.0.1:9000 --users configs/users.json` | — | — |
| 2 — Client 1 | `go run ./cmd/client --mode ui --server 127.0.0.1:9000 --ui-port 8081 --storage storage/client1` | `http://127.0.0.1:8081` | `hung.vo` |
| 3 — Client 2 | `go run ./cmd/client --mode ui --server 127.0.0.1:9000 --ui-port 8082 --storage storage/client2` | `http://127.0.0.1:8082` | `Alice` |
| 4 — Client 3 | `go run ./cmd/client --mode ui --server 127.0.0.1:9000 --ui-port 8083 --storage storage/client3` | `http://127.0.0.1:8083` | `Bob` |

Open each URL in a separate browser tab or window. Sign in with that row's account. The peer list should show the other connected users. Send a room message or select a peer to send a direct message.

To stop a client or the server, focus its terminal and press **Ctrl+C**. Stop the client agents before stopping the server so they can send their logout request.

## Docker quick start

You can also launch the complete multi-client topology (one central RUDP server and three client UI agents) with a single Docker Compose command:

```powershell
docker compose up --build -d
```

This starts the following containers on an isolated bridge network (`mmt-network`):

| Container | Role | Host browser address | Storage volume on host | Suggested demo user |
| --- | --- | --- | --- | --- |
| `mmt-server` | Central RUDP Server (`:9000/udp`) | — | `configs/users.json` (read-only) | — |
| `mmt-client-1` | Client Agent 1 | `http://127.0.0.1:8081` | `./storage/client1` | `hung.vo` (`Password@123`) |
| `mmt-client-2` | Client Agent 2 | `http://127.0.0.1:8082` | `./storage/client2` | `Alice` (`Alice@123`) |
| `mmt-client-3` | Client Agent 3 | `http://127.0.0.1:8083` | `./storage/client3` | `Bob` (`Bob@123`) |

View logs or stop the containers:

```powershell
docker compose logs -f
docker compose down
```

### Simulate packet loss and jitter in Docker

Add `--loss` and/or `--jitter` to the relevant service's `command` list in `docker-compose.yml`. These are command-line flags (not environment variables). For example, to simulate 5% outbound packet loss and up to 30 ms of extra outbound delay from Client 2, add these entries under `mmt-client-2`'s existing `command`:

```yaml
      - --loss=0.05
      - --jitter=30ms
```

Recreate the service to apply the change:

```powershell
docker compose up --build -d mmt-client-2
```

Impairments are outbound and apply only to the process configured. Configure the same flags on `mmt-server` as well to simulate impaired traffic in both directions. Add them to a client to affect that client's outgoing traffic; add them to the server to affect server-to-client traffic. `--loss` accepts a rate from `0` to `1` (`0.05` means 5%). `--jitter` accepts a non-negative Go duration such as `20ms`; each outbound packet gets a random additional delay from 0 up to the configured maximum. It does not set a fixed round-trip time.

## Use the app

The browser UI supports peer discovery, broadcast and direct chat, and file transfer. For a small file-transfer check, choose a file in one client, send it to a connected peer, and save the received file. The default per-upload limit is 1 GiB.

Client files are kept under each agent's configured storage directory:

- `staging/` holds files being prepared or uploaded.
- `downloads/` holds received files.

For example, Client 2 uses `storage/client2/downloads`. Compare a downloaded file with its source in PowerShell:

```powershell
Get-FileHash .\sample.bin -Algorithm SHA256
Get-FileHash .\storage\client2\downloads\sample.bin -Algorithm SHA256
```

The download filename may be adjusted to avoid overwriting an existing file. Compare the actual saved path shown by the UI.

## Configuration

### Server

```text
--mode       Server mode; defaults to full. Use step2-tcp, rudp, or step3-auth through step9-http-ui for individual milestones.
--addr       Server listen address; defaults to :9000.
--users      Credential JSON file; defaults to configs/users.json.
--log-level  Accepted for compatibility; the current command uses the default Go logger configuration.
```

`full` runs the encrypted reliable UDP backend used by the web UI. The HTTP UI is served by each client process, not by the central server.

### Client

```text
--mode             Client mode; defaults to ui. ui is an alias for step9-http-ui.
--server           Server address; defaults to 127.0.0.1:9000.
--ui-addr          Local UI listen address; defaults to 127.0.0.1:8081.
--ui-port          Local loopback port alias, for example 8082.
--storage-dir      Per-client storage root; defaults to storage.
--storage          Alias for --storage-dir.
--max-upload-bytes Maximum browser upload size; defaults to 1073741824 bytes.
```

If both storage options are provided with different non-default values, the command exits with an error. UI ports must be unique for simultaneously running agents. Keep the UI on loopback; it is designed for local access.

## Tests

Run all Go tests:

```powershell
go test ./...
```

Run static analysis and race checks:

```powershell
go vet ./...
go test -race ./...
```

Run the browser state tests when Node.js is installed:

```powershell
node --test web/static/js/*.test.mjs
```

## Network simulation

Both server and UI client accept outbound UDP impairment flags. Values apply independently to the process where they are set. For example, run a client with 10% outbound loss and up to 30 ms of jitter:

```powershell
go run ./cmd/client --mode ui --server 127.0.0.1:9000 --ui-port 8083 --storage storage/client3 --loss 0.10 --jitter 30ms
```

The supported options are `--loss`, `--reorder`, `--duplicate`, and `--corrupt` with rates from `0` to `1`, plus `--delay` and `--jitter` as non-negative Go durations such as `20ms`. The same flags are accepted by the server. Configure both sides to simulate impairments in both directions. Loss and corruption can prevent a session from completing, so begin with a low rate. Rates are validated before sockets are opened.

Use these jitter ranges as illustrative test scenarios. The labels describe the added variation being simulated, not a universal rating of a real network; actual quality also depends on baseline latency, packet loss, route, and application needs.

| `--jitter` value | Example scenario | What it simulates |
| --- | --- | --- |
| `0ms` | Stable link | No added packet delay variation. |
| `1–10ms` | Very stable / good network | Small variation, similar to a steady local or well-connected link. |
| `10–30ms` | Typical variable Wi-Fi or busy link | Noticeable timing variation while interactive chat should generally remain responsive. |
| `30–80ms` | Unstable or congested connection | Large variation; packet arrivals may feel uneven and transfers may slow as retransmissions and buffering respond. |
| `80–150ms` | Poor / highly unstable connection | Severe variation that can make interactive use feel laggy. |
| `Over 150ms` | Extreme impairment test | Very large delay swings, useful for stress testing rather than representing a normal connection. |

The configured value is the **maximum additional one-way delay per outbound packet**, sampled from 0 to that value. For example, `--jitter=30ms` means each packet can get 0–30 ms of extra delay; it does not mean every packet is delayed by 30 ms. Use `--delay` when you also want a fixed base delay.

## Troubleshooting

- **Address already in use:** choose another server UDP port or UI port. Update every client's `--server` value if the server port changes.
- **Cannot connect / `ERR_SERVER_UNREACHABLE`:** confirm the server terminal is still running, that the client uses the same server address and port, and that local firewall rules allow UDP traffic.
- **The UI does not open:** confirm the client agent printed the expected URL and that no other process owns the chosen UI port. The browser URL uses HTTP, for example `http://127.0.0.1:8082`.
- **Login reports that the account is already online:** sign out from the other client window or use a different demo account.
- **A peer is missing:** refresh the peer list and confirm both client agents have logged in successfully.
- **File upload is rejected:** check the configured upload limit and available disk space in that client's storage directory.

## Demo accounts and security

The local demo credential file contains these accounts:

| Username | Password |
| --- | --- |
| `hung.vo` | `Password@123` |
| `Alice` | `Alice@123` |
| `Bob` | `Bob@123` |
| `Charlie` | `Charlie@123` |
| `David` | `David@123` |

These credentials are for local demonstrations only. Replace them before using the application outside a trusted development environment. Client UI agents bind to loopback and enforce local-origin and CSRF checks. The UDP session negotiates X25519 keys and encrypts application payloads with AES-256-GCM.
