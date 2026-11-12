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

### Run a network simulation in Docker

Add the impairment flags to the relevant service's `command` list in `docker-compose.yml` (they are command-line flags, not environment variables). For example, add these entries under `mmt-client-2`'s existing `command` to model a lossy, variable connection:

```yaml
      - --loss=0.05
      - --jitter=30ms
```

Recreate the service to apply the change:

```powershell
docker compose up --build -d mmt-client-2
```

The flags affect only outbound traffic from the configured process. Add them to a client to affect that client's outgoing packets; add them to `mmt-server` to affect server-to-client packets. Set them on both sides to impair traffic in both directions. See [Network simulation](#network-simulation) for each flag's meaning and suggested test ranges. Recreate the changed service with `docker compose up --build -d mmt-client-2` (or the server service name) to apply the configuration.

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

## Network simulation

Both server and UI client accept the following outbound UDP impairment flags. Rate flags use decimals from `0` to `1` (`0.05` means 5%); duration flags use non-negative Go durations such as `20ms` or `1s`. The suggested ranges below are practical test scenarios, not universal network-quality standards. Real conditions vary with baseline latency, route, Wi-Fi quality, and application needs.

| Flag | What it simulates | Suggested values and example condition |
| --- | --- | --- |
| `--loss` | Randomly drops outbound packets. | `0`: none; `0.001–<0.01` (0.1–<1%): occasional loss on otherwise stable Wi-Fi; `0.01–<0.05` (1–<5%): congested Wi-Fi or mobile link; `0.05–0.15` (5–15%): poor/edge connection; `>0.15`: severe impairment. `1` drops every outbound packet. |
| `--reorder` | Delays selected packets so later packets can arrive first, exercising stream reordering and recovery. | `0`: none; `0.001–<0.01`: occasional out-of-order delivery on a variable route; `0.01–<0.05`: frequent reordering on a busy or unstable path; `≥0.05`: severe stress test. |
| `--duplicate` | Sends a second copy of selected outbound packets. | `0`: none; `0.001–<0.01`: rare duplicate datagrams; `0.01–<0.05`: noticeable duplicate traffic; `≥0.05`: abnormal-network stress test. |
| `--corrupt` | Flips a bit in selected packets to simulate data corruption. | `0`: none; `0.001–<0.01`: rare corruption; `0.01–<0.05`: frequent integrity failures; `≥0.05`: severe stress test. This is an artificial fault-injection scenario, not a typical healthy link profile. |
| `--delay` | Adds a fixed one-way delay to every outbound packet. | `0ms`: none; `1–<20ms`: nearby LAN or low-latency link; `20–<80ms`: ordinary regional internet path; `80–150ms`: distant route or congested connection; `>150ms`: high-latency/very distant path stress test. |
| `--jitter` | Adds a random one-way delay from 0 up to the configured maximum to each outbound packet. | `0ms`: stable timing; `1–<10ms`: steady broadband or good Wi-Fi; `10–<30ms`: variable Wi-Fi or busy link; `30–<80ms`: unstable/congested connection; `80–150ms`: poor, highly variable path; `>150ms`: extreme impairment test. |

Example client command with 10% outbound loss and up to 30 ms of extra jitter:

```powershell
go run ./cmd/client --mode ui --server 127.0.0.1:9000 --ui-port 8083 --storage storage/client3 --loss 0.10 --jitter 30ms
```

`--delay` and `--jitter` can be combined: the fixed delay is applied first, then the random jitter is added. The jitter value is a maximum, not a fixed amount (for example, `--jitter=30ms` adds 0–30 ms per packet). `--reorder` uses extra delay to exercise reordering. Start with low loss/corruption rates because they can prevent sessions from completing. Rates and non-negative durations are validated before sockets are opened.

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
