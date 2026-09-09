# ms — MIDICOLLAB realtime studio backend

Rust + Axum WebSocket server that owns the authoritative musical `ProjectState`
for MIDICOLLAB. The Next.js app (`mcw` repo) handles accounts, project metadata
and invites; this service handles the live session.

The deployed binary is **`studio_server`** (see `Dockerfile`).

## Endpoints

| Route                     | Purpose                                                       |
| ------------------------- | ------------------------------------------------------------- |
| `GET /health`             | liveness — returns `ok`                                       |
| `GET /ws`                 | WebSocket. First message must be `{ "type":"join", "project_id":"…", "token":"…" }` |
| `GET /api/projects/{id}`  | read a document — needs a valid token unless the project is public |
| `PUT /api/projects/{id}`  | replace a document — needs an editor/owner token              |
| `POST /api/samples`       | multipart upload — needs any valid token, 25 MB cap          |
| `GET /samples/*`          | static sample files                                           |

## Auth

Every privileged action requires a short-lived HMAC token minted by the Next.js
app and signed with `REALTIME_SHARED_SECRET` (shared, identical value on both
deployments).

Token = `base64url(payloadJson).base64url(hmacSha256(payloadB64))` where the
payload is `{ sub, name, image, pid, role, iat, exp }`. The server checks the
signature, the expiry, and that `pid` matches the room being joined. Identity
and the `owner`/`editor` gate come from the verified payload — this service
never talks to Postgres.

## Environment

| Key                      | Notes                                                   |
| ------------------------ | ------------------------------------------------------- |
| `REALTIME_SHARED_SECRET` | required; identical to the value set on Vercel          |
| `DATA_DIR`               | where project JSON + samples live (default `data`)      |
| `PORT`                   | listen port (default `8080`; Railway sets it)           |

**On Railway, mount a persistent volume at `DATA_DIR` (`/data`).** Otherwise
every redeploy erases all projects and samples.

## Run locally

```bash
DATA_DIR=./data \
REALTIME_SHARED_SECRET=dev-secret-change-me-1234567890 \
PORT=8090 \
cargo run --release --bin studio_server
```

## Build

```bash
cargo build --release --bin studio_server
cargo check          # all targets
```
