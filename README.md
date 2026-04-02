# matrix-signal-adapter

**A drop-in replacement for `signal-cli` that uses Matrix as the messaging backend.**

All three `signal-cli` interfaces are supported:

| Interface | signal-cli | matrix-signal-adapter |
|-----------|-----------|----------------------|
| CLI commands | `signal-cli send …` | `matrix-signal-adapter send …` |
| JSON-RPC stdin/stdout | `signal-cli jsonRpc` | `matrix-signal-adapter jsonRpc` |
| Daemon (socket / TCP / HTTP) | `signal-cli daemon --socket` | `matrix-signal-adapter daemon --socket` |

---

## Quick start

### 1. Install

```bash
pip install matrix-signal-adapter
# For end-to-end encryption support (requires libolm ≥ 3):
pip install "matrix-signal-adapter[e2ee]"
```

Or from source:

```bash
git clone https://github.com/example/matrix-signal-adapter
cd matrix-signal-adapter
pip install -e .
```

### 2. Configure

Create a config file for your Matrix account:

```bash
mkdir -p ~/.config/matrix-signal-adapter
cat > ~/.config/matrix-signal-adapter/@you_matrix.org.json <<'EOF'
{
  "homeserver": "https://matrix.org",
  "user_id":    "@you:matrix.org",
  "access_token": "syt_your_token_here"
}
EOF
```

> **Tip:** Use `access_token` instead of `password` so your password is not stored on disk.  
> You can get an access token from Element → Settings → Help & About → Access Token.

> **Important:** `homeserver` must be the server base URL (for example `https://matrix.org`),
> not a client API path like `.../_matrix/client/v3`.

The config filename is derived from the account: `@` and `:` are replaced by `_`.  
So `@alice:example.org` → `alice_example.org.json`.

If you see warnings like `Error validating response: 'next_batch' is a required property`,
the sync endpoint returned an error payload instead of a normal `/sync` response. Common
causes are an invalid/expired `access_token` or an incorrect `homeserver` URL.

### 3. Use it

```bash
# Send a message to a recipient (signal-cli style)
matrix-signal-adapter -a @you:matrix.org send \
  @bob:matrix.org \
  -m "Hello from the adapter!"

# Also supported: localpart shorthand if unique across known contacts
matrix-signal-adapter -a @you:matrix.org send \
  bob \
  -m "Hi Bob"

# Send to a specific room with an attachment
matrix-signal-adapter -a @you:matrix.org send \
  -r '!roomid:matrix.org' \
  -m "Here's a file" \
  --attachment /path/to/file.pdf

# Receive pending messages (prints JSON envelopes)
matrix-signal-adapter -a @you:matrix.org receive --timeout 5

# List all joined rooms (= "groups")
matrix-signal-adapter -a @you:matrix.org listGroups

# Create a new room
matrix-signal-adapter -a @you:matrix.org createGroup \
  -n "My Group" \
  -m @bob:matrix.org -m @carol:matrix.org

# Join a room
matrix-signal-adapter -a @you:matrix.org joinGroup \
  --uri '#my-room:matrix.org'

# Leave a room
matrix-signal-adapter -a @you:matrix.org quitGroup \
  -g '!roomid:matrix.org'
```

---

## Daemon mode

### UNIX socket (default)

```bash
matrix-signal-adapter -a @you:matrix.org daemon --socket
# Listens at: /tmp/matrix-signal-you_matrix.org.sock

# Custom path:
matrix-signal-adapter -a @you:matrix.org daemon --socket /run/signal/matrix.sock
```

### TCP

```bash
matrix-signal-adapter -a @you:matrix.org daemon --tcp localhost:7583
```

### HTTP

```bash
matrix-signal-adapter -a @you:matrix.org daemon --http localhost:8080
# POST JSON-RPC requests to: http://localhost:8080/api/v1/rpc
```

Events stream (SSE):

```bash
curl -N http://localhost:8080/api/v1/events
```

You should immediately see `: connected` and then periodic `: keepalive` lines
until real incoming events arrive as `data: {...}`.

---

## JSON-RPC mode (stdin/stdout)

Reads newline-delimited JSON requests from stdin, writes responses and
incoming-message notifications to stdout — **identical to `signal-cli jsonRpc`**.

```bash
matrix-signal-adapter -a @you:matrix.org jsonRpc
```

Example request/response:

```jsonc
// → stdin
{"jsonrpc":"2.0","id":1,"method":"send","params":{"recipient":"@bob:matrix.org","message":"Hi!"}}

// ← stdout
{"jsonrpc":"2.0","id":1,"result":{"timestamp":1712000000000}}

// ← stdout (incoming message notification)
{"jsonrpc":"2.0","method":"receive","params":{"envelope":{"timestamp":1712000001000,"source":"@bob:matrix.org","sourceName":"Bob","dataMessage":{"message":"Hello back!"}}}}
```

---

## Concept mapping

| signal-cli concept | Matrix equivalent |
|-------------------|------------------|
| Phone number (`+491234…`) | Matrix user ID (`@user:homeserver`) |
| Group | Room |
| Group ID (base64) | Room ID (`!id:homeserver`) |
| Attachment | Matrix media upload (mxc:// URI) |
| Receipt | (stub — no-op) |
| Expiring messages | (not yet implemented) |
| E2EE | Supported if `matrix-nio[e2e]` + `libolm` installed |

---

## Supported commands

| Command | Status | Notes |
|---------|--------|-------|
| `send` | ✅ | text + attachments |
| `receive` | ✅ | drains internal queue |
| `listGroups` | ✅ | all joined rooms |
| `createGroup` | ✅ | creates Matrix room |
| `updateGroup` | ✅ | invite / kick members |
| `quitGroup` | ✅ | leaves room |
| `joinGroup` | ✅ | join by alias or room ID |
| `listContacts` | ✅ | members across all rooms |
| `updateProfile` | ✅ | sets display name |
| `sendTyping` | ✅ | typing indicator |
| `sendReaction` | ✅ | m.reaction event |
| `deleteMessage` | ✅ | redacts event |
| `getUserStatus` | ⚠️  | always returns "registered" |
| `listDevices` | ⚠️  | stub (returns adapter device) |
| `block` / `unblock` | ⚠️  | stub (no-op) |
| `sendReceipt` | ⚠️  | stub (no-op) |
| `register` / `verify` | ❌ | use Matrix account directly |
| `link` | ❌ | not applicable |

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  Your application                    │
│   (uses signal-cli JSON-RPC / CLI / socket API)      │
└────────────────┬────────────────────────────────────┘
                 │  signal-cli compatible interface
┌────────────────▼────────────────────────────────────┐
│           matrix-signal-adapter                      │
│  ┌──────────────┐  ┌───────────────┐  ┌──────────┐  │
│  │  cli.py      │  │jsonrpc_server │  │command_  │  │
│  │  (argparse)  │→ │  .py          │→ │handler   │  │
│  └──────────────┘  │  socket/tcp/  │  │.py       │  │
│                    │  http/stdio   │  └────┬─────┘  │
│                    └───────────────┘       │        │
│                                    ┌───────▼──────┐ │
│                                    │matrix_backend│ │
│                                    │  .py (nio)   │ │
│                                    └───────┬──────┘ │
└────────────────────────────────────────────┼────────┘
                                             │  Matrix Client-Server API
                                    ┌────────▼──────────┐
                                    │  Matrix Homeserver │
                                    │  (Synapse, etc.)   │
                                    └───────────────────┘
```

---

## Troubleshooting & Debug

### Events stream not receiving messages after reconnect

Enable debug logging to trace the sync loop and message queue:

```bash
matrix-signal-adapter -a @you:matrix.org daemon --http -v
```

The verbose flag enables `DEBUG` level logging. Look for messages like:
- `SSE client connected: ...` (new connection open)
- `Message callback: queuing ...` (incoming messages queued)
- `SSE sending message to ...` (event sent to client)
- `Sync loop starting / crashed` (background sync status)

If sync loop crashes repeatedly, check the homeserver URL and access token are valid.

### How to diagnose issues

1. **Check daemon startup:**
   ```bash
   matrix-signal-adapter -a @you:matrix.org daemon --http -v 2>&1 | head -50
   ```

2. **Test events stream in one terminal:**
   ```bash
   curl -N http://localhost:8080/api/v1/events
   ```
   Should show `: connected` immediately, then `: keepalive` every 15s.

3. **Send a test message in another terminal:**
   ```bash
   matrix-signal-adapter -a @you:matrix.org send bob -m "test"
   ```
   
   The events stream should show the received message (or check logs for why it didn't).

4. **Check Matrix account directly:**
   Use Element (Matrix client) to verify messages are actually arriving at the homeserver.

---

## License

MIT
