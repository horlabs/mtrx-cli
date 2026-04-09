# mtrx-cli

`mtrx-cli` is a signal-cli-compatible adapter that uses Matrix as the
messaging backend. It exposes the common signal-cli interfaces (CLI, JSON-RPC, and
daemon) while mapping those operations to a Matrix account.

Status: Alpha — test before using in production.

---

Important notice
----------------

This tool was implemented as part of a research project and receives limited
maintenance and support. Issues and pull requests are welcome, but users should be
aware that active development resources are constrained.

---

Quick overview
--------------

- Supported interfaces: CLI, JSON-RPC (stdin/stdout), Daemon (Unix socket / TCP / HTTP)
- Python >= 3.10
- Optional: E2EE support with `matrix-nio[e2e]` and `libolm`

---

Installation
------------

Install from PyPI:

```bash
pip install mtrx-cli
# Optional: enable E2EE support
pip install "mtrx-cli[e2ee]"
```

From source:

```bash
git clone https://github.com/horlabs/matrix-cli
cd matrix-cli
pip install -e .
```

The CLI entry point is `mtrx-cli` (see `pyproject.toml`).

---

Configuration
-------------

Configuration files live in `~/.config/mtrx-cli/` by default. Filenames are
derived from the account identifier with `@` and `:` replaced by `_` (for example
`@alice:example.org` → `alice_example.org.json`).

Example configuration (minimal):

```json
{
  "homeserver": "https://matrix.org",
  "user_id": "@you:matrix.org",
  "access_token": "syt_..."
}
```

Tip: Prefer `access_token` over `password` to avoid storing plaintext passwords on disk.

---

Usage examples
--------------

Replace `@you:matrix.org` with your account.

Send a message:

```bash
mtrx-cli -a @you:matrix.org send @bob:matrix.org -m "Hello"
```

Receive pending messages (outputs JSON envelopes):

```bash
mtrx-cli -a @you:matrix.org receive --timeout 5
```

List joined rooms ("groups"):

```bash
mtrx-cli -a @you:matrix.org listGroups
```

Create a room:

```bash
mtrx-cli -a @you:matrix.org createGroup -n "My Group" -m @bob:matrix.org
```

Join a room by alias or ID:

```bash
mtrx-cli -a @you:matrix.org joinGroup --uri '#my-room:matrix.org'
```

Other supported commands include `updateGroup`, `quitGroup`, `listContacts`, `updateProfile`,
`sendTyping`, `sendReaction`, and `deleteMessage`.

---

Daemon mode
-----------

The daemon supports multiple bind modes:

- UNIX socket (default):

```bash
mtrx-cli -a @you:matrix.org daemon --socket
# default socket: /tmp/mtrx-cli-you_matrix.org.sock
```

- TCP:

```bash
mtrx-cli -a @you:matrix.org daemon --tcp localhost:7583
```

- HTTP (JSON-RPC + SSE events):

```bash
mtrx-cli -a @you:matrix.org daemon --http localhost:8080
# JSON-RPC endpoint: POST http://localhost:8080/api/v1/rpc
# SSE events: http://localhost:8080/api/v1/events
```

Start with `-v` (verbose) to enable DEBUG logging for troubleshooting.

---

JSON-RPC (stdin/stdout)
------------------------

The adapter implements newline-delimited JSON-RPC on stdin/stdout, compatible with
`signal-cli jsonRpc`.

```bash
mtrx-cli -a @you:matrix.org jsonRpc
```

Example request:

```json
{"jsonrpc":"2.0","id":1,"method":"send","params":{"recipient":"@bob:matrix.org","message":"Hi"}}
```

---

Troubleshooting
---------------

- If you see `next_batch` validation warnings, check that `access_token` and
  `homeserver` are correct.
- Use `-v` to enable debug logs and inspect the sync loop and SSE handling.

Useful commands:

```bash
mtrx-cli -a @you:matrix.org daemon --http -v
curl -N http://localhost:8080/api/v1/events
```

---

Contributing
------------

Issues and pull requests are welcome. Note that this project is developed as part of a
research effort and receives limited maintenance — we still accept contributions
and will review PRs when possible.

Developer extras and testing dependencies are declared in `pyproject.toml`.

---

License
-------

MIT

---

Project: https://github.com/horlabs/matrix-cli
