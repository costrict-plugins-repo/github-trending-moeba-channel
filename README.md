# moeba-claude-channel

Claude Code channel for [Moeba](https://moeba.co.za) — chat with Claude Code from the Moeba mobile app.

## What it does

Bridges your Claude Code session to the Moeba app. Send messages from your phone, Claude Code processes them and replies back.

```
Moeba App → Moeba Server → SSE → Local MCP Channel → Claude Code
                        ← HTTP reply ← moeba_reply tool ←
```

## Install

```bash
npm install -g moeba-claude-channel
```

## Setup

Add to `~/.claude.json`:

```json
{
  "mcpServers": {
    "moeba": {
      "command": "moeba-claude-channel",
      "args": ["--login"],
      "autoApprove": ["moeba_reply", "moeba_progress"]
    }
  }
}
```

### First run

1. Start Claude Code — the channel starts automatically
2. Browser opens for Google/Apple sign-in (same account as your Moeba app)
3. Credentials cached at `~/.moeba/channel-<project>.json`
4. A **"Claude Code — \<project\>"** agent appears in your Moeba app

### Headless mode (SSH, CI, or setting up for another user)

No browser needed — pass an API key and email via env vars:

```json
{
  "mcpServers": {
    "moeba": {
      "command": "moeba-claude-channel",
      "args": ["--login"],
      "env": {
        "MOEBA_API_KEY": "mba_your_key_here",
        "MOEBA_EMAIL": "user@example.com"
      },
      "autoApprove": ["moeba_reply", "moeba_progress"]
    }
  }
}
```

## Multi-project support

Each project directory gets its own agent in the Moeba app:

- `Claude Code — kepler`
- `Claude Code — moeba`
- `Claude Code — roxy`

The project name is detected from the git repo. Override with `MOEBA_PROJECT` env var.

## Tools

| Tool | Description |
|------|-------------|
| `moeba_reply` | Send a reply back to the Moeba user |
| `moeba_progress` | Show a typing indicator while working |

## Channel notifications (two-way)

The tools work once the MCP server is loaded, but to let Claude Code
**auto-respond to incoming messages** from the app, load it as a channel:

```bash
claude --dangerously-load-development-channels server:moeba
```

For fully unattended operation (no permission prompts — Claude can read/write files, run commands, etc.):

```bash
claude --dangerously-load-development-channels server:moeba --dangerously-skip-permissions
```

> **Warning:** Only use `--dangerously-skip-permissions` in trusted environments. Any message from an authenticated Moeba user can trigger file operations and command execution.

### Channel modes — when does a session become an agent?

- **Every session (simplest):** keep `--login` in the MCP `args` (Setup above).
  Every Claude Code session you open authenticates and registers its own
  `Claude Code — <project>` agent in the app.

- **Selective (recommended when the `moeba` MCP server is user-scoped / loads in
  every session):** if you don't want *every* folder to create an agent, **remove
  `--login` from `args`** and gate on the `MOEBA_CHANNEL` env var — set it only
  when you actually want a channel:

  ```bash
  alias moeba='MOEBA_CHANNEL=1 claude --dangerously-load-development-channels server:moeba'
  ```

  Then plain `claude` loads the tools but creates **no** agent; running `moeba`
  starts a channel session with its own per-folder agent. Auth still uses the
  cached creds / browser sign-in / `MOEBA_API_KEY`+`MOEBA_EMAIL` as configured.

## Requirements

- [Claude Code](https://claude.ai/code) v2.1.80+
- [Moeba](https://moeba.co.za) account (free)
- Node.js 20+

## Links

- [Moeba App (iOS)](https://apps.apple.com/za/app/moeba/id6758993423)
- [Moeba App (Android)](https://play.google.com/store/apps/details?id=za.co.moeba.app)
- [Moeba Web](https://web.moeba.co.za)
