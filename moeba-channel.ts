#!/usr/bin/env node
/**
 * Moeba Channel for Claude Code
 *
 * Two-way bridge: Moeba app users ↔ Claude Code session.
 * Authenticates via OAuth on startup, connects via SSE, replies via HTTP.
 *
 * Usage:
 *   npx @moeba/claude-channel
 *
 * On first run, opens browser for Google/Apple sign-in before connecting.
 * Credentials are cached per project at ~/.moeba/channel-<project>.json
 */
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';
import { createServer } from 'http';
import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'fs';
import { homedir } from 'os';
import { join } from 'path';
import { execSync, exec } from 'child_process';
import { createHash } from 'crypto';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const MOEBA_API_URL = (
  process.env.MOEBA_API_URL ||
  'https://moeba-api-999642860678.africa-south1.run.app'
).replace(/\/$/, '');

const MOEBA_AUTH_URL =
  process.env.MOEBA_AUTH_URL || 'https://admin.moeba.co.za';

// Optional explicit project override (CI / headless / power users):
//   --project <name> CLI flag  >  MOEBA_PROJECT env.
// When NEITHER is set, the channel does NOT guess a project from the folder —
// it uses a single creds file and the agent the user picks in the login screen.
// This is the clean default: one channel, one chosen agent, same everywhere.
function projectFromArgs(): string | undefined {
  const argv = process.argv;
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--project' && argv[i + 1]) return argv[i + 1];
    if (argv[i].startsWith('--project=')) return argv[i].slice('--project='.length);
  }
  return undefined;
}

// Private agent-to-agent mesh call-sign (separate from the human channel).
// Set via --agent <callSign> or MOEBA_AGENT. When present, this session joins
// the hidden mesh under that call-sign: discoverable by, and able to converse
// two-way with, other agents — never visible to human Moeba users.
function flagValue(flag: string): string | undefined {
  const argv = process.argv;
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === flag && argv[i + 1]) return argv[i + 1];
    if (argv[i].startsWith(`${flag}=`)) return argv[i].slice(flag.length + 1);
  }
  return undefined;
}
let meshCallSign: string = (flagValue('--agent') || process.env.MOEBA_AGENT || '')
  .trim().toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');

// WHERE the effective call-sign came from. A folder-derived one is silently
// correct in most projects and silently WRONG when a session runs from a
// directory that doesn't match the identity it means to act as — which is
// exactly how a subscription can be pointed at a call-sign nobody is listening
// on. Tracking the provenance lets moeba_whoami say so out loud.
type CallSignSource = 'flag' | 'joined' | 'folder' | 'none';
let callSignSource: CallSignSource = meshCallSign ? 'flag' : 'none';
// Liveness of the inbound mesh stream + when it last actually carried something,
// so "why am I not receiving events?" is answerable without guesswork.
let meshStreamConnected = false;
let lastMeshEventAt: number | null = null;

// Explicit project override (a channel-enabling signal): --project / MOEBA_PROJECT.
const EXPLICIT_PROJECT: string | undefined = projectFromArgs() || process.env.MOEBA_PROJECT || undefined;

// Channel mode gate. ONLY a real channel session authenticates, provisions its
// agent, and connects the human + mesh SSE. Plain `claude` in any folder loads
// the tools but creates NO agent in the Moeba app. We can't auto-detect channel
// mode from the MCP handshake (the client reports empty capabilities), so we
// treat any explicit channel intent as the signal:
//   • MOEBA_CHANNEL=1           (set by the `moeba` launch alias)
//   • --login                   (interactive browser sign-in)
//   • --agent / MOEBA_AGENT     (joining the agent mesh)
//   • --project / MOEBA_PROJECT (explicit per-project pin)
//
// NOTE: MOEBA_API_KEY / MOEBA_EMAIL are deliberately NOT signals — they live in
// the user-scoped mcpServers.moeba env, so they're present in EVERY claude
// session. Keying off them would make the gate always-on (the bug we're fixing).
// They're still used for headless token refresh once we're in channel mode.
const CHANNEL_MODE =
  !!process.env.MOEBA_CHANNEL ||
  process.argv.includes('--login') ||
  process.argv.includes('--relogin') ||
  !!meshCallSign ||
  !!EXPLICIT_PROJECT;

// Per-folder identity (channel mode only): when not explicitly pinned, derive the
// project from the git repo / folder so each folder binds its OWN human agent/chat
// (creds cached at ~/.moeba/channel-<project>.json). This is what makes phone
// messages go to one session instead of broadcasting to a shared connection.
function detectProjectName(): string {
  try {
    const top = execSync('git rev-parse --show-toplevel', {
      stdio: ['ignore', 'pipe', 'ignore'],
    })
      .toString()
      .trim();
    if (top) return top.split('/').pop() || 'claude-code';
  } catch {}
  return process.cwd().split('/').pop() || 'claude-code';
}

// undefined when not a channel session — auth never runs, so no agent provisioned.
const PROJECT_NAME: string | undefined = CHANNEL_MODE
  ? EXPLICIT_PROJECT || detectProjectName()
  : undefined;
const PROJECT_SLUG = PROJECT_NAME ? PROJECT_NAME.toLowerCase().replace(/[^a-z0-9]+/g, '-') : '';
// Per-project creds file (channel mode); a non-channel session uses none.
const CREDENTIALS_PATH = join(
  homedir(),
  '.moeba',
  PROJECT_SLUG ? `channel-${PROJECT_SLUG}.json` : 'channel.json',
);

interface Credentials {
  token: string;
  email: string;
  businessId: string;
  agentId: string;
  connectionId: string;
  agentApiKey: string;
  projectName: string;
  // Mesh call-sign persists alongside the human agent so restarts auto-rejoin
  // the mesh under the same identity (defaults to the folder identity).
  meshCallSign?: string;
}

// ---------------------------------------------------------------------------
// Credential storage
// ---------------------------------------------------------------------------
function loadCredentials(): Credentials | null {
  try {
    if (!existsSync(CREDENTIALS_PATH)) return null;
    return JSON.parse(readFileSync(CREDENTIALS_PATH, 'utf-8'));
  } catch {
    return null;
  }
}

function saveCredentials(c: Credentials): void {
  const dir = join(homedir(), '.moeba');
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  writeFileSync(CREDENTIALS_PATH, JSON.stringify(c, null, 2));
}

// Module-level credentials so the SSE loop and tool handlers always read the
// freshest token after a refresh.
let creds: Credentials | null = null;
// Inbound messages per connection awaiting proof the agent picked them up —
// acked by Claude's first Moeba tool call (see noteAgentActivity).
const pendingActivity = new Map<string, string[]>();

// ---------------------------------------------------------------------------
// Session token (JWT) expiry — the session token is short-lived (~30 days),
// so we refresh it proactively rather than wedging on a dead token.
// ---------------------------------------------------------------------------
function decodeJwtExp(token: string): number | null {
  try {
    const part = token.split('.')[1];
    if (!part) return null;
    const json = Buffer.from(
      part.replace(/-/g, '+').replace(/_/g, '/'),
      'base64',
    ).toString('utf-8');
    const payload = JSON.parse(json);
    return typeof payload.exp === 'number' ? payload.exp : null;
  } catch {
    return null;
  }
}

// True when the token expires within `skewSeconds`. Returns false if the token
// has no decodable exp — in that case we let the server be the judge (the SSE
// 401 handler is the backstop) rather than forcing a refresh on every connect.
function isTokenExpired(token: string, skewSeconds = 300): boolean {
  const exp = decodeJwtExp(token);
  if (exp === null) return false;
  return Date.now() / 1000 >= exp - skewSeconds;
}

// ---------------------------------------------------------------------------
// OAuth flow — opens browser, receives Firebase token via localhost callback
// ---------------------------------------------------------------------------
function authenticate(): Promise<Credentials> {
  return new Promise((resolve, reject) => {
    const callbackPort = 9876;

    const httpServer = createServer(async (req, res) => {
      const url = new URL(req.url!, `http://localhost:${callbackPort}`);

      if (url.pathname === '/callback') {
        const firebaseIdToken = url.searchParams.get('token');
        // The picker returns the user's chosen agent (id + business). When
        // present we bind to that agent; otherwise we fall back to projectName.
        const chosenAgentId = url.searchParams.get('agentId') || undefined;
        const chosenBusinessId = url.searchParams.get('businessId') || undefined;

        if (!firebaseIdToken) {
          res.writeHead(400);
          res.end('Missing token parameter');
          return;
        }

        try {
          const response = await fetch(`${MOEBA_API_URL}/channel/auth`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              firebaseIdToken,
              projectName: PROJECT_NAME,
              agentId: chosenAgentId,
              businessId: chosenBusinessId,
            }),
          });

          if (!response.ok) {
            const err = await response.text();
            res.writeHead(500);
            res.end(`Auth failed: ${err}`);
            reject(new Error(`Channel auth failed: ${err}`));
            return;
          }

          const data = (await response.json()) as any;
          const newCreds: Credentials = {
            token: data.token,
            email: data.email,
            businessId: data.businessId,
            agentId: data.agentId,
            connectionId: data.connectionId,
            // The server only returns the agent key when it mints a new one (it
      // stores only the hash) — keep the known-good key instead of nulling it.
      agentApiKey: data.agentApiKey ?? creds?.agentApiKey ?? null,
            projectName: PROJECT_NAME || creds?.projectName || '',
            // Server is the source of truth for the mesh identity; fall back to
            // the previously-persisted value, never silently to the folder slug.
            meshCallSign: data.meshCallSign ?? creds?.meshCallSign,
          };

          saveCredentials(newCreds);

          res.writeHead(200, { 'Content-Type': 'text/html' });
          res.end(`
            <html><body style="font-family:system-ui;text-align:center;padding:60px">
              <h2>Connected to Moeba!</h2>
              <p>You can close this tab and return to Claude Code.</p>
            </body></html>
          `);

          httpServer.close();
          resolve(newCreds);
        } catch (err: any) {
          res.writeHead(500);
          res.end(`Error: ${err.message}`);
          reject(err);
        }
        return;
      }

      res.writeHead(404);
      res.end('Not found');
    });

    httpServer.listen(callbackPort, '127.0.0.1', () => {
      const authUrl = `${MOEBA_AUTH_URL}/channel-login?redirect=http://localhost:${callbackPort}/callback`;
      console.error(`\n🔑 Sign in to Moeba: ${authUrl}\n`);

      const cmd =
        process.platform === 'darwin'
          ? 'open'
          : process.platform === 'win32'
            ? 'start'
            : 'xdg-open';
      exec(`${cmd} "${authUrl}"`);
    });

    setTimeout(() => {
      httpServer.close();
      reject(new Error('Authentication timed out after 2 minutes'));
    }, 120_000);
  });
}

// ---------------------------------------------------------------------------
// Headless auth — API key + email (no browser). Used for first-run headless
// setup and for unattended token refresh once the session token expires.
// ---------------------------------------------------------------------------
async function authenticateViaApiKey(
  apiKey: string,
  email: string,
): Promise<Credentials | null> {
  try {
    const response = await fetch(`${MOEBA_API_URL}/channel/auth`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ apiKey, email, projectName: PROJECT_NAME }),
    });
    if (!response.ok) {
      console.error(`API key auth failed: ${await response.text()}`);
      return null;
    }
    const data = (await response.json()) as any;
    const newCreds: Credentials = {
      token: data.token,
      email: data.email,
      businessId: data.businessId,
      agentId: data.agentId,
      connectionId: data.connectionId,
      // The server only returns the agent key when it mints a new one (it
      // stores only the hash) — keep the known-good key instead of nulling it.
      agentApiKey: data.agentApiKey ?? creds?.agentApiKey ?? null,
      projectName: PROJECT_NAME || '',
      meshCallSign: data.meshCallSign ?? creds?.meshCallSign,
    };
    saveCredentials(newCreds);
    return newCreds;
  } catch (err: any) {
    console.error(`API key auth error: ${err.message}`);
    return null;
  }
}

// Re-mint the short-lived session token when it expires. The agent API key
// stored alongside it is long-lived and re-mints a token unattended, so a
// configured channel self-heals with no user login, env vars, or browser.
// Order: stored agent key → env API key (first-run) → browser OAuth.
//
// `requireFreshAgentKey`: headless auth (tiers 1 & 2) can come back "ok" while
// silently keeping the SAME agentApiKey — the server only mints a new one on
// interactive login, so a headless refresh of an already-provisioned agent
// just re-confirms the session JWT and hands back the old (possibly dead) key
// unchanged. A caller recovering from a 401 on that exact key needs to know
// the refresh didn't actually fix anything, so it can keep escalating to the
// browser instead of stopping at a "successful" no-op.
async function refreshCredentials(opts: {
  allowBrowser: boolean;
  requireFreshAgentKey?: boolean;
}): Promise<Credentials | null> {
  const originalAgentKey = creds?.agentApiKey;
  const isFresh = (c: Credentials | null) =>
    !opts.requireFreshAgentKey || !!(c?.agentApiKey && c.agentApiKey !== originalAgentKey);
  let best: Credentials | null = null;

  // 1. Best path: the already-stored agent key is non-expiring and works
  //    unattended inside the SSE reconnect loop.
  if (creds?.agentApiKey && creds?.email) {
    console.error('Refreshing Moeba session token via stored agent key...');
    const refreshed = await authenticateViaApiKey(creds.agentApiKey, creds.email);
    if (refreshed) {
      if (isFresh(refreshed)) return refreshed;
      best = refreshed;
    }
  }

  // 2. Headless env credentials (e.g. first run before anything is cached).
  const envApiKey = process.env.MOEBA_API_KEY;
  const envEmail = process.env.MOEBA_EMAIL;
  if (envApiKey && envEmail) {
    console.error('Refreshing Moeba session token via API key...');
    const refreshed = await authenticateViaApiKey(envApiKey, envEmail);
    if (refreshed) {
      if (isFresh(refreshed)) return refreshed;
      best = refreshed;
    }
  }

  // 3. Last resort: interactive browser OAuth — the only path that always
  //    mints a genuinely fresh agent key.
  if (opts.allowBrowser) {
    console.error('Refreshing Moeba session token — opening browser...');
    try {
      return await authenticate();
    } catch (err: any) {
      console.error(`Re-authentication failed: ${err.message}`);
      return best;
    }
  }

  if (best) {
    console.error('Could not silently obtain a fresh agent key — session token refreshed, but agent-key calls will keep failing until an interactive login.');
    return best;
  }

  console.error(
    'Moeba session token expired and cannot refresh — reconnect with --login or set MOEBA_API_KEY + MOEBA_EMAIL.',
  );
  return null;
}

// ---------------------------------------------------------------------------
// Lazy interactive auth — the browser tab only opens the first time it's
// actually needed (a moeba_* tool call with no silently-refreshable creds),
// never automatically on every process start/restart. Silent recovery
// (stored agent key / env key) still runs eagerly at startup in main() — this
// only gates the disruptive "open a browser" step.
// ---------------------------------------------------------------------------
let sseStarted = false;
let interactiveAuthPromise: Promise<Credentials | null> | null = null;

function startSSEIfNeeded(mcp: Server): void {
  if (sseStarted || !creds) return;
  sseStarted = true;
  connectSSE(mcp);
  console.error('Moeba channel ready — waiting for messages');
  if (meshCallSign) connectMeshSSE(mcp);
}

// `force` skips the "JWT not expired" short-circuit. The session JWT's local
// expiry says nothing about whether the separate long-lived agentApiKey is
// still valid server-side (it can be rotated out from under us by another
// login at any time) — callers that just got a 401 on a request authenticated
// with the agent key need to force a real re-check, not trust the cache.
async function ensureAuthenticated(mcp: Server, opts: { force?: boolean } = {}): Promise<Credentials | null> {
  if (!opts.force && creds && !isTokenExpired(creds.token)) return creds;
  if (interactiveAuthPromise) return interactiveAuthPromise;

  interactiveAuthPromise = (async () => {
    try {
      const allowBrowser = process.argv.includes('--login');
      // force === recovering from a live 401 on the agent key: a refresh that
      // silently keeps the same dead key isn't good enough, so keep escalating.
      const refreshed = await refreshCredentials({ allowBrowser, requireFreshAgentKey: opts.force });
      if (refreshed) {
        creds = refreshed;
        console.error(`Authenticated as ${creds.email} (project: ${PROJECT_NAME})`);
        startSSEIfNeeded(mcp);
      }
      return creds;
    } finally {
      interactiveAuthPromise = null;
    }
  })();

  return interactiveAuthPromise;
}

// Calls a moeba API endpoint authenticated with the current agent API key. If
// the server rejects the key (401 — e.g. it was rotated out by another
// login), forces a real re-authentication (silent first, interactive browser
// as the lazy last resort) and retries the call once with whatever key comes
// back. This is the only way to recover from a rotated key: nothing about the
// locally-cached session JWT tells us the agent key itself went stale.
async function agentFetch(
  mcp: Server,
  path: string,
  init: { method: string; body?: string },
): Promise<{ ok: boolean; status: number; text: string }> {
  if (!creds?.agentApiKey) {
    return { ok: false, status: 401, text: 'Not authenticated yet — reconnect the moeba channel.' };
  }

  const doFetch = async () => {
    const res = await fetch(`${MOEBA_API_URL}${path}`, {
      ...init,
      headers: { 'Content-Type': 'application/json', 'X-Moeba-Agent-Key': creds!.agentApiKey },
    });
    return { ok: res.ok, status: res.status, text: await res.text() };
  };

  let result = await doFetch();
  if (result.status === 401) {
    console.error(`Moeba agent key rejected (401) on ${path} — forcing re-authentication...`);
    const staleKey = creds?.agentApiKey;
    await ensureAuthenticated(mcp, { force: true });
    if (creds?.agentApiKey && creds.agentApiKey !== staleKey) {
      result = await doFetch();
    }
  }
  return result;
}

// ---------------------------------------------------------------------------
// SSE client — connects to Moeba and receives messages
// ---------------------------------------------------------------------------
function connectSSE(mcp: Server): void {
  let reconnectDelay = 1000;

  async function connect() {
    if (!creds) {
      console.error('Moeba SSE: no credentials — not connecting.');
      return;
    }
    try {
      // Proactively refresh a token that's expired/near-expiry so we don't
      // burn a guaranteed-401 connect attempt.
      if (isTokenExpired(creds.token)) {
        const refreshed = await refreshCredentials({ allowBrowser: false });
        if (refreshed) creds = refreshed;
      }

      const c = creds!; // snapshot for the life of this connection
      const url = `${MOEBA_API_URL}/api/channel/events?connectionId=${c.connectionId}`;
      console.error('Connecting to Moeba SSE...');
      const response = await fetch(url, {
        headers: { Authorization: `Bearer ${c.token}` },
      });

      if (!response.ok) {
        console.error(`SSE connection failed: ${response.status}`);
        // 401/403 → the session token was rejected. Re-authenticate (headless
        // only — we're past startup with no TTY) and reconnect promptly.
        if (response.status === 401 || response.status === 403) {
          const refreshed = await refreshCredentials({ allowBrowser: false });
          if (refreshed) {
            creds = refreshed;
            reconnectDelay = 1000;
          }
        }
        scheduleReconnect();
        return;
      }

      console.error('Connected to Moeba — listening for messages');
      reconnectDelay = 1000;

      const reader = response.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      // Parser state MUST persist across read() chunks — a long event can span
      // multiple chunks (event: line in one, data: in the next). Resetting these
      // per chunk silently drops large messages.
      let eventType = '';
      let eventData = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });

        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (line.startsWith('event: ')) {
            eventType = line.slice(7);
          } else if (line.startsWith('data: ')) {
            eventData = line.slice(6);
          } else if (line === '' && eventType && eventData) {
            if (eventType === 'message') {
              try {
                const event = JSON.parse(eventData);

                // Approve/Deny verdict from the app's approvalRequest widget —
                // relay to Claude Code as a permission verdict, not a chat message.
                if (
                  event.type === 'action' &&
                  event.action?.actionId === 'approval_response'
                ) {
                  const d = event.action.data || {};
                  mcp
                    .notification({
                      method: 'notifications/claude/channel/permission',
                      params: {
                        request_id: String(d.requestId || '').toLowerCase(),
                        behavior: d.behavior === 'allow' ? 'allow' : 'deny',
                      },
                    })
                    .catch((err: any) =>
                      console.error('Permission verdict failed:', err.message),
                    );
                  eventType = '';
                  eventData = '';
                  continue;
                }

                const connId = event.connectionId || c.connectionId;
                let content = event.message?.text || '';
                // Files the user attached in the Moeba app (images, PDFs, …) arrive
                // as URLs (Moeba-hosted or business-hosted). Download each to a
                // local file so the agent can open it with the Read tool — same
                // pattern as inbound mesh attachments.
                const chatAtts = Array.isArray(event.message?.attachments) ? event.message.attachments : [];
                if (chatAtts.length) {
                  const dir = join(homedir(), '.moeba', 'chat-attachments');
                  try { if (!existsSync(dir)) mkdirSync(dir, { recursive: true }); } catch {}
                  const saved: string[] = [];
                  for (const att of chatAtts) {
                    try {
                      if (!att?.url) continue;
                      const r = await fetch(att.url);
                      if (!r.ok) continue;
                      const buf = Buffer.from(await r.arrayBuffer());
                      const safe = String(att.name || att.url.split('?')[0].split('/').pop() || 'file')
                        .replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 80) || 'file';
                      const p = join(dir, `${Date.now()}-${safe}`);
                      writeFileSync(p, buf);
                      saved.push(p);
                    } catch { /* skip a failed attachment */ }
                  }
                  if (saved.length) {
                    content += (content ? '\n\n' : '') +
                      saved.map((p) => `[file attached → ${p} — use the Read tool to view it]`).join('\n');
                  }
                }
                const meta: Record<string, string> = {
                  sender_email: event.senderEmail || '',
                  sender_name: event.senderName || '',
                  connection_id: connId,
                  conversation_id: event.conversationId || '',
                };
                if (event.type === 'action') {
                  meta.type = 'action';
                }
                // Scheduled trigger fired by Moeba's cron scheduler — not a human
                // typing. Tell the agent so it acts on the task and reports back
                // with moeba_reply (the user is expecting the result on their phone).
                if (event.cron) {
                  meta.type = 'cron';
                  meta.cron_job_id = String(event.cron.cronJobId || '');
                  const n = Number(event.cron.runCount || 0) + 1;
                  content =
                    `[Scheduled task fired by Moeba (job ${event.cron.cronJobId}, run #${n}). ` +
                    `Do the task below, then send the result to the user with moeba_reply.]\n\n` +
                    content;
                }

                // No ack and no spinner here: reaching this process proves nothing
                // about the agent. Both fire from Claude's first Moeba tool call on
                // this connection (see noteAgentActivity). Cron triggers carry no
                // messageId but still get the "Working on it" indicator that way.
                const pending = pendingActivity.get(connId) || [];
                if (event.messageId) pending.push(String(event.messageId));
                pendingActivity.set(connId, pending);

                mcp
                  .notification({
                    method: 'notifications/claude/channel',
                    params: { content, meta },
                  })
                  .catch((err: any) =>
                    console.error('Notification failed:', err.message),
                  );
              } catch {}
            } else if (eventType === 'connected') {
              console.error('SSE stream established');
            }
            eventType = '';
            eventData = '';
          }
        }
      }

      console.error('SSE stream ended');
      scheduleReconnect();
    } catch (err: any) {
      console.error(`SSE error: ${err.message}`);
      scheduleReconnect();
    }
  }

  function scheduleReconnect() {
    console.error(`Reconnecting in ${reconnectDelay / 1000}s...`);
    setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 30_000);
  }

  connect();
}

// ---------------------------------------------------------------------------
// Mesh SSE — private agent-to-agent inbound stream (keyed by call-sign).
// Messages are injected into Claude tagged source="agent-mesh" so the session
// knows to reply with moeba_mesh_send (not moeba_reply). Never touches the
// human conversation/push.
// ---------------------------------------------------------------------------
let meshAbort: AbortController | null = null;

function connectMeshSSE(mcp: Server): void {
  if (!meshCallSign || !creds?.agentApiKey) return;
  // Tear down any existing mesh stream (e.g. the agent joined a new call-sign).
  if (meshAbort) { try { meshAbort.abort(); } catch {} }
  const myAbort = new AbortController();
  meshAbort = myAbort;
  const myCallSign = meshCallSign;
  let reconnectDelay = 1000;

  async function connect() {
    if (myAbort.signal.aborted || meshCallSign !== myCallSign) return; // superseded
    try {
      const url = `${MOEBA_API_URL}/agent-mesh/events?key=${encodeURIComponent(creds!.agentApiKey)}&callSign=${encodeURIComponent(myCallSign)}`;
      const res = await fetch(url, { headers: { Accept: 'text/event-stream' }, signal: myAbort.signal });
      if (!res.ok || !res.body) { scheduleReconnect(); return; }
      const reader = (res.body as any).getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      // Parser state persists across read() chunks (long events span chunks).
      let eventType = '';
      let eventData = '';
      const divergent = PROJECT_SLUG && myCallSign !== PROJECT_SLUG ? ` (folder=${PROJECT_SLUG})` : '';
      console.error(`Mesh: joined as "${myCallSign}"${divergent}`);
      meshStreamConnected = true;
      reconnectDelay = 1000;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';
        for (const line of lines) {
          if (line.startsWith('event: ')) eventType = line.slice(7);
          else if (line.startsWith('data: ')) eventData = line.slice(6);
          else if (line === '' && eventType && eventData) {
            if (eventType === 'message') {
              try {
                const m = JSON.parse(eventData);
                lastMeshEventAt = Date.now();
                let content = m.message || '';
                // Download any attachments locally so the agent can Read (view) them.
                const atts = Array.isArray(m.attachments) ? m.attachments : [];
                if (atts.length) {
                  const dir = join(homedir(), '.moeba', 'mesh-attachments');
                  try { if (!existsSync(dir)) mkdirSync(dir, { recursive: true }); } catch {}
                  const saved: string[] = [];
                  for (const att of atts) {
                    try {
                      const r = await fetch(att.url);
                      if (!r.ok) continue;
                      const buf = Buffer.from(await r.arrayBuffer());
                      const safe = String(att.name || 'image').replace(/[^a-zA-Z0-9._-]/g, '_') || 'image';
                      const p = join(dir, `${Date.now()}-${safe}`);
                      writeFileSync(p, buf);
                      saved.push(p);
                    } catch { /* skip a failed attachment */ }
                  }
                  if (saved.length) {
                    content += (content ? '\n\n' : '') +
                      saved.map((p) => `[image attached → ${p} — use the Read tool to view it]`).join('\n');
                  }
                }
                mcp.notification({
                  method: 'notifications/claude/channel',
                  params: {
                    content,
                    meta: { source: 'agent-mesh', from_agent: m.from || '', to_agent: myCallSign },
                  },
                }).catch(() => {});
              } catch {}
            }
            eventType = '';
            eventData = '';
          }
        }
      }
      scheduleReconnect();
    } catch {
      scheduleReconnect();
    }
  }

  function scheduleReconnect() {
    meshStreamConnected = false;
    if (myAbort.signal.aborted || meshCallSign !== myCallSign) return;
    setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 30_000);
  }

  connect();
}

// ---------------------------------------------------------------------------
// Main — authenticate first, then connect MCP
// ---------------------------------------------------------------------------
async function main() {
  // Auth modes: (a) cached creds file, (b) API key + email env (headless),
  // (c) browser OAuth (--login). Load the cached creds synchronously now (fast,
  // no network) so a warm start (the common /mcp reconnect case) has creds
  // immediately. The heavier auth (token refresh / browser login / api-key
  // exchange) runs in the BACKGROUND after mcp.connect — see below — so the MCP
  // handshake NEVER blocks and can't time out.
  const loginMode = process.argv.includes('--login');
  // Explicit forced re-login: a human ran this directly (e.g. to recover from
  // a dead agentApiKey) and is standing there to complete a browser flow.
  // Unlike plain --login, this ALWAYS opens the browser, even if a cached
  // session JWT still looks unexpired — that's precisely the state that
  // leaves a stale-but-locally-valid-looking agentApiKey stuck forever,
  // since nothing in a bare terminal run ever calls a moeba_* tool to trip
  // the lazy re-auth in ensureAuthenticated/agentFetch.
  const forceRelogin = process.argv.includes('--relogin');
  const envApiKey = process.env.MOEBA_API_KEY;
  const envEmail = process.env.MOEBA_EMAIL;

  // Only a channel session carries credentials. A plain `claude` session loads
  // no creds and never authenticates, so it provisions no agent in the app.
  creds = CHANNEL_MODE ? loadCredentials() : null;

  // The server only returns the agent key when it MINTS one, so a re-auth of an
  // existing agent leaves agentApiKey null in the cache — which would break every
  // outbound tool (reply/progress/mesh/proxy) even though inbound SSE still works
  // off the token. Fall back to the user-scoped env key (mcpServers.moeba env) so
  // tools can always authenticate against the business.
  if (creds && !creds.agentApiKey && process.env.MOEBA_API_KEY) {
    creds.agentApiKey = process.env.MOEBA_API_KEY;
  }

  // Mesh identity persists like the human agent. Effective call-sign priority:
  //   explicit --agent / MOEBA_AGENT  >  previously-joined (persisted in creds)
  //   >  the folder identity (same as the human agent's project).
  // With this set, the warm-path / background SSE connect auto-rejoins the mesh
  // on every restart — no env var needed. An explicit moeba_mesh_join still
  // overrides and persists the new call-sign.
  if (CHANNEL_MODE && !meshCallSign) {
    meshCallSign = (creds?.meshCallSign || PROJECT_SLUG || '').trim();
    callSignSource = creds?.meshCallSign ? 'joined' : (meshCallSign ? 'folder' : 'none');
  }

  // 2. Create MCP channel server
  const mcp = new Server(
    { name: 'moeba', version: '0.0.2' },
    {
      capabilities: {
        experimental: {
          'claude/channel': {},
          // Permission relay: Claude Code forwards tool-approval prompts so the
          // user can approve/deny from the Moeba app (terminal dialog stays
          // live too; first answer wins). Sender is trusted — the Moeba app
          // authenticates the connection owner via Firebase + session JWT.
          'claude/channel/permission': {},
        },
        tools: {},
      },
      instructions: `Messages from Moeba users arrive as <channel source="moeba" ...>.
Each message has attributes: sender_email, sender_name, connection_id, conversation_id.

When you receive a message:
1. FIRST, before anything else, call moeba_progress with the connection_id and a
   short status (e.g. "Reading your message..."). This is what tells Moeba the
   message actually reached you — it turns the sender's delivery ticks blue and
   shows a thinking indicator. Skipping it means the user sees nothing until your
   reply. Do this even for trivial one-line answers.
2. Read and understand the user's request
3. Take whatever actions are needed (read files, run commands, etc.)
4. Send further moeba_progress updates while working on longer tasks
5. Reply using moeba_reply with the connection_id from the message tag

You can also START a conversation with no inbound message: call moeba_reply with
just \`text\` (no connection_id, no email) and it goes to the logged-in Moeba user.
Use this for scheduled/cron output or when unattended work needs a human —
a scheduled job failed, an OAuth token expired, a deploy needs approval — rather
than failing silently where nobody sees it.

IMPORTANT: The user is on a mobile app and CANNOT approve terminal permissions.
- Prefer reading files (Read tool) over running commands (Bash) when possible
- Use Glob, Grep, and Read tools which don't need permission
- If you must run a command, warn the user it needs terminal approval
- NEVER get stuck silently — if something needs approval, send a moeba_reply explaining that the action requires terminal approval
- Keep replies concise — the user is on a small screen

Connected accounts: the moeba_email_*, moeba_calendar_*, moeba_contacts and
moeba_connections tools access the user's connected Gmail/Outlook/Calendar via
Moeba's proxy (Moeba stores and refreshes the OAuth tokens). connection_id is
optional — it defaults to this project's connection. If a tool returns
"not_connected", connect the account: send a moeba_reply with an oauthConnect
component using a CONCRETE provider — "gmail" for Google (email + calendar) or
"office365" for Microsoft. The error's provider field may say the generic
"email"/"calendar"; map it to gmail (or office365), never pass it verbatim.

Email attachments/documents workflow (bank statements, invoices, PDFs, …):
1. Hunt: moeba_email_search with filename ("pdf" / "statement.pdf") and/or label.
2. Locate: moeba_email_read lists each message's attachments
   ({attachment_id, filename, mime_type, size_bytes}) + threadId;
   moeba_email_thread shows a whole conversation's per-message attachments in one
   call ("which reply carried the PDFs").
3. Fetch: moeba_email_attachment downloads ONE file to local disk (sha256-verified)
   and returns the path + a short-lived pre-signed URL — bytes never enter model
   context. raw:true exports the original message as .eml for audit/archive.
4. Forward: moeba_email_send accepts attachments as {messageId, attachment_id}
   re-references — the file is copied server-side, no download/re-upload.

Widgets: moeba_reply accepts an optional components array to render interactive
widgets in the Moeba app chat alongside (or instead of) text:
- { type: "oauthConnect", provider, scopes?, label?, description? }
  — button that connects the user's account (use after a "not_connected" error).
  provider MUST be a concrete provider: "gmail" (Google — covers BOTH email and
  calendar) or "office365" (Microsoft). Do NOT pass the generic category "email"
  or "calendar" — if a not_connected error reports provider "email"/"calendar",
  translate it: email→gmail (or office365), calendar→gmail.
- { type: "secretInput", name, label, description? }
  — masked input for the user to submit an API key/secret securely
- { type: "approvalRequest", requestId, toolName, description, inputPreview? }
  — Approve/Deny card; the decision comes back as an action event. Tool-permission
  prompts are relayed to the app automatically — you don't need to send these
  yourself unless you want a custom approval step.

Build/deploy notifications (pub/sub by GitHub repo, scoped to YOUR call-sign):
- First moeba_mesh_join (subscriptions are tied to your call-sign). Then:
  • moeba_subscribe({repos:["owner/repo", "owner/*", "*"]}) — start receiving a repo's
    statuses (exact name, "owner/*" wildcard, bare repo name, or "*" for everything).
  • moeba_unsubscribe({repos:[...]}) — stop receiving them.
  • moeba_subscriptions() — list YOUR subscriptions.
  Each agent subscribes only to the repos it cares about; delivery is PRIVATE to
  your call-sign over the mesh — other agents (even on the same login) do NOT get
  your subscribed repos' pings. When the user says "tell me when Kepler deploys",
  call moeba_subscribe with the matching pattern; updates then arrive tagged
  source="agent-mesh" from_agent="deploy" in real time.
- Flaredeck card activity (same subscribe mechanism, different topics): subscribe to
  "flaredeck/board/<board>", "flaredeck/card/<cardCode>", "flaredeck/tenant/<id>", or
  "flaredeck/*" to get real-time Flaredeck card events — comments, lane moves, assigns,
  created/done — in your inbox tagged source="agent-mesh" from_agent="flaredeck". This
  lets you react to board activity WITHOUT polling Flaredeck.
- SCHEDULING (moeba_schedule / moeba_schedules / moeba_unschedule): Moeba keeps a
  durable scheduler. When the user asks for something recurring ("every morning at
  7 send me…", "remind me Friday", "check the build every hour") call moeba_schedule
  with a cron expression (or run_at for one-shot) and a prompt describing the task.
  Moeba fires the prompt into this session at the scheduled time as a channel message
  tagged type="cron"; do the task, then report to the user with moeba_reply. Jobs
  survive session restarts (they live in Moeba, not in this process). If no session
  is connected when a job fires, the user gets a push saying it was missed. Prefer
  moeba_schedule over Claude Code's own cron for anything that must outlive this
  session.
- To SET UP CI publishing (GitHub Action, build server, cron, etc.) — no agent/SDK
  needed, just a stable token. Call moeba_deploy_token() to fetch the business's
  deploy token + a ready-to-paste workflow step; add the token as a GitHub Actions
  secret (MOEBA_DEPLOY_TOKEN). The publisher then POSTs:
    curl -X POST "<MOEBA_API_URL>/deploy/notify?repo=<owner/repo>&status=<status>" \\
         -H "X-Moeba-Deploy-Token: <MOEBA_DEPLOY_TOKEN>"
  The deploy token is business-scoped and NEVER rotates on login (unlike an agent
  key), so it never silently 401s. One token covers every repo. (Legacy CI using
  -H "X-Moeba-Agent-Key: <key>" still works but rotates on interactive login.)
  Optional query params: app (label), env (PROD/QA/…), sha, run (CI link),
  message (overrides the composed text). status is free-form: success | failure |
  building | <anything>. In GitHub Actions use repo=\${{ github.repository }} and
  status=\${{ job.status }} with an "if: always()" step. Every channel subscribed
  to that repo is notified — publisher and subscriber never reference each other.

Private agent-to-agent mesh (hidden from human users): a separate channel for
agents to talk to each other. It never appears in the human Moeba app and never
sends push notifications.
- moeba_mesh_join({callSign, description}) — JOIN the mesh under a call-sign YOU
  choose (e.g. "kepler", "payinc-orchestrator") with a short description of what
  you do. Call this once at startup to become discoverable + reachable. Re-call
  to rename or update your description.
- moeba_mesh_agents() — list who's in the mesh (call-signs, descriptions, online,
  and how long offline ones have been silent).
- moeba_whoami() — YOUR call-sign, where it came from, whether your inbound stream
  is live, and your subscriptions. Call it before telling any other system which
  call-sign to route to you, and first when expected events aren't arriving: a
  call-sign derived from the folder name is the usual reason events end up in an
  inbox nobody is reading.
- moeba_mesh_send({to, message}) — message another agent by call-sign. Two-way:
  the recipient replies with moeba_mesh_send back to your call-sign.
- Sending images: moeba_mesh_send({to, message, imagePath}) attaches a LOCAL image
  file (uploaded automatically); use imageUrl instead for an already-hosted image.
  message is optional when sending an image.
- Receiving images: when an inbound mesh message includes an image, it is
  downloaded to a local file and the path is appended as
  "[image attached → <path> — use the Read tool to view it]". Use the Read tool on
  that path to actually view the image.
- Inbound mesh messages arrive tagged source="agent-mesh" with from_agent set —
  when you see that tag, reply with moeba_mesh_send (NOT moeba_reply, which is for
  human users). This is how independent context-keeping agents hold ongoing
  conversations and coordinate work.`,
    },
  );

  // 3. Register tools
  // Proxy tools — access the user's connected accounts (Gmail/Outlook/Calendar)
  // through Moeba's proxy API. Same auth as moeba_reply (agent API key); Moeba
  // owns the OAuth tokens and refresh. connection_id defaults to this
  // project's connection so the tools also work without an inbound message.
  const PROXY_TOOLS: Record<
    string,
    {
      endpoint: string;
      description: string;
      properties: Record<string, unknown>;
      required?: string[];
    }
  > = {
    moeba_email_search: {
      endpoint: '/api/proxy/email/search',
      description:
        "Search the Moeba user's connected email (Gmail/Outlook). Returns message summaries with ids for moeba_email_read.",
      properties: {
        query: { type: 'string', description: 'Free-form search query' },
        keywords: { type: 'string', description: 'Keywords to match' },
        from: { type: 'string', description: 'Filter by sender' },
        to: { type: 'string', description: 'Filter by recipient' },
        subject: { type: 'string', description: 'Filter by subject' },
        after: { type: 'string', description: 'Only emails after this date (YYYY/MM/DD)' },
        before: { type: 'string', description: 'Only emails before this date (YYYY/MM/DD)' },
        newer_than: { type: 'string', description: 'Relative window, e.g. "7d" or "1m"' },
        has_attachment: { type: 'boolean' },
        filename: { type: 'string', description: 'Only emails carrying an attachment with this filename or extension (e.g. "pdf" or "statement.pdf") — for document hunting' },
        label: { type: 'string', description: 'Gmail label filter (e.g. "Statements" or "important"); ignored on Outlook' },
        is_unread: { type: 'boolean', description: 'Only unread emails' },
        maxResults: { type: 'number' },
        platform: { type: 'string', description: '"gmail" or "outlook" — omit for all connected' },
        account: { type: 'string', description: 'When several accounts are connected, restrict the search to one — its email or a substring (e.g. "phiresky"). Omit to search all.' },
      },
    },
    moeba_email_read: {
      endpoint: '/api/proxy/email/read',
      description: 'Read a specific email (full body) by id from moeba_email_search. The result includes threadId and an attachments array ({attachment_id, filename, mime_type, size_bytes}) — fetch a file with moeba_email_attachment.',
      properties: {
        messageId: { type: 'string', description: 'Message id from moeba_email_search' },
        platform: { type: 'string', description: '"gmail" or "outlook"' },
        account: { type: 'string', description: 'Which connected account the message id belongs to — its email or a substring (e.g. "phiresky"). Needed when several accounts are connected.' },
      },
      required: ['messageId', 'platform'],
    },
    moeba_email_thread: {
      endpoint: '/api/proxy/email/thread',
      description: 'Fetch a WHOLE email conversation in one call — every message with sender, date, snippet, and per-message attachment metadata. Use to find which reply in a thread carried a file, then fetch it with moeba_email_attachment. Pass threadId (from search/read results) or any messageId in the thread.',
      properties: {
        threadId: { type: 'string', description: 'Thread id (Gmail threadId / Outlook conversationId) from search or read results' },
        messageId: { type: 'string', description: 'Alternatively: any message id in the thread' },
        platform: { type: 'string', description: '"gmail" or "outlook"' },
        account: { type: 'string', description: 'Which connected account — its email or a substring. Needed when several accounts are connected.' },
      },
      required: ['platform'],
    },
    moeba_email_send: {
      endpoint: '/api/proxy/email/send',
      description:
        "Send an email from the user's connected account. Sends as the user — confirm with them before using. Supports attachments. When several accounts are connected (e.g. two Gmail addresses), pass `account` to choose which one to send FROM; otherwise the account marked primary (or, failing that, a deterministic default) is used.",
      properties: {
        to: { type: 'string', description: 'Recipient email address' },
        subject: { type: 'string' },
        body: { type: 'string', description: 'Plain-text body' },
        platform: { type: 'string', description: '"gmail" or "outlook"' },
        account: { type: 'string', description: 'Which connected account to send FROM — its email or a substring (e.g. "phiresky" or "keplerco"). Omit to use the primary/default account. Use moeba_connections to see connected accounts.' },
        from: { type: 'string', description: 'Send as a VERIFIED send-as alias on the chosen account, e.g. "cobus@musai.co.za" or "Cobus <cobus@musai.co.za>". Must already be verified in Gmail → Settings → Accounts → "Send mail as"; unverified addresses are rejected with the permitted list. Omit to send as the account\'s own address. Gmail only — Outlook connections reject it.' },
        attachments: {
          type: 'array',
          description: 'Optional attachments, max 10 / ~25MB total. Each item is ONE of: a LOCAL FILE PATH (string — read and base64-encoded automatically); an object {filename, content (base64), contentType?}; or a RE-REFERENCE to a received attachment {messageId, attachment_id, filename?} — forwarded entirely server-side, no download/re-upload round-trip (add platform/account if the source message lives on a different account than the one sending).',
          items: {},
        },
      },
      required: ['to', 'subject', 'body'],
    },
    moeba_calendar_events: {
      endpoint: '/api/proxy/calendar/events',
      description: "List upcoming events from the user's connected calendar. When several accounts are connected, pass `account` to pick which calendar to read (e.g. Kepler meetings live on the keplerco calendar).",
      properties: {
        maxResults: { type: 'number' },
        timeMin: { type: 'string', description: 'ISO datetime lower bound (default: now)' },
        timeMax: { type: 'string', description: 'ISO datetime upper bound' },
        platform: { type: 'string', description: '"gmail" or "outlook"' },
        account: { type: 'string', description: 'Which connected account/calendar to read — its email or a substring (e.g. "keplerco"). Omit to use the primary/default account.' },
      },
    },
    moeba_calendar_create: {
      endpoint: '/api/proxy/calendar/create',
      description:
        "Create an event on the user's connected calendar. Confirm details with the user before using. When several accounts are connected, pass `account` to choose which calendar.",
      properties: {
        summary: { type: 'string', description: 'Event title' },
        startTime: { type: 'string', description: 'ISO datetime' },
        endTime: { type: 'string', description: 'ISO datetime' },
        description: { type: 'string' },
        location: { type: 'string' },
        attendees: { type: 'array', items: { type: 'string' }, description: 'Attendee emails' },
        platform: { type: 'string', description: '"gmail" or "outlook"' },
        account: { type: 'string', description: 'Which connected account/calendar to create on — its email or a substring (e.g. "keplerco"). Omit to use the primary/default account.' },
      },
      required: ['summary', 'startTime', 'endTime'],
    },
    moeba_set_primary_account: {
      endpoint: '/api/proxy/account/primary',
      description:
        "Set the DEFAULT account for a provider when the user has several connected (e.g. two Gmail addresses). After this, moeba_email_send / moeba_calendar_* without an explicit `account` use this one. Run moeba_connections first to see the connected accounts.",
      properties: {
        platform: { type: 'string', description: '"gmail" or "outlook" — defaults to gmail' },
        account: { type: 'string', description: 'The account to make default — its email or a substring (e.g. "phiresky").' },
      },
      required: ['account'],
    },
    moeba_contacts: {
      endpoint: '/api/proxy/contacts',
      description: "Get the user's frequent contacts from recent emails.",
      properties: {
        search: { type: 'string', description: 'Filter contacts by name/email' },
        maxResults: { type: 'number' },
        platform: { type: 'string', description: '"gmail" or "outlook"' },
      },
    },
    moeba_connections: {
      endpoint: '/api/proxy/connections',
      description:
        'List which services (email, calendar) the user has connected. Use to check before other proxy tools.',
      properties: {},
    },
  };

  const connectionIdProp = {
    connection_id: {
      type: 'string',
      description:
        "Moeba connection id — omit to use this project's own connection; pass the connection_id from an inbound <channel> tag to act for that conversation",
    },
  };

  mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: [
      ...Object.entries(PROXY_TOOLS).map(([name, tool]) => ({
        name,
        description: tool.description,
        inputSchema: {
          type: 'object' as const,
          properties: { ...connectionIdProp, ...tool.properties },
          required: tool.required ?? [],
        },
      })),
      {
        name: 'moeba_email_attachment',
        description:
          'Download ONE attachment from a received email to a LOCAL FILE (or, with raw:true, export the whole message as .eml for provenance/archival). Get attachment_id from moeba_email_read / moeba_email_thread. The file is saved locally and its sha256 verified byte-for-byte against the server hash — the result gives the saved path, metadata, and a short-lived pre-signed download URL you can hand to curl or another tool. File bytes never pass through model context.',
        inputSchema: {
          type: 'object' as const,
          properties: {
            ...connectionIdProp,
            messageId: { type: 'string', description: 'Message id from moeba_email_search / moeba_email_read' },
            attachment_id: { type: 'string', description: 'Attachment id from the attachments array on moeba_email_read / moeba_email_thread. Omit only with raw:true.' },
            raw: { type: 'boolean', description: 'Fetch the raw RFC-822 message (.eml) instead of an attachment — the original email itself, for audit/archive flows.' },
            platform: { type: 'string', description: '"gmail" or "outlook"' },
            account: { type: 'string', description: 'Which connected account the message belongs to — its email or a substring. Needed when several accounts are connected.' },
            save_dir: { type: 'string', description: 'Directory to save the file into. Defaults to ~/.moeba/email-attachments' },
          },
          required: ['messageId', 'platform'],
        },
      },
      {
        name: 'moeba_reply',
        description:
          'Send a message to a Moeba user — either replying to an inbound message (pass connection_id) or INITIATING one unprompted, e.g. to report that an unattended job failed, a scheduled job finished, or an OAuth token expired. Omit both connection_id and email to message the logged-in Moeba user (the account this channel authenticated as) — no inbound message needed. Optionally attach interactive widgets (components) rendered in the app chat: oauthConnect (account connect button), secretInput (masked secret entry), approvalRequest (Approve/Deny card).',
        inputSchema: {
          type: 'object' as const,
          properties: {
            connection_id: {
              type: 'string',
              description: 'The connection_id from the inbound <channel> tag. Omit to message the logged-in Moeba user.',
            },
            email: {
              type: 'string',
              description: "Another Moeba user's account email — only needed to START a conversation with someone other than the logged-in user. A connection is created if none exists.",
            },
            text: {
              type: 'string',
              description: 'The message to send back',
            },
            components: {
              type: 'array',
              description:
                'Optional widgets to render with the message. Each item is an object with a "type" plus type-specific fields: {type:"oauthConnect", provider, scopes?, label?, description?} | {type:"secretInput", name, label, description?} | {type:"approvalRequest", requestId, toolName, description, inputPreview?}',
              items: { type: 'object' },
            },
          },
          required: ['text'],
        },
      },
      {
        name: 'moeba_progress',
        description: 'Show a typing/progress indicator while working on a task. Omit both connection_id and email to target the logged-in Moeba user.',
        inputSchema: {
          type: 'object' as const,
          properties: {
            connection_id: {
              type: 'string',
              description: 'The connection_id from the inbound <channel> tag. Omit to target the logged-in Moeba user.',
            },
            email: {
              type: 'string',
              description: "Another Moeba user's account email — only needed when addressing someone other than the logged-in user.",
            },
            text: {
              type: 'string',
              description: 'Progress text (e.g. "Reading files...")',
            },
          },
          required: ['text'],
        },
      },
      {
        name: 'moeba_schedule',
        description:
          'Schedule a task in Moeba\'s durable scheduler. Give EITHER cron (recurring, 5-field, e.g. "0 7 * * *" = daily 07:00, "0 8 * * 1-5" = weekdays 08:00, "*/30 * * * *" = every 30 min; minimum 1 minute) OR run_at (one-shot ISO 8601 with offset, e.g. "2026-09-08T07:00:00+02:00"; runs once then deletes itself). At fire time Moeba sends the prompt into this session as a channel message tagged type="cron" — you then do the task and reply to the user with moeba_reply. Jobs survive session restarts.',
        inputSchema: {
          type: 'object' as const,
          properties: {
            cron: { type: 'string', description: 'Standard 5-field cron expression for a recurring job' },
            run_at: { type: 'string', description: 'ISO 8601 datetime with timezone offset for a one-shot job' },
            prompt: { type: 'string', description: 'What to do when the job fires — written as an instruction to yourself, including what to send the user' },
            timezone: { type: 'string', description: 'IANA timezone for the cron expression (e.g. "Africa/Johannesburg"). Defaults to this machine\'s timezone.' },
            connection_id: { type: 'string', description: 'Connection to schedule for — omit to use this project\'s own connection (the logged-in user)' },
          },
          required: ['prompt'],
        },
      },
      {
        name: 'moeba_schedules',
        description: 'List the scheduled tasks Moeba holds for this connection — id, schedule, prompt, status, last/next run.',
        inputSchema: {
          type: 'object' as const,
          properties: {
            connection_id: { type: 'string', description: 'Omit to use this project\'s own connection' },
          },
        },
      },
      {
        name: 'moeba_unschedule',
        description: 'Cancel (delete) a scheduled task by its id from moeba_schedules or moeba_schedule.',
        inputSchema: {
          type: 'object' as const,
          properties: {
            cron_job_id: { type: 'string', description: 'The job id to cancel' },
            connection_id: { type: 'string', description: 'Omit to use this project\'s own connection' },
          },
          required: ['cron_job_id'],
        },
      },
      {
        name: 'moeba_subscribe',
        description:
          'Subscribe YOU (your mesh call-sign) to topics to receive real-time notifications privately in your mesh inbox. Requires moeba_mesh_join first — subscriptions are scoped to your call-sign, so ONLY you get them. Two topic families: (1) DEPLOY/BUILD by GitHub repo — patterns "owner/repo", "owner/*", or "*" (CI publishes with ?repo=<owner/repo>). (2) FLAREDECK card activity (comments, lane moves, assigns, etc.) — patterns "flaredeck/board/<board>", "flaredeck/card/<cardCode>", "flaredeck/tenant/<id>", or "flaredeck/*". Subscribe only to what you care about; matching events arrive tagged source="agent-mesh" (from_agent="deploy" or "flaredeck").',
        inputSchema: {
          type: 'object' as const,
          properties: {
            repos: { type: 'array', items: { type: 'string' }, description: 'Topics/patterns to subscribe to — e.g. "KeplerTechnologies/*" (deploys) or "flaredeck/board/PayInc" (Flaredeck card activity)' },
          },
          required: ['repos'],
        },
      },
      {
        name: 'moeba_unsubscribe',
        description: 'Stop YOUR call-sign receiving deploy notifications for the given GitHub repos/patterns.',
        inputSchema: {
          type: 'object' as const,
          properties: {
            repos: { type: 'array', items: { type: 'string' }, description: 'Repos/patterns to remove' },
          },
          required: ['repos'],
        },
      },
      {
        name: 'moeba_subscriptions',
        description: 'List the GitHub repos/patterns YOUR call-sign is subscribed to for deploy/build notifications.',
        inputSchema: { type: 'object' as const, properties: {} },
      },
      {
        name: 'moeba_deploy_token',
        description:
          "Get this business's STABLE CI deploy token — the credential a CI/GitHub Actions deploy step uses to POST deploy notifications to Moeba (POST /deploy/notify). Unlike your agent key it NEVER rotates on login, so it never 401s. Use this to SET UP CI: fetch the token, add it to the repo's GitHub Actions secrets as MOEBA_DEPLOY_TOKEN, and add the returned workflow step (POSTs with header X-Moeba-Deploy-Token). ONE token covers EVERY repo in the business — each agent picks which repos to actually receive via moeba_subscribe. Returns the token + a ready-to-paste workflow snippet. Pass action:'rotate' ONLY if the token leaked (invalidates the old one).",
        inputSchema: {
          type: 'object' as const,
          properties: {
            action: { type: 'string', enum: ['get', 'rotate'], description: "'get' (default) returns the current token, minting one if none exists; 'rotate' replaces it with a new one and invalidates the old." },
          },
        },
      },
      {
        name: 'moeba_mesh_join',
        description:
          "Join the private agent-to-agent mesh under a call-sign YOU choose (e.g. 'kepler', 'payinc-orchestrator'), with a description of what you do. This is how you become discoverable and reachable by other agents. Call it once at the start. You can re-call to change call-sign or update your description. Hidden from human users.",
        inputSchema: {
          type: 'object' as const,
          properties: {
            callSign: { type: 'string', description: 'The unique call-sign to identify this agent by (lowercase-kebab)' },
            description: { type: 'string', description: 'What this agent does' },
          },
          required: ['callSign'],
        },
      },
      {
        name: 'moeba_mesh_agents',
        description:
          'List the private agent-to-agent mesh directory: call-signs, what each does, and whether online. Use to find whom to message with moeba_mesh_send. (Hidden from human users.)',
        inputSchema: { type: 'object' as const, properties: {} },
      },
      {
        name: 'moeba_mesh_send',
        description:
          "Send a message (and optionally an image) to another agent in the private mesh by call-sign. Two-way: the recipient replies with moeba_mesh_send back to your call-sign. To send an image, pass imagePath (a LOCAL file path — it's uploaded automatically) or imageUrl (an already-hosted URL). The recipient receives the image downloaded to a local file and can view it with the Read tool. Hidden from human users. Inbound mesh messages arrive tagged source=\"agent-mesh\".",
        inputSchema: {
          type: 'object' as const,
          properties: {
            to: { type: 'string', description: "Recipient call-sign (from moeba_mesh_agents)" },
            message: { type: 'string', description: 'Message text (optional if sending an image)' },
            imagePath: { type: 'string', description: 'Local file path of an image to attach — read + uploaded automatically, then delivered to the recipient' },
            imageUrl: { type: 'string', description: 'URL of an already-hosted image to attach instead of uploading a local file' },
          },
          required: ['to'],
        },
      },
      {
        name: 'moeba_whoami',
        description:
          "Report YOUR OWN mesh identity: the call-sign you are currently acting as, where that call-sign came from (explicit join vs derived from the project folder), whether your inbound mesh stream is live, your topic subscriptions, and when you last received an event. Call this FIRST when events you expect are not arriving, or before telling another system which call-sign to route to you — a call-sign derived from the folder name is the usual cause of events being routed somewhere nobody is listening.",
        inputSchema: { type: 'object' as const, properties: {}, required: [] },
      },
      {
        name: 'moeba_describe_self',
        description:
          "Save a description of who you are and what you do onto your Moeba agent record (visible in the Moeba app/admin). Use this to persist your role/context server-side. Provide a concise description (max 2000 characters).",
        inputSchema: {
          type: 'object' as const,
          properties: {
            description: {
              type: 'string',
              description: 'A concise description of this agent — its role, what it does, and any useful context.',
            },
          },
          required: ['description'],
        },
      },
    ],
  }));

  // Inbound messages awaiting proof that the AGENT (not just this process)
  // picked them up. Claude Code silently drops channel notifications when the
  // session wasn't launched in channel mode, and it never tells the server — so
  // the only honest "delivered" signal is Claude's first Moeba tool call on that
  // connection. That call acks the pending user messages (blue ticks) and, unless
  // it is itself a progress/reply, turns on the "Working on it" indicator.

  function noteAgentActivity(name: string, args: any) {
    if (!creds?.agentApiKey) return;
    const connId = (args?.connection_id as string) || creds.connectionId;
    if (!connId || !pendingActivity.has(connId)) return;
    const messageIds = pendingActivity.get(connId) || [];
    pendingActivity.delete(connId);
    const post = (body: Record<string, unknown>) =>
      agentFetch(mcp, '/api/agent/send', { method: 'POST', body: JSON.stringify({ connectionId: connId, ...body }) }).catch(() => {});
    for (const messageId of messageIds) post({ type: 'ack', messageId });
    if (name !== 'moeba_progress' && name !== 'moeba_reply') {
      post({ type: 'progress', message: { text: 'Working on it...' } });
    }
  }

  mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
    const { name, arguments: args } = req.params;

    if (name.startsWith('moeba_')) noteAgentActivity(name, args);

    // Lazy interactive auth: the first moeba_* tool call of the session is
    // what's allowed to pop the browser (if silent refresh didn't already
    // cover it) — never automatically on process start/restart.
    if (CHANNEL_MODE && name.startsWith('moeba_')) {
      await ensureAuthenticated(mcp);
    }

    // ── Private agent-to-agent mesh ──────────────────────────────────────────
    if (name === 'moeba_mesh_join' || name === 'moeba_mesh_agents' || name === 'moeba_mesh_send') {
      if (!creds?.agentApiKey) {
        return { content: [{ type: 'text' as const, text: 'Not authenticated yet — reconnect the moeba channel.' }], isError: true };
      }
      if (name === 'moeba_mesh_join') {
        const want = String((args as any)?.callSign || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
        if (!want) return { content: [{ type: 'text' as const, text: 'callSign is required.' }], isError: true };
        const res = await agentFetch(mcp, '/agent-mesh/register', {
          method: 'POST',
          body: JSON.stringify({ callSign: want, description: (args as any)?.description || '' }),
        });
        if (!res.ok) return { content: [{ type: 'text' as const, text: `Failed: ${res.status} ${res.text}` }], isError: true };
        meshCallSign = want;          // adopt the chosen call-sign
        // Persist it alongside the human agent so restarts auto-rejoin under it.
        if (creds) { creds.meshCallSign = want; try { saveCredentials(creds); } catch {} }
        connectMeshSSE(mcp);          // (re)connect the inbound mesh stream for it
        return { content: [{ type: 'text' as const, text: `Joined the mesh as "${want}". Other agents can now reach you with moeba_mesh_send({to:"${want}", …}); inbound mesh messages arrive tagged source="agent-mesh".` }] };
      }

      if (!meshCallSign && name === 'moeba_mesh_send') {
        return { content: [{ type: 'text' as const, text: 'Join the mesh first: moeba_mesh_join({callSign, description}).' }], isError: true };
      }

      if (name === 'moeba_mesh_agents') {
        const res = await agentFetch(mcp, '/agent-mesh/list', {
          method: 'POST', body: JSON.stringify({ callSign: meshCallSign }),
        });
        if (!res.ok) return { content: [{ type: 'text' as const, text: `Failed: ${res.status} ${res.text}` }], isError: true };
        const list = (JSON.parse(res.text).agents || []) as Array<{ callSign: string; description: string; online: boolean; self: boolean; lastSeenAt?: string | null; staleDays?: number | null }>;
        if (list.length === 0) return { content: [{ type: 'text' as const, text: 'No agents in the mesh yet.' }] };
        // Show last-seen for offline entries. Without it, a long-abandoned
        // call-sign is indistinguishable from one that just went idle, and
        // near-duplicate names (foo / foo-orchestrator) are a coin flip.
        const age = (a: { online: boolean; lastSeenAt?: string | null; staleDays?: number | null }) => {
          if (a.online) return '';
          if (!a.lastSeenAt) return ' (never seen)';
          const d = a.staleDays ?? 0;
          return d >= 1 ? ` (last seen ${d}d ago)` : ' (last seen today)';
        };
        const lines = list.map((a) => `${a.online ? '🟢' : '⚪'} ${a.callSign}${a.self ? ' [you]' : ''}${age(a)}${a.description ? ` — ${a.description}` : ''}`);
        const stale = list.filter((a) => !a.online && (a.staleDays ?? 0) >= 14).length;
        const footer = stale
          ? `\n\n${stale} call-sign(s) not seen in 14+ days — likely abandoned sessions. Prefer a 🟢 recipient; an offline one still receives (durable inbox) but nothing may ever read it.`
          : '';
        return { content: [{ type: 'text' as const, text: `Mesh directory:\n${lines.join('\n')}${footer}` }] };
      }

      // moeba_mesh_send
      const a = (args ?? {}) as { to?: string; message?: string; imagePath?: string; imageUrl?: string };
      const attachments: Array<{ url: string; type?: string; name?: string }> = [];
      if (a.imageUrl) attachments.push({ url: a.imageUrl, type: 'image' });
      if (a.imagePath) {
        try {
          const buf = readFileSync(a.imagePath);
          const name = a.imagePath.split('/').pop() || 'image';
          const ext = (name.split('.').pop() || '').toLowerCase();
          const ctype = ext === 'png' ? 'image/png'
            : ext === 'jpg' || ext === 'jpeg' ? 'image/jpeg'
            : ext === 'gif' ? 'image/gif'
            : ext === 'webp' ? 'image/webp'
            : 'application/octet-stream';
          const up = await agentFetch(mcp, '/agent-mesh/upload', {
            method: 'POST',
            body: JSON.stringify({ data: buf.toString('base64'), contentType: ctype, name }),
          });
          if (!up.ok) return { content: [{ type: 'text' as const, text: `Image upload failed: ${up.status} ${up.text}` }], isError: true };
          const uj = JSON.parse(up.text);
          attachments.push({ url: uj.url, type: uj.type || 'image', name: uj.name || name });
        } catch (e: any) {
          return { content: [{ type: 'text' as const, text: `Could not read image at "${a.imagePath}": ${e.message}` }], isError: true };
        }
      }
      const res = await agentFetch(mcp, '/agent-mesh/send', {
        method: 'POST',
        body: JSON.stringify({ from: meshCallSign, to: a.to, message: a.message || '', attachments }),
      });
      if (!res.ok) return { content: [{ type: 'text' as const, text: `Failed: ${res.status} ${res.text}` }], isError: true };
      const delivered = JSON.parse(res.text).delivered;
      const imgNote = attachments.length ? ` (with ${attachments.length} image${attachments.length > 1 ? 's' : ''})` : '';
      // An offline recipient is NOT a failure: /agent-mesh/send persists to the
      // recipient's Firestore inbox, which drains on their next connect. Saying
      // "may not receive it" understated a delivery guarantee that does hold.
      return { content: [{ type: 'text' as const, text: delivered
        ? `Sent to "${a.to}"${imgNote}.`
        : `Queued for "${a.to}"${imgNote} — it is offline, so this is held in its durable inbox and delivered when it next connects.` }] };
    }

    // ── Who am I? ────────────────────────────────────────────────────────────
    if (name === 'moeba_whoami') {
      const sourceNote: Record<CallSignSource, string> = {
        flag: 'set explicitly via --agent / MOEBA_AGENT',
        joined: 'established by a previous moeba_mesh_join (persisted)',
        folder: `DERIVED FROM THE PROJECT FOLDER ("${PROJECT_SLUG}") — not an explicit join`,
        none: 'none',
      };
      const lines = [
        meshCallSign ? `Call-sign: ${meshCallSign}` : 'Call-sign: (none — you have not joined the mesh)',
        `Source: ${sourceNote[callSignSource]}`,
        `Inbound mesh stream: ${meshStreamConnected ? 'connected' : 'NOT connected'}`,
        `Last mesh event received: ${lastMeshEventAt ? new Date(lastMeshEventAt).toISOString() : 'none this session'}`,
        // Report the BACKING FILE, because its name is derived from the project
        // folder while the call-sign is chosen independently — so channel-<X>.json
        // is NOT the credential file of the agent whose call-sign is X. That
        // inference looks safe and isn't; stating the real path removes the guess.
        `Credentials file: ${CREDENTIALS_PATH}${PROJECT_SLUG && PROJECT_SLUG !== meshCallSign ? ` (named for folder "${PROJECT_SLUG}", not your call-sign)` : ''}`,
      ];
      if (callSignSource === 'folder') {
        lines.push(
          '',
          `⚠️  Your call-sign came from the folder name, not a deliberate join. Anything routing events to you MUST target "${meshCallSign}" — if you told another system (e.g. a Flaredeck channel subscription) to target a different name, those events are being written to that other call-sign's inbox and you will never see them here. Run moeba_mesh_join({callSign}) to claim the name you actually mean to be.`,
        );
      }
      // Subscriptions are server-side state keyed by call-sign — fetch rather
      // than guess, so this answers "why am I not getting events?" completely.
      if (meshCallSign && creds?.agentApiKey) {
        const subRes = await agentFetch(mcp, '/agent-mesh/subscribe', {
          method: 'POST', body: JSON.stringify({ callSign: meshCallSign, action: 'list' }),
        });
        if (subRes.ok) {
          try {
            const subs = JSON.parse(subRes.text).subscribedRepos || [];
            lines.push('', subs.length
              ? `Subscribed topics (${subs.length}): ${subs.join(', ')}`
              : 'Subscribed topics: none — moeba_subscribe({repos:[…]}) to add some.');
          } catch { /* omit rather than mislead */ }
        }
      }
      return { content: [{ type: 'text' as const, text: lines.join('\n') }] };
    }

    // ── Agent self-description ───────────────────────────────────────────────
    if (name === 'moeba_describe_self') {
      if (!creds?.agentApiKey) {
        return {
          content: [{ type: 'text' as const, text: 'Not authenticated — reconnect the moeba MCP server (run with --login or set MOEBA_API_KEY + MOEBA_EMAIL).' }],
          isError: true,
        };
      }
      const description = String((args as any)?.description || '').trim();
      if (!description) {
        return { content: [{ type: 'text' as const, text: 'description is required.' }], isError: true };
      }
      const res = await agentFetch(mcp, '/api/agent/describe', {
        method: 'POST',
        body: JSON.stringify({ description }),
      });
      if (!res.ok) return { content: [{ type: 'text' as const, text: `Failed: ${res.status} ${res.text}` }], isError: true };
      // Name the record written to, and say plainly that this is NOT the mesh
      // directory entry. A bare "Saved" here has been read as "my identity is
      // established", while the mesh still had the agent under another call-sign.
      let target = 'your Moeba agent record';
      try {
        const j = JSON.parse(res.text);
        if (j.agentName) target = `Moeba agent record "${j.agentName}"`;
      } catch { /* keep the generic wording */ }
      const meshNote = meshCallSign
        ? `This is separate from your MESH directory entry (call-sign "${meshCallSign}"), whose description is set via moeba_mesh_join({callSign, description}) — this write will NOT appear in moeba_mesh_agents.`
        : 'You have NOT joined the mesh, so you have no mesh directory entry. This write does not create one — use moeba_mesh_join({callSign, description}) for that.';
      return { content: [{ type: 'text' as const, text: `Saved your description to ${target}.\n\n${meshNote}` }] };
    }

    // Repo subscription management — scoped to THIS agent's mesh call-sign so
    // deploy/build pings are delivered privately over the mesh (no shared
    // human-connection cross-talk). Requires joining the mesh first.
    if (name === 'moeba_subscribe' || name === 'moeba_unsubscribe' || name === 'moeba_subscriptions') {
      if (!creds?.agentApiKey) {
        return { content: [{ type: 'text' as const, text: 'Not authenticated yet — reconnect the moeba channel.' }], isError: true };
      }
      if (!meshCallSign) {
        return { content: [{ type: 'text' as const, text: 'Join the mesh first: moeba_mesh_join({callSign, description}). Subscriptions are scoped to your call-sign.' }], isError: true };
      }
      const action = name === 'moeba_subscribe' ? 'add' : name === 'moeba_unsubscribe' ? 'remove' : 'list';
      const repos = ((args as any)?.repos ?? []) as string[];
      const response = await agentFetch(mcp, '/agent-mesh/subscribe', {
        method: 'POST',
        body: JSON.stringify({ callSign: meshCallSign, repos, action }),
      });
      if (!response.ok) {
        return { content: [{ type: 'text' as const, text: `Failed: ${response.status} ${response.text}` }], isError: true };
      }
      const subs = (JSON.parse(response.text).subscribedRepos || []) as string[];
      return {
        content: [{
          type: 'text' as const,
          text: subs.length ? `Subscribed repos:\n${subs.map((r) => `• ${r}`).join('\n')}` : 'No repo subscriptions.',
        }],
      };
    }

    if (name === 'moeba_deploy_token') {
      if (!creds?.agentApiKey) {
        return { content: [{ type: 'text' as const, text: 'Not authenticated yet — reconnect the moeba channel.' }], isError: true };
      }
      const action = (args as any)?.action === 'rotate' ? 'rotate' : 'get';
      const response = await agentFetch(mcp, '/agent/deploy-token', {
        method: 'POST',
        body: JSON.stringify({ action }),
      });
      if (!response.ok) {
        return { content: [{ type: 'text' as const, text: `Failed: ${response.status} ${response.text}` }], isError: true };
      }
      const { token, endpoint } = JSON.parse(response.text) as { token: string; endpoint: string };
      // Single-quoted lines so nothing interpolates; ${{ }} / ${VAR} stay literal.
      const snippet = [
        '- name: Notify Moeba (deploy result)',
        '  if: always()',
        '  continue-on-error: true   # never fail the deploy on a notification',
        '  run: |',
        '    curl -s -X POST "__ENDPOINT__?repo=$GITHUB_REPOSITORY&env=QA&status=${{ job.status }}&sha=${GITHUB_SHA:0:7}&run=$GITHUB_SERVER_URL/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID" \\',
        '      -H "X-Moeba-Deploy-Token: ${{ secrets.MOEBA_DEPLOY_TOKEN }}" \\',
        '      -o /dev/null -w "Moeba notify: HTTP %{http_code}\\n" || true',
      ].join('\n').replace('__ENDPOINT__', endpoint);
      return {
        content: [{
          type: 'text' as const,
          text: [
            `Deploy token (${action === 'rotate' ? 'ROTATED — the previous token is now invalid' : 'stable — never rotates on login'}):`,
            '',
            token,
            '',
            'Setup:',
            '1. GitHub → repo → Settings → Secrets and variables → Actions → New repository secret',
            '   Name: MOEBA_DEPLOY_TOKEN   Value: (the token above)',
            '2. Add this step to your deploy workflow:',
            '',
            snippet,
            '',
            `Endpoint: ${endpoint}`,
            'Auth: header X-Moeba-Deploy-Token (or ?token= in the query).',
            'One token covers every repo in this business — each agent chooses which repos to receive with moeba_subscribe.',
          ].join('\n'),
        }],
      };
    }

    // ── Email attachment download ────────────────────────────────────────────
    // Custom (not a plain proxy tool): the server returns metadata + a pre-signed
    // URL; we fetch the bytes HERE and land them on local disk, verifying the
    // sha256 — so file content never inflates the model context.
    if (name === 'moeba_email_attachment') {
      if (!creds?.agentApiKey) {
        return { content: [{ type: 'text' as const, text: 'Not authenticated — reconnect the moeba MCP server (run with --login or set MOEBA_API_KEY + MOEBA_EMAIL).' }], isError: true };
      }
      const a = (args ?? {}) as {
        connection_id?: string; messageId?: string; attachment_id?: string;
        raw?: boolean; platform?: string; account?: string; save_dir?: string;
      };
      const res = await agentFetch(mcp, '/api/proxy/email/attachment', {
        method: 'POST',
        body: JSON.stringify({
          connectionId: a.connection_id || creds.connectionId,
          messageId: a.messageId,
          attachment_id: a.attachment_id,
          raw: a.raw,
          platform: a.platform,
          account: a.account,
        }),
      });
      if (!res.ok) return { content: [{ type: 'text' as const, text: `Failed: ${res.status} ${res.text}` }], isError: true };

      let meta: { filename: string; mime_type: string; size_bytes: number; sha256: string; download_url: string; expires_in_seconds: number };
      try {
        meta = JSON.parse(res.text);
      } catch {
        return { content: [{ type: 'text' as const, text: `Unexpected server response: ${res.text}` }], isError: true };
      }

      const dir = a.save_dir || join(homedir(), '.moeba', 'email-attachments');
      try { if (!existsSync(dir)) mkdirSync(dir, { recursive: true }); } catch (e: any) {
        return { content: [{ type: 'text' as const, text: `Could not create save directory "${dir}": ${e.message}` }], isError: true };
      }

      try {
        const dl = await fetch(meta.download_url);
        if (!dl.ok) throw new Error(`download returned ${dl.status}`);
        const buf = Buffer.from(await dl.arrayBuffer());
        const localSha = createHash('sha256').update(buf).digest('hex');

        const safe = (meta.filename || 'attachment').replace(/[^a-zA-Z0-9._ -]/g, '_').trim() || 'attachment';
        let path = join(dir, safe);
        if (existsSync(path)) path = join(dir, `${String(a.messageId).slice(-8)}-${safe}`);
        writeFileSync(path, buf);

        const verified = localSha === meta.sha256;
        return {
          content: [{
            type: 'text' as const,
            text: [
              `Saved: ${path}`,
              `Filename: ${meta.filename}`,
              `Type: ${meta.mime_type}  Size: ${meta.size_bytes} bytes`,
              `SHA-256: ${meta.sha256}${verified ? ' (verified byte-for-byte against the downloaded file)' : ` — ⚠️ MISMATCH: local file hashed ${localSha}. Re-fetch before trusting it.`}`,
              `Download URL (valid ~${Math.round((meta.expires_in_seconds || 900) / 60)} min, the URL is the credential): ${meta.download_url}`,
            ].join('\n'),
          }],
          isError: !verified,
        };
      } catch (e: any) {
        // The local download failed — the pre-signed URL is still usable directly.
        return {
          content: [{
            type: 'text' as const,
            text: [
              `Could not save the file locally (${e.message}), but the attachment is fetchable:`,
              `Filename: ${meta.filename}  Type: ${meta.mime_type}  Size: ${meta.size_bytes} bytes  SHA-256: ${meta.sha256}`,
              `Download URL (valid ~${Math.round((meta.expires_in_seconds || 900) / 60)} min): ${meta.download_url}`,
              `e.g.  curl -o "<dest>" "${meta.download_url}"`,
            ].join('\n'),
          }],
        };
      }
    }

    const proxyTool = PROXY_TOOLS[name];
    if (proxyTool) {
      if (!creds?.agentApiKey) {
        return {
          content: [
            {
              type: 'text' as const,
              text: 'Not authenticated — reconnect the moeba MCP server (run with --login or set MOEBA_API_KEY + MOEBA_EMAIL).',
            },
          ],
          isError: true,
        };
      }

      const { connection_id, ...rest } = (args ?? {}) as Record<string, unknown>;

      // moeba_email_send: resolve attachments. Each item may be a LOCAL FILE PATH
      // (string) — read + base64-encode it here — or an already-encoded object
      // {filename, content, contentType}. Agents have files on disk, so paths are
      // the natural input.
      if (name === 'moeba_email_send' && Array.isArray((rest as any).attachments)) {
        const mimeByExt: Record<string, string> = {
          pdf: 'application/pdf', png: 'image/png', jpg: 'image/jpeg', jpeg: 'image/jpeg',
          gif: 'image/gif', webp: 'image/webp', txt: 'text/plain', csv: 'text/csv',
          json: 'application/json', zip: 'application/zip', md: 'text/markdown',
          doc: 'application/msword', docx: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
          xls: 'application/vnd.ms-excel', xlsx: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        };
        try {
          (rest as any).attachments = ((rest as any).attachments as unknown[]).map((a) => {
            if (typeof a === 'string') {
              const buf = readFileSync(a);
              const filename = a.split('/').pop() || 'attachment';
              const ext = (filename.split('.').pop() || '').toLowerCase();
              return { filename, content: buf.toString('base64'), contentType: mimeByExt[ext] || 'application/octet-stream' };
            }
            return a; // already {filename, content, contentType}
          });
        } catch (e: any) {
          return { content: [{ type: 'text' as const, text: `Could not read an attachment file: ${e?.message ?? 'unknown error'}` }], isError: true };
        }
      }

      const body = {
        connectionId: (connection_id as string) || creds.connectionId,
        ...rest,
      };

      const response = await agentFetch(mcp, proxyTool.endpoint, {
        method: 'POST',
        body: JSON.stringify(body),
      });

      if (!response.ok) {
        return {
          content: [{ type: 'text' as const, text: `Failed: ${response.status} ${response.text}` }],
          isError: true,
        };
      }
      return { content: [{ type: 'text' as const, text: response.text }] };
    }

    if (name === 'moeba_schedule' || name === 'moeba_schedules' || name === 'moeba_unschedule') {
      if (!creds?.agentApiKey) {
        return {
          content: [{ type: 'text' as const, text: 'Moeba channel not authenticated yet — try again in a moment (or reconnect with --login).' }],
          isError: true,
        };
      }
      const a = (args ?? {}) as {
        cron?: string; run_at?: string; prompt?: string; timezone?: string;
        connection_id?: string; cron_job_id?: string;
      };
      const connectionId = a.connection_id || creds.connectionId;
      let body: Record<string, unknown>;
      if (name === 'moeba_schedule') {
        if (!a.cron && !a.run_at) {
          return { content: [{ type: 'text' as const, text: 'Give either cron (recurring) or run_at (one-shot).' }], isError: true };
        }
        const timezone = a.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
        body = {
          action: 'create', connectionId,
          cron: a.cron, runAt: a.run_at, prompt: a.prompt, timezone,
          data: { source: 'claude-channel', project: PROJECT_NAME },
        };
      } else if (name === 'moeba_schedules') {
        body = { action: 'list', connectionId };
      } else {
        body = { action: 'delete', connectionId, cronJobId: a.cron_job_id };
      }

      const response = await agentFetch(mcp, '/api/proxy/cron', {
        method: 'POST',
        body: JSON.stringify(body),
      });
      if (!response.ok) {
        return { content: [{ type: 'text' as const, text: `Failed: ${response.status} ${response.text}` }], isError: true };
      }
      if (name === 'moeba_schedule') {
        const when = a.cron ? `cron "${a.cron}" (${(body as any).timezone})` : `once at ${a.run_at}`;
        return { content: [{ type: 'text' as const, text: `Scheduled ${when}. ${response.text}` }] };
      }
      return { content: [{ type: 'text' as const, text: response.text }] };
    }

    if (name === 'moeba_reply' || name === 'moeba_progress') {
      if (!creds?.agentApiKey) {
        return {
          content: [{ type: 'text' as const, text: 'Moeba channel not authenticated yet — try again in a moment (or reconnect with --login).' }],
          isError: true,
        };
      }
      const { connection_id, email, text, components } = args as {
        connection_id?: string;
        email?: string;
        text: string;
        components?: unknown[];
      };

      // /api/agent/send accepts EITHER a connectionId or an email (it resolves or
      // creates the connection for that user). With neither given, default to
      // the logged-in user's own connection so an agent with no inbound message
      // (cron, CI, a scheduled task) can still reach the person who owns this
      // channel without knowing any ids.
      const body: any = { message: { text } };
      if (connection_id) body.connectionId = connection_id;
      else if (email) body.email = email;
      else if (creds.connectionId) body.connectionId = creds.connectionId;
      else body.email = creds.email;
      if (name === 'moeba_reply' && Array.isArray(components) && components.length > 0) {
        body.message.components = components;
      }
      if (name === 'moeba_progress') {
        body.type = 'progress';
      }

      const response = await agentFetch(mcp, '/api/agent/send', {
        method: 'POST',
        body: JSON.stringify(body),
      });

      if (!response.ok) {
        return {
          content: [
            {
              type: 'text' as const,
              text: `Failed: ${response.status} ${response.text}`,
            },
          ],
          isError: true,
        };
      }

      return {
        content: [
          {
            type: 'text' as const,
            text: name === 'moeba_progress' ? 'Progress updated' : 'Sent',
          },
        ],
      };
    }

    throw new Error(`Unknown tool: ${name}`);
  });

  // Permission relay — Claude Code notifies us when a tool-approval dialog
  // opens. Forward it to the Moeba app as an approvalRequest widget; the
  // verdict returns through the SSE action path (see connectSSE) and is
  // relayed back as notifications/claude/channel/permission.
  const PermissionRequestSchema = z.object({
    method: z.literal('notifications/claude/channel/permission_request'),
    params: z.object({
      request_id: z.string(),
      tool_name: z.string(),
      description: z.string(),
      input_preview: z.string(),
    }),
  });

  mcp.setNotificationHandler(PermissionRequestSchema, async ({ params }) => {
    if (!creds?.agentApiKey || !creds?.connectionId) return;
    await fetch(`${MOEBA_API_URL}/api/agent/send`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Moeba-Agent-Key': creds.agentApiKey,
      },
      body: JSON.stringify({
        connectionId: creds.connectionId,
        message: {
          text: `🔐 Claude Code wants to run ${params.tool_name}`,
          components: [
            {
              type: 'approvalRequest',
              requestId: params.request_id,
              toolName: params.tool_name,
              description: params.description,
              inputPreview: params.input_preview,
            },
          ],
        },
      }),
    }).catch((err: any) =>
      console.error('Permission relay send failed:', err.message),
    );
  });

  // 4. Connect MCP to Claude Code FIRST so the handshake completes instantly —
  //    the client always knows we're connected, even before auth finishes.
  await mcp.connect(new StdioServerTransport());

  // Not a channel session → tools are registered (and report "not authenticated"
  // if called) but we never authenticate, connect SSE, or provision an agent.
  if (!CHANNEL_MODE) {
    console.error('Moeba: not in channel mode — tools loaded, no agent provisioned. Launch via the `moeba` alias (MOEBA_CHANNEL=1) to connect.');
    return;
  }

  // Forced re-login: skip every "looks fine" shortcut and go straight to an
  // interactive browser login, awaited (not backgrounded) since a human is
  // actively running this command and waiting on it.
  if (forceRelogin) {
    console.error('Forcing a fresh interactive login (--relogin)...');
    try {
      creds = await authenticate();
      console.error(`Authenticated as ${creds.email} (project: ${PROJECT_NAME})`);
      startSSEIfNeeded(mcp);
    } catch (err: any) {
      console.error(`Re-login failed: ${err.message}`);
    }
    return;
  }

  // If cached creds are already valid, start SSE right away (warm path).
  if (creds && !isTokenExpired(creds.token)) {
    startSSEIfNeeded(mcp);
  }

  // 5. Do any SILENT auth recovery in the BACKGROUND (stored agent key / env
  //    API key). Never awaited, so /mcp reconnects can't time out waiting on
  //    it. Deliberately does NOT fall back to the browser here — that only
  //    happens lazily, on the first actual moeba_* tool call (see
  //    ensureAuthenticated, wired into the CallToolRequestSchema handler
  //    below), so restarting Claude Code never pops a login tab on its own.
  void (async () => {
    try {
      if (!creds && envApiKey && envEmail) {
        console.error(`Authenticating as ${envEmail} via API key...`);
        creds = await authenticateViaApiKey(envApiKey, envEmail);
      } else if (creds && isTokenExpired(creds.token)) {
        console.error('Cached Moeba session token expired — refreshing silently...');
        const refreshed = await refreshCredentials({ allowBrowser: false });
        if (refreshed) creds = refreshed;
      } else {
        return; // already connected on the warm path above, or nothing to silently refresh yet
      }

      if (creds) {
        console.error(`Authenticated as ${creds.email} (project: ${PROJECT_NAME})`);
        startSSEIfNeeded(mcp);
      } else if (loginMode) {
        console.error(`Moeba channel: no valid credentials yet for project "${PROJECT_NAME}" — will prompt to sign in on first moeba tool call.`);
      } else {
        console.error(`Moeba channel: no credentials for project "${PROJECT_NAME}" — set MOEBA_API_KEY + MOEBA_EMAIL, or call a moeba tool to sign in interactively.`);
      }
    } catch (err: any) {
      console.error('Background auth failed:', err.message);
    }
  })();
}

main().catch((err) => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
