#!/usr/bin/env node
/**
 * moeba-notify — inject a message into a running Claude Code Moeba session.
 *
 * Posts text to the project's Moeba conversation exactly like a user message,
 * so a Claude Code session connected to that channel reacts to it. Use it from
 * build scripts, CI, cron, git hooks, etc.:
 *
 *   moeba-notify "Build finished ✅"
 *   npm run build && moeba-notify "build done" || moeba-notify "build FAILED"
 *   moeba-notify --project kepler "deploy complete"
 *
 * Reads the cached channel credentials written by moeba-claude-channel at
 * ~/.moeba/channel-<project>.json. The project defaults to MOEBA_PROJECT or the
 * current git repo name (same detection as the channel). If the cached session
 * token has expired, it re-mints one using the stored agent key + email.
 */
import { readFileSync } from 'fs';
import { homedir } from 'os';
import { join } from 'path';
import { execSync } from 'child_process';

const MOEBA_API_URL = (
  process.env.MOEBA_API_URL ||
  'https://moeba-api-999642860678.africa-south1.run.app'
).replace(/\/$/, '');

interface Credentials {
  token: string;
  email: string;
  businessId: string;
  agentId: string;
  connectionId: string;
  agentApiKey: string;
  projectName: string;
}

function detectProjectName(): string {
  try {
    const root = execSync('git rev-parse --show-toplevel', {
      encoding: 'utf-8',
      stdio: ['pipe', 'pipe', 'pipe'],
    }).trim();
    return root.split('/').pop() || 'default';
  } catch {
    return 'default';
  }
}

function isTokenExpired(token: string): boolean {
  try {
    const payload = JSON.parse(
      Buffer.from(token.split('.')[1].replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString(),
    );
    return typeof payload.exp === 'number' && payload.exp * 1000 < Date.now() + 60_000;
  } catch {
    return false;
  }
}

/**
 * Authenticate headlessly with an agent API key + email. Returns full creds
 * (token + businessId + connectionId for the project's Claude Code agent).
 */
async function authViaApiKey(
  apiKey: string,
  email: string,
  projectName: string,
): Promise<Credentials | null> {
  try {
    const res = await fetch(`${MOEBA_API_URL}/channel/auth`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ apiKey, email, projectName }),
    });
    if (!res.ok) return null;
    const d = (await res.json()) as any;
    return {
      token: d.token,
      email: d.email,
      businessId: d.businessId,
      agentId: d.agentId,
      connectionId: d.connectionId,
      agentApiKey: d.agentApiKey || apiKey,
      projectName,
    };
  } catch {
    return null;
  }
}

async function send(creds: Credentials, token: string, text: string) {
  return fetch(`${MOEBA_API_URL}/chat/send`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify({
      businessId: creds.businessId,
      connectionId: creds.connectionId,
      chatMode: 'agent',
      message: { text },
    }),
  });
}

async function main() {
  const args = process.argv.slice(2);
  let project = process.env.MOEBA_PROJECT || '';
  const textParts: string[] = [];
  for (let i = 0; i < args.length; i++) {
    if ((args[i] === '--project' || args[i] === '-p') && args[i + 1]) {
      project = args[++i];
    } else {
      textParts.push(args[i]);
    }
  }
  const text = textParts.join(' ').trim();

  if (!text) {
    console.error('Usage: moeba-notify [--project <name>] "<message>"');
    process.exit(2);
  }

  project = project || detectProjectName();
  const slug = project.toLowerCase().replace(/[^a-z0-9]+/g, '-');
  const credsPath = join(homedir(), '.moeba', `channel-${slug}.json`);

  // Resolve credentials. Dev machine: read the cached file written by the
  // running channel. CI / headless (no file): authenticate with the agent API
  // key + email from env (MOEBA_API_KEY, MOEBA_EMAIL) — the same secrets used
  // for agent-initiated notifications.
  let creds: Credentials | null = null;
  try {
    creds = JSON.parse(readFileSync(credsPath, 'utf-8'));
  } catch {
    /* no cached file — fall back to env below */
  }

  let token = creds?.token || '';

  if (!creds || isTokenExpired(token)) {
    const apiKey = creds?.agentApiKey || process.env.MOEBA_API_KEY || '';
    const email = creds?.email || process.env.MOEBA_EMAIL || '';
    if (!apiKey || !email) {
      console.error(
        `No Moeba channel credentials for project "${project}".\n` +
          `On a dev machine, start a Claude Code session with the moeba channel first.\n` +
          `In CI, set MOEBA_API_KEY + MOEBA_EMAIL (and MOEBA_PROJECT) env vars.`,
      );
      process.exit(1);
      return;
    }
    const authed = await authViaApiKey(apiKey, email, project);
    if (!authed) {
      console.error('Moeba auth failed — check MOEBA_API_KEY / MOEBA_EMAIL.');
      process.exit(1);
      return;
    }
    creds = authed;
    token = authed.token;
  }

  let res = await send(creds, token, text);
  if (res.status === 401) {
    // Token rejected — re-auth once via the agent key, then retry.
    const reauthed = await authViaApiKey(creds.agentApiKey, creds.email, project);
    if (reauthed) res = await send(reauthed, reauthed.token, text);
  }

  if (!res.ok) {
    console.error(`moeba-notify failed: ${res.status} ${await res.text()}`);
    process.exit(1);
  }
  console.error(`Sent to "${project}" Moeba channel: ${text}`);
}

main().catch((err) => {
  console.error('moeba-notify error:', err.message);
  process.exit(1);
});
