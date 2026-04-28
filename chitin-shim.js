/**
 * chitin-shim.js — Chitin Clack Gateway
 *
 * Standalone registry-based A2A mesh router.
 * Implements the Chitin endpoint contracts defined in docs/observability.md
 * and the registration protocol in docs/registry-schema.md.
 *
 * Endpoints:
 *   POST /register          — agent self-registration (requires X-Clack-Token)
 *   POST /heartbeat         — refresh agent TTL (unauthenticated, agentId only)
 *   GET  /health            — liveness + registry stats
 *   GET  /registry          — full registry state
 *   GET  /routes            — preferred URL per active agent
 *   GET  /deliveries/recent — last N delivery attempts (ring buffer)
 *
 * Config:  CLACK_CONFIG env var → gateway.yml (js-yaml)
 * Secrets: CLACK_BOOTSTRAP_SECRET env var — never in config file
 *
 * Chitin fork of win4r/openclaw-a2a-gateway.
 * Upstream plugin (index.ts) remains intact for OpenClaw compatibility.
 */

import express from 'express';
import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'node:fs';
import { timingSafeEqual } from 'node:crypto';
import { join } from 'node:path';

export const SHIM_VERSION = '1.0.0-chitin';
const startedAt = Date.now();

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function deepMerge(target, source) {
  if (!source || typeof source !== 'object') return target;
  const out = { ...target };
  for (const k of Object.keys(source)) {
    const sv = source[k];
    if (sv && typeof sv === 'object' && !Array.isArray(sv)) {
      out[k] = deepMerge(target[k] ?? {}, sv);
    } else {
      out[k] = sv;
    }
  }
  return out;
}

/** Timing-safe string comparison. Returns false on length mismatch. */
function tokenMatch(supplied, expected) {
  if (!expected || !supplied) return false;
  try {
    const a = Buffer.from(String(supplied), 'utf8');
    const b = Buffer.from(String(expected), 'utf8');
    if (a.length !== b.length) return false;
    return timingSafeEqual(a, b);
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const CONFIG_DEFAULTS = {
  server: { host: '127.0.0.1', port: 15200 },
  auth: {
    allowlist_policy: 'strict',
    allowed_agents: [],
    remote_auth_required: true,
    local_mdns_trusted: true,
  },
  registry: {
    ttl_default: 120,
    ttl_local_bonus: 60,
    stale_check_interval: 30,
    bootstrap_seed: '',
  },
  routing: {
    prefer_tailscale: true,
    fallback_to_public: true,
    delivery_timeout: 10,
    max_retries: 3,
    retry_backoff_ms: 1000,
  },
  observability: { deliveries_retention: 500 },
};

export async function loadConfig() {
  const configPath = process.env.CLACK_CONFIG || '/opt/clack-gateway/config/gateway.yml';
  const logDir   = process.env.CLACK_LOG_DIR  || '/opt/clack-gateway/logs';
  const dataDir  = process.env.CLACK_DATA_DIR || '/opt/clack-gateway/data';

  let cfg = CONFIG_DEFAULTS;
  try {
    const { load } = await import('js-yaml');
    const raw = readFileSync(configPath, 'utf8');
    cfg = deepMerge(CONFIG_DEFAULTS, load(raw));
    console.log(`[clack] config loaded from ${configPath}`);
  } catch (e) {
    console.warn(`[clack] could not load config from ${configPath}: ${e.message}`);
    console.warn('[clack] using built-in defaults — edit gateway.yml and restart');
  }

  const port = parseInt(process.env.CLACK_PORT ?? String(cfg.server.port), 10);
  return {
    host: cfg.server.host,
    port,
    auth: cfg.auth,
    registry: cfg.registry,
    routing: cfg.routing,
    deliveriesRetention: cfg.observability?.deliveries_retention ?? 500,
    logDir,
    dataDir,
  };
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

export class Registry {
  /**
   * @param {object} registryCfg  — config.registry block
   * @param {string} dataDir      — path to /opt/clack-gateway/data
   */
  constructor(registryCfg, dataDir) {
    this.cfg = registryCfg;
    this.registryPath = join(dataDir, 'registry.json');
    /** @type {Map<string, object>} agentId → stored record */
    this.store = new Map();
    this._sweepTimer = null;
  }

  // --- Persistence ---

  load(seedPath) {
    if (existsSync(this.registryPath)) {
      try {
        const records = JSON.parse(readFileSync(this.registryPath, 'utf8'));
        for (const rec of records) this.store.set(rec.agentId, rec);
        console.log(`[clack] registry: ${this.store.size} agents loaded from disk`);
        return;
      } catch (e) {
        console.warn(`[clack] registry: load error: ${e.message}`);
      }
    }
    if (seedPath && existsSync(String(seedPath))) {
      this._seed(String(seedPath));
    }
  }

  _seed(seedPath) {
    try {
      const raw = JSON.parse(readFileSync(seedPath, 'utf8'));
      const agents = Array.isArray(raw) ? raw : (raw.agents ?? []);
      const now = new Date().toISOString();
      for (const rec of agents) {
        const ttl = rec.ttl ?? this.cfg.ttl_default;
        this.store.set(rec.agentId, {
          ...rec,
          registeredAt: now,
          expiresAt: new Date(Date.now() + ttl * 1000).toISOString(),
        });
      }
      console.log(`[clack] registry: seeded ${agents.length} agents from ${seedPath}`);
      this._persist();
    } catch (e) {
      console.warn(`[clack] registry: seed error: ${e.message}`);
    }
  }

  _persist() {
    try {
      writeFileSync(this.registryPath, JSON.stringify([...this.store.values()], null, 2));
    } catch (e) {
      console.warn(`[clack] registry: persist error: ${e.message}`);
    }
  }

  // --- Sweep ---

  startStaleSweep() {
    const intervalMs = (this.cfg.stale_check_interval ?? 30) * 1000;
    this._sweepTimer = setInterval(() => this._sweep(), intervalMs);
  }

  stopStaleSweep() {
    if (this._sweepTimer) { clearInterval(this._sweepTimer); this._sweepTimer = null; }
  }

  _sweep() {
    const now = Date.now();
    let changed = false;
    for (const rec of this.store.values()) {
      if (!rec._stale && new Date(rec.expiresAt).getTime() < now) {
        rec._stale = true;
        changed = true;
        console.log(`[clack] stale: ${rec.agentId}`);
      }
    }
    if (changed) this._persist();
  }

  // --- Mutations ---

  /**
   * Upsert a registration record.
   * Throws if a remote agent tries to overwrite a local (mDNS-trusted) agent.
   */
  register(body) {
    const existing = this.store.get(body.agentId);
    if (existing?._localMdns && !body._localMdns) {
      throw new Error(`remote agent cannot overwrite locally-registered agentId '${body.agentId}'`);
    }

    const ttl = body.ttl ?? this.cfg.ttl_default;
    const now = new Date().toISOString();
    const rec = {
      agentId:      body.agentId,
      hostId:       body.hostId,
      tailscaleUrl: body.tailscaleUrl,
      publicUrl:    body.publicUrl ?? null,
      wakeUrl:      body.wakeUrl,
      harnessType:  body.harnessType,
      transport:    body.transport,
      capabilities: body.capabilities ?? [],
      ttl,
      registeredAt: now,
      expiresAt:    new Date(Date.now() + ttl * 1000).toISOString(),
      _stale:       false,
    };
    this.store.set(rec.agentId, rec);
    this._persist();
    return rec;
  }

  /** Refresh an agent's TTL. Returns null if not found. */
  heartbeat(agentId) {
    const rec = this.store.get(agentId);
    if (!rec) return null;
    rec.expiresAt = new Date(Date.now() + (rec.ttl ?? this.cfg.ttl_default) * 1000).toISOString();
    rec._stale = false;
    this._persist();
    return rec;
  }

  // --- Reads ---

  /** All records with live status computed from expiresAt. Private fields stripped. */
  getAll() {
    const now = Date.now();
    return [...this.store.values()].map((rec) => {
      const { _stale, _localMdns, ...pub } = rec;
      return { ...pub, status: new Date(rec.expiresAt).getTime() < now ? 'stale' : 'active' };
    });
  }

  getStats() {
    const all = this.getAll();
    return { total: all.length, stale: all.filter((a) => a.status === 'stale').length };
  }

  /**
   * Resolve the preferred delivery URL for an active agent.
   * Returns null for stale or missing agents.
   */
  resolveRoute(agentId, routingCfg) {
    const rec = this.store.get(agentId);
    if (!rec) return null;
    if (new Date(rec.expiresAt).getTime() < Date.now()) return null; // stale

    if (routingCfg.prefer_tailscale && rec.tailscaleUrl) {
      return {
        agentId,
        resolvedUrl: rec.tailscaleUrl,
        via: 'tailscale',
        fallbackAvailable: !!(routingCfg.fallback_to_public && rec.publicUrl),
      };
    }
    if (rec.publicUrl) {
      return { agentId, resolvedUrl: rec.publicUrl, via: 'public', fallbackAvailable: false };
    }
    return null; // agent has no reachable URL
  }
}

// ---------------------------------------------------------------------------
// Delivery log
// ---------------------------------------------------------------------------

export class DeliveryLog {
  constructor(retention) {
    this.retention = retention;
    /** @type {object[]} newest-first ring buffer */
    this.entries = [];
  }

  /** Record a delivery attempt. */
  record(entry) {
    this.entries.unshift({ ...entry, timestamp: new Date().toISOString() });
    if (this.entries.length > this.retention) this.entries.length = this.retention;
  }

  recent(n = this.retention) {
    return this.entries.slice(0, Math.min(n, this.retention));
  }
}

// ---------------------------------------------------------------------------
// Express app
// ---------------------------------------------------------------------------

export function buildApp(cfg, registry, deliveryLog) {
  const app = express();
  app.use(express.json({ limit: '64kb' }));
  app.disable('x-powered-by');

  const secret = process.env.CLACK_BOOTSTRAP_SECRET ?? '';
  const isStrict = (cfg.auth.allowlist_policy ?? 'strict') === 'strict';
  const allowedAgents = new Set(cfg.auth.allowed_agents ?? []);

  // --- Auth middleware ---
  function requireToken(req, res, next) {
    if (!secret) {
      return res.status(503).json({ error: 'gateway not ready: CLACK_BOOTSTRAP_SECRET not set' });
    }
    if (!tokenMatch(req.headers['x-clack-token'], secret)) {
      return res.status(401).json({ error: 'invalid or missing X-Clack-Token' });
    }
    next();
  }

  function checkAllowlist(agentId, res) {
    if (!isStrict) return true;
    if (!allowedAgents.has(agentId)) {
      res.status(403).json({ error: `agentId '${agentId}' not in allowed_agents` });
      return false;
    }
    return true;
  }

  // -----------------------------------------------------------------
  // POST /register
  // -----------------------------------------------------------------
  app.post('/register', requireToken, (req, res) => {
    const b = req.body;
    const required = ['agentId', 'hostId', 'tailscaleUrl', 'wakeUrl', 'harnessType', 'transport', 'ttl'];
    const missing = required.filter((f) => b[f] == null);
    if (missing.length) {
      return res.status(400).json({ error: 'missing required fields', missing });
    }
    if (!checkAllowlist(b.agentId, res)) return;

    try {
      const rec = registry.register(b);
      console.log(`[clack] registered ${b.agentId} @ ${b.tailscaleUrl} (ttl ${b.ttl}s)`);
      return res.json({ agentId: rec.agentId, expiresAt: rec.expiresAt });
    } catch (err) {
      return res.status(403).json({ error: err.message });
    }
  });

  // -----------------------------------------------------------------
  // POST /heartbeat
  // -----------------------------------------------------------------
  app.post('/heartbeat', (req, res) => {
    const { agentId } = req.body ?? {};
    if (!agentId) return res.status(400).json({ error: 'missing agentId' });
    const rec = registry.heartbeat(agentId);
    if (!rec) return res.status(404).json({ error: `agent '${agentId}' not registered` });
    return res.json({ agentId, expiresAt: rec.expiresAt });
  });

  // -----------------------------------------------------------------
  // GET /health
  // -----------------------------------------------------------------
  // GET /health — liveness probe only. Always HTTP 200 while the process is up.
  // Empty registry means 'starting', not 'unhealthy'. Use GET /ready for readiness.
  app.get('/health', (_req, res) => {
    const { total, stale } = registry.getStats();
    const uptimeSeconds = Math.floor((Date.now() - startedAt) / 1000);
    const active = total - stale;
    const status = active > 0 ? 'ok' : total > 0 ? 'degraded' : 'starting';
    res.json({ status, uptime_seconds: uptimeSeconds, registry_agents: total, registry_stale: stale, version: SHIM_VERSION });
  });

  // GET /ready — readiness probe. 503 until at least one active agent is registered.
  // Kubernetes/systemd readiness checks, load-balancer health gates, and monitoring
  // should use this endpoint — not /health — to determine mesh readiness.
  app.get('/ready', (_req, res) => {
    const { total, stale } = registry.getStats();
    const active = total - stale;
    if (active === 0) {
      return res.status(503).json({
        ready: false,
        reason: total === 0 ? 'no agents registered' : 'all agents stale',
        registry_agents: total,
        registry_stale: stale,
      });
    }
    return res.json({ ready: true, registry_agents: total, registry_active: active });
  });

  // -----------------------------------------------------------------
  // GET /registry
  // -----------------------------------------------------------------
  app.get('/registry', (_req, res) => {
    const agents = registry.getAll();
    res.json({
      agents,
      total: agents.length,
      stale: agents.filter((a) => a.status === 'stale').length,
      as_of: new Date().toISOString(),
    });
  });

  // -----------------------------------------------------------------
  // GET /routes
  // -----------------------------------------------------------------
  app.get('/routes', (_req, res) => {
    const active = registry.getAll().filter((a) => a.status === 'active');
    const routes = active.map((a) => registry.resolveRoute(a.agentId, cfg.routing)).filter(Boolean);
    res.json({ routes });
  });

  // -----------------------------------------------------------------
  // GET /deliveries/recent
  // -----------------------------------------------------------------
  app.get('/deliveries/recent', (req, res) => {
    const n = Math.min(
      parseInt(req.query.n ?? String(cfg.deliveriesRetention), 10),
      cfg.deliveriesRetention,
    );
    const deliveries = deliveryLog.recent(n);
    res.json({ deliveries, total_shown: deliveries.length, retention: cfg.deliveriesRetention });
  });

  return app;
}

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------

export async function startGateway() {
  const cfg = await loadConfig();

  mkdirSync(cfg.dataDir, { recursive: true });
  mkdirSync(cfg.logDir, { recursive: true });

  const registry = new Registry(cfg.registry, cfg.dataDir);
  registry.load(cfg.registry.bootstrap_seed);
  registry.startStaleSweep();

  const deliveryLog = new DeliveryLog(cfg.deliveriesRetention);
  const app = buildApp(cfg, registry, deliveryLog);

  return new Promise((resolve, reject) => {
    const server = app.listen(cfg.port, cfg.host, () => {
      console.log(`[clack] Clack Gateway ${SHIM_VERSION} listening on ${cfg.host}:${cfg.port}`);
      console.log(`[clack] allowlist: ${cfg.auth.allowlist_policy}, agents: ${cfg.auth.allowed_agents?.length ?? 0}`);
      resolve(server);
    });
    server.on('error', reject);

    for (const sig of ['SIGTERM', 'SIGINT']) {
      process.on(sig, () => {
        console.log(`[clack] ${sig} — shutting down`);
        registry.stopStaleSweep();
        registry._persist();
        server.close(() => process.exit(0));
      });
    }
  });
}
