/**
 * Clack Gateway — entry point
 *
 * Started by systemd: node /opt/clack-gateway/app/index.js
 * Delegates to chitin-shim.js which implements the full gateway.
 *
 * The upstream index.ts (OpenClaw plugin) is preserved but not
 * loaded here — it remains available for OpenClaw embedding.
 */
import { startGateway } from './chitin-shim.js';

startGateway().catch((err) => {
  console.error('[clack] fatal startup error:', err.message);
  process.exit(1);
});
