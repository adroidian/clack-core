import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  buildHermesWakeRequest,
  isHermesDeliveryTarget,
  type DeliveryEnvelope,
  type DeliveryTarget,
} from "../src/internal/delivery.js";

const envelope: DeliveryEnvelope = {
  from: "vesper",
  to: "zari",
  topic: "ops",
  message: "status check",
  priority: "high",
  idempotencyKey: "msg-123",
  taskId: "task-123",
  contextId: "ctx-123",
};

describe("delivery adapter contract", () => {
  it("maps a generic envelope into the Hermes wake contract", () => {
    const target: DeliveryTarget = {
      agentId: "zari",
      harnessType: "hermes",
      transport: "hermes-wake",
      wakeUrl: "http://100.99.159.110:18890/wake",
      hostId: "ex",
    };

    const request = buildHermesWakeRequest(envelope, target);

    assert.equal(request.url, target.wakeUrl);
    assert.equal(request.method, "POST");
    assert.equal(request.headers["content-type"], "application/json");
    assert.equal(request.headers["x-clack-transport"], "hermes-wake");
    assert.equal(request.headers["x-clack-idempotency-key"], "msg-123");
    assert.deepEqual(request.body, {
      from: "vesper",
      to: "zari",
      topic: "ops",
      message: "status check",
      priority: "high",
      idempotencyKey: "msg-123",
      taskId: "task-123",
      contextId: "ctx-123",
      metadata: {},
    });
  });

  it("recognizes Hermes wake and A2A transports only for Hermes harnesses", () => {
    assert.equal(isHermesDeliveryTarget({
      agentId: "alf",
      harnessType: "hermes",
      transport: "hermes-a2a",
      wakeUrl: "http://ex:18890/wake",
    }), true);

    assert.equal(isHermesDeliveryTarget({
      agentId: "sable",
      harnessType: "openclaw",
      transport: "http-wake",
      wakeUrl: "http://omni:18789/hooks/agent",
    }), false);
  });

  it("rejects non-Hermes targets before building a Hermes request", () => {
    assert.throws(
      () => buildHermesWakeRequest(envelope, {
        agentId: "sable",
        harnessType: "openclaw",
        transport: "http-wake",
        wakeUrl: "http://omni:18789/hooks/agent",
      }),
      /cannot deliver to harnessType=openclaw/,
    );
  });
});
