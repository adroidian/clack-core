/**
 * Harness-neutral delivery adapter contract.
 *
 * Keep this layer free of OpenClaw runtime assumptions. OpenClaw, Hermes,
 * Cloud Run, and custom receivers implement this contract behind their own
 * transport adapters.
 */

export type HarnessType = "openclaw" | "hermes" | "cloudrun" | "custom";

export type DeliveryTransport =
  | "http-wake"
  | "openclaw-task"
  | "hermes-wake"
  | "hermes-a2a"
  | "webhook"
  | "queue";

export type DeliveryPriority = "low" | "normal" | "high" | "urgent";

export interface DeliveryEnvelope {
  from: string;
  to: string;
  topic: string;
  message: string;
  priority: DeliveryPriority;
  idempotencyKey: string;
  taskId?: string;
  contextId?: string;
  metadata?: Record<string, unknown>;
}

export interface DeliveryTarget {
  agentId: string;
  harnessType: HarnessType;
  transport: DeliveryTransport;
  wakeUrl: string;
  hostId?: string;
  capabilities?: string[];
}

export type DeliveryStatus = "delivered" | "queued" | "retrying" | "failed" | "dead-lettered";

export interface DeliveryResult {
  status: DeliveryStatus;
  target: string;
  transport: DeliveryTransport;
  attempts: number;
  durationMs: number;
  remoteStatusCode?: number;
  remoteBody?: unknown;
  error?: string;
}

export interface HttpDeliveryRequest {
  url: string;
  method: "POST";
  headers: Record<string, string>;
  body: Record<string, unknown>;
}

export interface DeliveryAdapter {
  readonly harnessType: HarnessType;
  readonly transports: readonly DeliveryTransport[];
  supports(target: DeliveryTarget): boolean;
  deliver(envelope: DeliveryEnvelope, target: DeliveryTarget): Promise<DeliveryResult>;
}

export function buildHermesWakeRequest(
  envelope: DeliveryEnvelope,
  target: DeliveryTarget,
): HttpDeliveryRequest {
  if (target.harnessType !== "hermes") {
    throw new Error(`Hermes adapter cannot deliver to harnessType=${target.harnessType}`);
  }
  if (target.transport !== "hermes-wake" && target.transport !== "hermes-a2a") {
    throw new Error(`Hermes adapter cannot use transport=${target.transport}`);
  }

  return {
    url: target.wakeUrl,
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-clack-transport": target.transport,
      "x-clack-idempotency-key": envelope.idempotencyKey,
    },
    body: {
      from: envelope.from,
      to: target.agentId || envelope.to,
      topic: envelope.topic,
      message: envelope.message,
      priority: envelope.priority,
      idempotencyKey: envelope.idempotencyKey,
      taskId: envelope.taskId,
      contextId: envelope.contextId,
      metadata: envelope.metadata ?? {},
    },
  };
}

export function isHermesDeliveryTarget(target: DeliveryTarget): boolean {
  return (
    target.harnessType === "hermes" &&
    (target.transport === "hermes-wake" || target.transport === "hermes-a2a")
  );
}
