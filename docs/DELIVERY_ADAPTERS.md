# Delivery Adapters

Clack core keeps delivery harness-neutral. Routing chooses a target; delivery
adapters translate the generic envelope into the target harness contract.

## Core Contract

```ts
Envelope -> Router -> DeliveryAdapter -> DeliveryResult
```

The core envelope uses stable Clack fields:

- `from`
- `to`
- `topic`
- `message`
- `priority`
- `idempotencyKey`
- optional `taskId`
- optional `contextId`
- optional `metadata`

Harness-specific fields belong inside adapters, not in routing or registry
logic.

## Adapter Families

| Harness | Transports | Ownership |
|---|---|---|
| `openclaw` | `http-wake`, `openclaw-task` | OpenClaw plugin/runtime bridge |
| `hermes` | `hermes-wake`, `hermes-a2a` | Hermes receiver/sidecar bridge |
| `cloudrun` | `webhook`, `queue` | Hosted worker bridge |
| `custom` | `webhook`, `queue` | Site-specific bridge |

## Hermes Contract

Hermes receivers expose:

```http
POST /wake
Content-Type: application/json
X-Clack-Transport: hermes-wake
X-Clack-Idempotency-Key: <idempotencyKey>
```

Body:

```json
{
  "from": "vesper",
  "to": "zari",
  "topic": "ops",
  "message": "status check",
  "priority": "high",
  "idempotencyKey": "msg-123",
  "taskId": "task-123",
  "contextId": "ctx-123",
  "metadata": {}
}
```

The receiver resolves `to` to the local Hermes profile/session, queues or wakes
the profile as needed, and returns one of:

- `delivered`
- `queued`
- `retrying`
- `failed`
- `dead-lettered`

This is fleet-level. Zari is only the first proof route; Alf and other Hermes
agents should use the same adapter contract.
