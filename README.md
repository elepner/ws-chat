# ws-chat

A small WebSocket chat server built as a study project for [Effect](https://effect.website) — structured concurrency, `Layer`-based dependency injection, `Stream`/`PubSub`, and scoped resource management, applied to something concrete instead of toy examples.

> **Note:** ~90% of the code in this repository was written by hand; AI assistance was used for targeted help, not to generate the implementation wholesale.

## Running

```sh
npm run dev      # start the server with --watch, reload on save
npm start        # start the server once
npm test         # run the test suite
npm run format   # format with vp fmt
```

The server listens on `ws://localhost:8080`. `npm run clients` opens two `websocat` sessions side by side in `tmux` for manual testing.

## Architecture

Each WebSocket connection runs as its own `Effect` fiber with its own `Scope`, registered with a central connection registry. Chat behavior (welcome messages, scheduled notices, broadcasts) is decoupled from both the transport and the registry — it reacts to domain events published on an in-process `PubSub`.

| File                    | Responsibility                                                                                                                                                        |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `connection.ts`         | Wraps one raw `ws` socket: inbound/outbound queues, and exposes the connection's own `Scope` so unrelated code can tie background work to this connection's lifetime. |
| `connection-manager.ts` | Tracks live connections (`SessionId -> Connection`) and publishes `UserConnected` / `UserDisconnected` / `MessageReceived` events. No messaging/business logic.       |
| `events.ts`             | The `ChatEvent` tagged union — the vocabulary connections and chat policy communicate through.                                                                        |
| `event-bus.ts`          | The `PubSub` + `EventPublisher` service wiring.                                                                                                                       |
| `event-router.ts`       | Subscribes to `ChatEvent`s and decides what to do about them (e.g. send a welcome message on connect).                                                                |
| `operations.ts`         | Reusable message-sending primitives: `sendTo`, `broadcast`, `sendOthers`, `scheduleMessage`.                                                                          |
| `hub.ts`                | Bridges the raw `WebSocketServer` to `ConnectionManager`, forking one fiber per connection.                                                                           |
| `program.ts`            | Composes all the layers and runs the app; turns unhandled fiber failures into a real process crash with a readable error instead of a silent hang.                    |

### Scoped scheduling, not defensive checks

`scheduleMessage` (in `operations.ts`) delays a send with `Effect.sleep` and forks it into the target connection's own `Scope` via `Effect.forkIn`. If the user disconnects before the delay elapses, that connection's scope closes and the scheduled fiber is interrupted automatically — no "is the user still connected?" branch anywhere in the code.
