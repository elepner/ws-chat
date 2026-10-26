import { Effect, Layer, Queue, Stream } from "effect";
import { WebSocketServer, WebSocket } from "ws";

import { makeSessionId } from "./common";
import { Connection, makeConnectionLive } from "./connection";
import { ConnectionManager } from "./connection-manager";

export function makeHub(wss: WebSocketServer) {
  const layerEffect = Effect.gen(function* () {
    let counter = 0;
    const connectionManager = yield* ConnectionManager;

    const connectionsStream = Stream.callback<WebSocket>(
      Effect.fn(function* (queue) {
        const onConnection = (con: WebSocket) => Queue.offerUnsafe(queue, con);

        yield* Effect.acquireRelease(
          Effect.sync(() => wss.on("connection", onConnection)),
          () => Effect.sync(() => wss.off("connection", onConnection)),
        );
      }),
    );

    yield* connectionsStream.pipe(
      Stream.runForEach((ws) =>
        Effect.gen(function* () {
          const connectionLayer = makeConnectionLive(ws);
          const id = makeSessionId(counter++);
          const program = Effect.gen(function* () {
            const connection = yield* Connection;
            yield* connectionManager.register(id, connection);
          }).pipe(
            Effect.annotateLogs("connectionId", id),
            Effect.provide(connectionLayer),
            Effect.scoped,
          );

          const fiber = yield* Effect.forkChild(program);

          ws.once("close", () => {
            fiber.interruptUnsafe();
          });
        }),
      ),
      Effect.forkScoped,
    );
  });

  return Layer.effectDiscard(layerEffect);
}
