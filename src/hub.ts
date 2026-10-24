import { Effect, Queue, Ref } from "effect";
import { WebSocketServer, WebSocket } from "ws";

import { Connection, makeConnectionLive } from "./connection";

export function makeHub(wss: WebSocketServer) {
  return Effect.gen(function* () {
    let counter = 0;
    const connectionHandler = (ws: WebSocket) => {
      const id = `${counter++}`;

      const program = Effect.gen(function* () {
        const con = yield* Connection;

        yield* Effect.gen(function* () {
          const msg = yield* Queue.take(con.inbox);
          yield* Effect.log(`Data arrived: ${msg}`);
        }).pipe(Effect.forever);
      }).pipe(
        Effect.annotateLogs("connectionId", id),
        Effect.provide(makeConnectionLive(ws)),
      );

      const fiber = Effect.runFork(program);

      ws.once("close", () => {
        fiber.interruptUnsafe();
      });
    };

    wss.on("connection", connectionHandler);

    yield* Effect.addFinalizer(() =>
      Effect.sync(() => wss.off("connection", connectionHandler)),
    );
  });
}
