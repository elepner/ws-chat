import { Effect, Layer } from "effect";
import { WebSocketServer, WebSocket } from "ws";

import { Connection, makeConnectionLive } from "./connection";
import { ConnectionManager } from "./connection-manager";

export function makeHub(wss: WebSocketServer) {
  return Layer.effectDiscard(
    Effect.gen(function* () {
      const manager = yield* ConnectionManager;
      let counter = 0;

      const connectionHandler = (ws: WebSocket) => {
        const id = `${counter++}`;

        const program = Effect.gen(function* () {
          const con = yield* Connection;

          yield* manager.register(id, con);
        }).pipe(
          Effect.annotateLogs("connectionId", id),
          Effect.scoped,
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
    }),
  );
}
