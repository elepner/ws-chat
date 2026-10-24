import { Effect, Queue, Stream } from "effect";
import { WebSocket } from "ws";

export function makeConnection(ws: WebSocket) {
  return Effect.gen(function* () {
    yield* Effect.log("Establishing connection...");

    const messages = Stream.callback<string>((queue) => {
      const msgHandler = (msg: string) => {
        Queue.offerUnsafe(queue, msg);
      };
      const closeHandler = () => Queue.endUnsafe(queue);
      ws.on("message", msgHandler);
      ws.once("close", closeHandler);

      return Effect.addFinalizer(() =>
        Effect.gen(function* () {
          yield* Effect.log("Closing connection");
          yield* Effect.sync(() => {
            ws.off("message", msgHandler);
            ws.off("close", closeHandler);
          });
        }),
      );
    });
    yield* Stream.runForEach(messages, (data) =>
      Effect.log(`Data arrived: ${data}`),
    );
  }).pipe(Effect.scoped);
}
