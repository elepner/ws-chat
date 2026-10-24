import { Cause, Context, Effect, Layer, Queue, Stream } from "effect";
import { WebSocket } from "ws";

export interface Connection {
  inbox: Queue.Dequeue<string, Cause.Done>;
  send: (data: string) => Effect.Effect<void>;
}

export const Connection = Context.Service<Connection>("Connection");

export function makeConnectionLive(ws: WebSocket) {
  return Layer.effect(
    Connection,
    Effect.gen(function* () {
      yield* Effect.log("Establishing connection...");
      const inbox = yield* Queue.unbounded<string, Cause.Done>();
      const outbox = yield* Queue.unbounded<string, Cause.Done>();

      const msgHandler = (msg: string) => Queue.offerUnsafe(inbox, msg);
      ws.on("message", msgHandler);

      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          yield* Effect.log("Closing connection");
          yield* Effect.sync(() => {
            ws.off("message", msgHandler);
            Queue.endUnsafe(inbox);
          });
        }),
      );

      yield* Stream.fromQueue(outbox).pipe(
        Stream.runForEach((msg) => Effect.sync(() => ws.send(msg))),
        Effect.forkScoped,
      );

      return {
        inbox,
        send: (msg) => Effect.sync(() => Queue.offerUnsafe(outbox, msg)),
      };
    }),
  );
}
