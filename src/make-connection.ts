import { Cause, Context, Effect, Layer, Queue, Stream } from "effect";
import { WebSocket } from "ws";

export type Inbox = Queue.Dequeue<string, Cause.Done>;
export const Inbox = Context.Service<Inbox>("Inbox");

export function makeConnectionLayer(
  ws: WebSocket,
  outbox: Queue.Dequeue<string>,
) {
  return Layer.effect(
    Inbox,
    Effect.gen(function* () {
      yield* Effect.log("Establishing connection...");
      const inbox = yield* Queue.unbounded<string, Cause.Done>();

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

      return inbox;
    }),
  );
}
