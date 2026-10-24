import { Effect, Layer, ManagedRuntime, Queue, Stream } from "effect";
import { WebSocketServer, WebSocket } from "ws";
const wss = new WebSocketServer({ port: 8080 });

let counter = 0;
const connections: Record<string, WebSocket> = {};

function makeConnection(id: string, ws: WebSocket) {
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
  }).pipe(Effect.scoped, Effect.annotateLogs("connectionId", id));
}

wss.on("connection", (ws) => {
  const id = `${counter++}`;
  runtime.runFork(makeConnection(id, ws));
});

const runtime = ManagedRuntime.make(Layer.empty);
