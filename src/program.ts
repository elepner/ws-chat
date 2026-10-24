import { Effect, Layer, ManagedRuntime, Queue } from "effect";
import { WebSocketServer, WebSocket } from "ws";
const wss = new WebSocketServer({ port: 8080 });

let counter = 0;
const connections: Record<string, WebSocket> = {};

function makeConnection(id: string, ws: WebSocket) {
  return Effect.gen(function* () {
    yield* Effect.log("Establishing connection...");
    const msgHandler = (data: string) => {
      console.log(`Data arrived: ${data}`);
    };
    ws.on("message", msgHandler);
    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        yield* Effect.log("Closing connection");
        yield* Effect.sync(() => ws.off("message", msgHandler));
      }),
    );
    yield* Effect.callback<void>((resume) => {
      ws.once("close", () => resume(Effect.void));
    });
  }).pipe(Effect.scoped, Effect.annotateLogs("connectionId", id));
}

wss.on("connection", (ws) => {
  const id = `${counter++}`;
  runtime.runFork(makeConnection(id, ws));
});

const runtime = ManagedRuntime.make(Layer.empty);
