import { Effect } from "effect";
import { WebSocketServer } from "ws";
export const hello = Effect.gen(function* () {
  yield* Effect.log("Hello, World!");
  return "Hello, World!";
});

const wss = new WebSocketServer({ port: 8080 });

if (import.meta.url === `file://${process.argv[1]}`) {
  wss.on("connection", function connection(ws) {
    ws.on("error", console.error);

    ws.on("message", function message(data: string) {
      console.log("received: %s", data);
    });

    ws.send("something");
  });

  Effect.runPromise(hello);
}
