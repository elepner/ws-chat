import { Effect, Layer, ManagedRuntime, Queue } from "effect";
import { WebSocketServer } from "ws";

import { Connection, makeConnectionLive } from "./connection";
import { Inbox, makeConnectionLayer } from "./make-connection";
const wss = new WebSocketServer({ port: 8080 });

let counter = 0;

wss.on("connection", (ws) => {
  const id = `${counter++}`;
  // const outbox = runtime.runSync(Queue.unbounded<string>());
  // const connection = ManagedRuntime.make(makeConnectionLayer(ws, outbox));
  // const inbox = connection.runSync(Inbox);

  const connectionRuntime = ManagedRuntime.make(makeConnectionLive(ws));

  ws.once("close", () => {
    connectionRuntime.runFork(connectionRuntime.disposeEffect);
  });

  connectionRuntime.runFork(
    Effect.gen(function* () {
      const con = yield* Connection;

      const msg = yield* Queue.take(con.inbox);
      yield* Effect.log(`Data arrived: ${msg}`);
    }).pipe(Effect.forever, Effect.annotateLogs("connectionId", id)),
  );
});

const runtime = ManagedRuntime.make(Layer.empty);
