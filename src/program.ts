import { Effect, Layer, ManagedRuntime } from "effect";
import { WebSocketServer } from "ws";

import { makeConnection } from "./make-connection";
const wss = new WebSocketServer({ port: 8080 });

let counter = 0;

wss.on("connection", (ws) => {
  const id = `${counter++}`;
  runtime.runFork(
    makeConnection(ws).pipe(Effect.annotateLogs("connectionId", id)),
  );
});

const runtime = ManagedRuntime.make(Layer.empty);
