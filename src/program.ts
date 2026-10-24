import { Effect } from "effect";
import { WebSocketServer } from "ws";

import { makeHub } from "./hub";
const wss = new WebSocketServer({ port: 8080 });

const hub = makeHub(wss).pipe(Effect.scoped);

const fiber = Effect.runFork(hub);

["SIGINT", "SIGTERM"].forEach((signal) => {
  process.on(signal, () => {
    fiber.addObserver(() => process.exit(0));
    fiber.interruptUnsafe();
  });
});
