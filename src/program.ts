import { Effect, Layer } from "effect";
import { WebSocketServer } from "ws";

import { ConnectionManagerLive } from "./connection-manager";
import { makeHub } from "./hub";
const wss = new WebSocketServer({ port: 8080 });

const HubLive = makeHub(wss).pipe(Layer.provide(ConnectionManagerLive));

const fiber = Effect.runFork(Layer.launch(HubLive));

["SIGINT", "SIGTERM"].forEach((signal) => {
  process.on(signal, () => {
    fiber.addObserver(() => process.exit(0));
    fiber.interruptUnsafe();
  });
});
