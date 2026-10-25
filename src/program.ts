import { Effect, Layer } from "effect";
import { WebSocketServer } from "ws";

import { ConnectionManagerLive } from "./connection-manager";
import { EventBusLive } from "./event-bus";
import { makeHub } from "./hub";
const wss = new WebSocketServer({ port: 8080 });

const AppLive = Layer.merge(ConnectionManagerLive, EventBusLive);
const HubLive = makeHub(wss).pipe(Layer.provide(AppLive));

const fiber = Effect.runFork(Layer.launch(HubLive));

["SIGINT", "SIGTERM"].forEach((signal) => {
  process.on(signal, () => {
    fiber.addObserver(() => process.exit(0));
    fiber.interruptUnsafe();
  });
});

// const inputMessages = new Subject<string>();

// inputMessages
//   .pipe(
//     exhaustMap((msg) => {
//       return inputMessages.pipe(
//         map(() => "only 1 msg each 5s"),
//         startWith(msg),
//         takeUntil(timer(5000)),
//       );
//     }),
//   )
//   .subscribe((obj) => {
//     console.log("Arrived message: ", obj);
//   });

// interval(1000).subscribe((i) => {
//   inputMessages.next(`Trying to write message ${i}`);
// });
