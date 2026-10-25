import { Effect } from "effect";

import { OutboundMessage, Self } from "./common";
import { ConnectionManager } from "./connection-manager";

export function sendSelf(message: OutboundMessage) {
  return Effect.gen(function* () {
    const manager = yield* ConnectionManager;
    const self = yield* Self;

    manager.sendTo(self, message);
  });
}

export function sendOthers(message: OutboundMessage) {
  return Effect.gen(function* () {
    const manager = yield* ConnectionManager;
    const self = yield* Self;
  });
}
