import { Effect, HashMap, Option } from "effect";

import { OutboundMessage, Self, SessionId } from "./common";
import { ConnectionManager } from "./connection-manager";

export function sendTo(message: OutboundMessage, to: SessionId) {
  return Effect.gen(function* () {
    const manager = yield* ConnectionManager;
    yield* manager.connections.pipe(
      Effect.map((map) => HashMap.get(map, to)),
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.void,
          onSome: (con) => con.send(message),
        }),
      ),
    );
  });
}

export function broadcast(message: OutboundMessage, exclude: SessionId) {
  return Effect.gen(function* () {
    const manager = yield* ConnectionManager;
    yield* manager.connections.pipe(
      Effect.map((map) => HashMap.remove(map, exclude)),
      Effect.flatMap((map) =>
        Effect.forEach(HashMap.values(map), (con) => con.send(message), {
          concurrency: "unbounded",
          discard: true,
        }),
      ),
    );
  });
}

export function sendOthers(message: OutboundMessage) {
  return Effect.gen(function* () {
    const self = yield* Self;

    yield* broadcast(message, self);
  });
}
