import { Data, Duration, Effect, HashMap, Option } from "effect";

import { OutboundMessage, Self, SessionId } from "./common";
import { ConnectionManager } from "./connection-manager";

export function sendTo(message: OutboundMessage, to: SessionId) {
  return Effect.gen(function* () {
    yield* findConnection(to).pipe(
      Effect.flatMap((con) => con.send(message)),
      Effect.catchTag("NoSuchConnection", () => Effect.void),
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

export function scheduleMessage(
  message: OutboundMessage,
  to: SessionId,
  after: Duration.Input,
) {
  return findConnection(to).pipe(
    Effect.flatMap((con) =>
      Effect.sleep(after).pipe(
        Effect.andThen(() => con.send(message)),
        Effect.forkIn(con.scope),
      ),
    ),
    Effect.asVoid,
  );
}
export class NoSuchConnection extends Data.TaggedError("NoSuchConnection")<{
  sessionId: SessionId;
}> {}
function findConnection(id: SessionId) {
  return Effect.gen(function* () {
    const manager = yield* ConnectionManager;

    const result = yield* manager.connections.pipe(
      Effect.map((map) => HashMap.get(map, id)),
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.fail(new NoSuchConnection({ sessionId: id })),
          onSome: (con) => Effect.succeed(con),
        }),
      ),
    );
    return result;
  });
}
