import {
  Context,
  Effect,
  HashMap,
  Layer,
  Option,
  Ref,
  Scope,
  Stream,
} from "effect";

import { OutboundMessage, SessionId } from "./common";
import { Connection } from "./connection";
import { EventPublisher } from "./event-bus";

export interface ConnectionManager {
  register: (
    id: SessionId,
    connection: Connection,
  ) => Effect.Effect<void, never, Scope.Scope>;
  unregister: (id: SessionId) => Effect.Effect<void>;
  sendTo: (id: SessionId, msg: OutboundMessage) => Effect.Effect<void>;
  broadcast: (msg: OutboundMessage, exclude: SessionId) => Effect.Effect<void>;
}

export const ConnectionManager =
  Context.Service<ConnectionManager>("ConnectionManager");

export const makeConnectionManager = Effect.gen(function* () {
  const connections = yield* Ref.make(HashMap.empty<SessionId, Connection>());
  const eventPublisher = yield* EventPublisher;

  const unregister = (id: SessionId) =>
    Ref.update(connections, HashMap.remove(id));

  const sendTo = (id: SessionId, msg: OutboundMessage) =>
    Ref.get(connections).pipe(
      Effect.map((map) => HashMap.get(map, id)),
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.void,
          onSome: (con) => con.send(msg),
        }),
      ),
    );

  const broadcast = (msg: OutboundMessage, exclude: SessionId) =>
    Ref.get(connections).pipe(
      Effect.map((map) => HashMap.remove(map, exclude)),
      Effect.flatMap((map) =>
        Effect.forEach(HashMap.values(map), (con) => con.send(msg), {
          concurrency: "unbounded",
          discard: true,
        }),
      ),
    );

  const register = (id: SessionId, connection: Connection) =>
    Effect.gen(function* () {
      yield* Ref.update(connections, HashMap.set(id, connection));
      yield* Effect.addFinalizer(() => unregister(id));

      yield* Stream.fromQueue(connection.inbox).pipe(
        Stream.runForEach((msg) =>
          Effect.gen(function* () {
            yield* eventPublisher.publish({
              _tag: "MessageReceived",
              from: id,
              message: msg,
            });
            yield* Effect.log("Message received " + msg);
          }),
        ),
      );
    });

  return ConnectionManager.of({ register, unregister, sendTo, broadcast });
});

export const ConnectionManagerLive = Layer.effect(
  ConnectionManager,
  makeConnectionManager,
);
