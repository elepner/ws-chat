import { Context, Effect, HashMap, Layer, Ref, Scope, Stream } from "effect";

import { SessionId } from "./common";
import { Connection } from "./connection";
import { EventPublisher } from "./event-bus";

export interface ConnectionManager {
  register: (
    id: SessionId,
    connection: Connection,
  ) => Effect.Effect<void, never, Scope.Scope>;
  unregister: (id: SessionId) => Effect.Effect<void>;
  connections: Effect.Effect<HashMap.HashMap<SessionId, Connection>>;
}

export const ConnectionManager =
  Context.Service<ConnectionManager>("ConnectionManager");

export const makeConnectionManager = Effect.gen(function* () {
  const connectionsRef = yield* Ref.make(
    HashMap.empty<SessionId, Connection>(),
  );
  const eventPublisher = yield* EventPublisher;

  const unregister = (id: SessionId) =>
    Effect.gen(function* () {
      yield* Ref.update(connectionsRef, HashMap.remove(id));
      yield* eventPublisher.publish({
        _tag: "UserDisconnected",
        id,
      });
    });

  const register = (id: SessionId, connection: Connection) =>
    Effect.gen(function* () {
      yield* Ref.update(connectionsRef, HashMap.set(id, connection));
      yield* Effect.addFinalizer(() => unregister(id));
      yield* eventPublisher.publish({
        _tag: "UserConnected",
        id,
      });

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

  return ConnectionManager.of({
    register,
    unregister,
    connections: Ref.get(connectionsRef),
  });
});

export const ConnectionManagerLive = Layer.effect(
  ConnectionManager,
  makeConnectionManager,
);
