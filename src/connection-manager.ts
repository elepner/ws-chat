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

import { Connection } from "./connection";

export interface ConnectionManager {
  register: (
    id: string,
    connection: Connection,
  ) => Effect.Effect<void, never, Scope.Scope>;
  unregister: (id: string) => Effect.Effect<void>;
  sendTo: (id: string, msg: string) => Effect.Effect<void>;
  broadcast: (msg: string, exclude: string) => Effect.Effect<void>;
}

export const ConnectionManager =
  Context.Service<ConnectionManager>("ConnectionManager");

export const makeConnectionManager = Effect.gen(function* () {
  const connections = yield* Ref.make(HashMap.empty<string, Connection>());

  const unregister = (id: string) =>
    Ref.update(connections, HashMap.remove(id));

  const sendTo = (id: string, msg: string) =>
    Ref.get(connections).pipe(
      Effect.map((map) => HashMap.get(map, id)),
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.void,
          onSome: (con) => con.send(msg),
        }),
      ),
    );

  const broadcast = (msg: string, exclude: string) =>
    Ref.get(connections).pipe(
      Effect.map((map) => HashMap.remove(map, exclude)),
      Effect.flatMap((map) =>
        Effect.forEach(HashMap.values(map), (con) => con.send(msg), {
          concurrency: "unbounded",
          discard: true,
        }),
      ),
    );

  const register = (id: string, connection: Connection) =>
    Effect.gen(function* () {
      yield* Ref.update(connections, HashMap.set(id, connection));
      yield* Effect.addFinalizer(() => unregister(id));

      yield* Stream.fromQueue(connection.inbox).pipe(
        Stream.runForEach((msg) =>
          Effect.gen(function* () {
            yield* broadcast(`New message from ${id}: ${msg}`, id);
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
