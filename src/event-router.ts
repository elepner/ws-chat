import { Effect, Layer, PubSub, Stream } from "effect";

import { EventPubSub } from "./event-bus";

const EventRouter = Effect.gen(function* () {
  const pubsub = yield* EventPubSub;
  const subscription = yield* PubSub.subscribe(pubsub);

  yield* Stream.fromSubscription(subscription).pipe(
    Stream.runForEach((event) => Effect.log("Received event", event)),
  );
});

export const EventRouterLive = Layer.effectDiscard(EventRouter);
