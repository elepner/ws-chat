import { Context, Effect, Layer, PubSub } from "effect";

import { ChatEvent } from "./events";

export interface EventPublisher {
  publish(event: ChatEvent): Effect.Effect<void>;
}

export const EventPublisher = Context.Service<EventPublisher>("EventPublisher");

export const EventPubSub =
  Context.Service<PubSub.PubSub<ChatEvent>>("EventPubSub");

export const EventPubSubLive = Layer.effect(
  EventPubSub,
  PubSub.unbounded<ChatEvent>(),
);

export const EventPublisherLive = Layer.effect(
  EventPublisher,
  Effect.gen(function* () {
    const pubsub = yield* EventPubSub;
    return {
      publish: (event) => PubSub.publish(pubsub, event),
    };
  }),
);
