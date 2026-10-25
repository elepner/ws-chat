import { Effect, Layer, Match, PubSub, Stream } from "effect";

import { makeOutboundMessage } from "./common";
import { EventPubSub } from "./event-bus";
import { ChatEvent } from "./events";
import { scheduleMessage, sendTo } from "./operations";

const EventRouter = Effect.gen(function* () {
  const pubsub = yield* EventPubSub;
  const subscription = yield* PubSub.subscribe(pubsub);

  yield* Stream.fromSubscription(subscription).pipe(
    Stream.runForEach(handleEvent),
  );
});

export const EventRouterLive = Layer.effectDiscard(EventRouter);

function handleEvent(chatEvent: ChatEvent) {
  return Match.value(chatEvent).pipe(
    Match.tag("UserConnected", ({ id }) =>
      Effect.all([
        sendTo(
          makeOutboundMessage(`Welcome to the chat, your session id is ${id}`),
          id,
        ),
        scheduleMessage(
          makeOutboundMessage("Be polite in the chat"),
          id,
          "10 seconds",
        ).pipe(Effect.catchTag("NoSuchConnection", Effect.orDie)),
      ]),
    ),
    Match.orElse(() => Effect.void),
  );
}
