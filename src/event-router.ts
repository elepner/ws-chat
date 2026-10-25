import { Effect, Layer, Match, PubSub, Stream } from "effect";

import { makeOutboundMessage } from "./common";
import { EventPubSub } from "./event-bus";
import { ChatEvent } from "./events";
import { broadcast, scheduleMessage, sendTo } from "./operations";

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
        broadcast(makeOutboundMessage(`User ${id} has joined the chat`), id),
        scheduleMessage(
          makeOutboundMessage("Be polite in the chat"),
          id,
          "10 seconds",
        ).pipe(Effect.catchTag("NoSuchConnection", Effect.orDie)),
      ]),
    ),
    Match.tag("MessageReceived", (msg) => {
      return broadcast(
        makeOutboundMessage(`Message from ${msg.from}: ${msg.message}`),
        msg.from,
      );
    }),
    Match.tag("UserDisconnected", (msg) => {
      return broadcast(
        makeOutboundMessage(`User ${msg.id} has left the chat`),
        msg.id,
      );
    }),
    Match.orElse(() => Effect.void),
  );
}
