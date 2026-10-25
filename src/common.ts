import { Context, Effect, PubSub, Schema } from "effect";

import { ChatEvent } from "./events";

export const SessionId = Schema.String.pipe(Schema.brand("SessionId"));
export type SessionId = Schema.Schema.Type<typeof SessionId>;
export function makeSessionId(val: number | string): SessionId {
  return val.toString() as SessionId;
}

export const OutboundMessage = Schema.String.pipe(
  Schema.brand("OutboundMessage"),
);
export type OutboundMessage = Schema.Schema.Type<typeof OutboundMessage>;
export function makeOutboundMessage(val: number | string): OutboundMessage {
  return val.toString() as OutboundMessage;
}

export const InboundMessage = Schema.String.pipe(
  Schema.brand("InboundMessage"),
);
export type InboundMessage = Schema.Schema.Type<typeof InboundMessage>;
export function makeInboundMessage(val: number | string): InboundMessage {
  return val.toString() as InboundMessage;
}

export const Self = Context.Service<SessionId>("Session/Self");
