import { Data } from "effect";

import { InboundMessage, SessionId } from "./common";

export type ChatEvent = Data.TaggedEnum<{
  MessageReceived: {
    readonly from: SessionId;
    readonly message: InboundMessage;
  };
  UserConnected: { readonly id: SessionId };
  UserDisconnected: { readonly id: SessionId };
}>;

export const ChatEvent = Data.taggedEnum<ChatEvent>();
