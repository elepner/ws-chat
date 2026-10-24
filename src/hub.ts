import { Effect, Ref } from "effect";
import { WebSocketServer } from "ws";

export function makeHub(wss: WebSocketServer) {
  let counter = 0;
  wss.on("connection", (ws) => {
    const id = `${counter++}`;
  });
}
