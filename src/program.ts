import { WebSocketServer, WebSocket } from "ws";
const wss = new WebSocketServer({ port: 8080 });

let counter = 0;
const connections: Record<string, WebSocket> = {};

wss.on("connection", (ws) => {
  const connectionId = `${counter++}`;
  connections[connectionId] = ws;

  ws.on("error", console.error);

  ws.on("message", function message(data: string) {
    console.log(`received ${connectionId}: ${data}`);
    Object.entries(connections)
      .filter(([key]) => key !== connectionId)
      .forEach(([k, v]) => v.send(data));
  });

  ws.send("something");

  ws.once("close", () => {
    delete connections[connectionId];
  });
});

process.on("SIGINT", () => {
  console.log("Sig int");

  Object.values(connections).forEach((x) => x.terminate());
  process.exit(0);
});

process.on("SIGTERM", () => {
  console.log(`Sig term need to disconnect ${Object.keys(connections).length}`);
  Object.values(connections).forEach((x) => x.terminate());
  wss.close();
  process.exit(0);
});
