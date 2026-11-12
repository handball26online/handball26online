// Matchmaking (signaling) server for the handball game.
// It only introduces two players to each other (room code + WebRTC offer / answer / ICE candidates).
// The match itself NEVER goes through this server: the players talk directly (WebRTC), or through a TURN server.
//
// Environment variables (all optional):
//   PORT         - set by Render by itself
//   ICE_SERVERS  - JSON array for WebRTC, e.g. STUN + TURN:
//                  [{"urls":["stun:stun.l.google.com:19302"]},
//                   {"urls":["turn:YOUR_TURN_HOST:443?transport=tcp"],"username":"...","credential":"..."}]
//                  The game gets this list from here, so the TURN password is never inside the game.

const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";   // no 0/O, 1/I
const ROOM_TTL_MS = 15 * 60 * 1000;                       // a room nobody joined is deleted after 15 minutes

let ICE = [{ urls: ["stun:stun.l.google.com:19302"] }];
if (process.env.ICE_SERVERS) {
  try {
    const parsed = JSON.parse(process.env.ICE_SERVERS);
    if (Array.isArray(parsed) && parsed.length > 0) ICE = parsed;
  } catch (e) {
    console.error("ICE_SERVERS is not valid JSON, using the default STUN only");
  }
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("handball signaling server ok\n");
});

const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });
const rooms = new Map();   // code -> { host: ws, client: ws|null, timer }

function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function newCode() {
  for (let tries = 0; tries < 50; tries++) {
    let code = "";
    for (let i = 0; i < 5; i++) code += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
    if (!rooms.has(code)) return code;
  }
  return null;
}

function deleteRoom(code) {
  const room = rooms.get(code);
  if (!room) return;
  clearTimeout(room.timer);
  rooms.delete(code);
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.room = null;
  ws.role = null;
  ws.on("pong", () => { ws.isAlive = true; });
  send(ws, { t: "config", ice: ICE });

  ws.on("message", (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch (e) { return; }
    if (!msg || typeof msg.t !== "string") return;

    if (msg.t === "host" && !ws.room) {
      const code = newCode();
      if (!code) return send(ws, { t: "error", msg: "No free room code, try again." });
      const room = { host: ws, client: null, timer: null };
      room.timer = setTimeout(() => {
        send(room.host, { t: "error", msg: "The room expired (nobody joined in 15 minutes)." });
        deleteRoom(code);
      }, ROOM_TTL_MS);
      rooms.set(code, room);
      ws.room = code;
      ws.role = "host";
      send(ws, { t: "hosted", code });

    } else if (msg.t === "join" && !ws.room) {
      const code = String(msg.code || "").trim().toUpperCase();
      const room = rooms.get(code);
      if (!room) return send(ws, { t: "error", msg: "Room " + code + " does not exist." });
      if (room.client) return send(ws, { t: "error", msg: "Room " + code + " is already full." });
      room.client = ws;
      ws.room = code;
      ws.role = "client";
      send(room.host, { t: "peer_joined" });   // sent BEFORE \"joined\", so the host is ready for the offer
      send(ws, { t: "joined" });

    } else if (msg.t === "sig" && ws.room) {
      const room = rooms.get(ws.room);
      if (!room) return;
      const other = ws.role === "host" ? room.client : room.host;
      send(other, { t: "sig", data: msg.data });
    }
  });

  ws.on("close", () => {
    const code = ws.room;
    if (!code) return;
    const room = rooms.get(code);
    if (!room) return;
    if (ws.role === "host") {
      send(room.client, { t: "peer_left" });
      deleteRoom(code);
    } else if (room.client === ws) {
      room.client = null;                       // the host stays in the room, somebody else can join
      send(room.host, { t: "peer_left" });
    }
  });
});

// Render closes quiet connections: ping everybody every 25 seconds
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

server.listen(PORT, () => console.log("signaling server on port " + PORT));
