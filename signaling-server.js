/*
  Servidor de señalización WebRTC · Bethel  (estilo push/view por nombre de sala)
  --------------------------------------------------------------------------------
  Solo retransmite mensajes JSON entre los peers de una misma sala. No toca el
  media: el video/audio va peer-to-peer por WebRTC. Equivale al "handshake server"
  de VDO.Ninja.

  Instalar y correr:
      npm init -y
      npm install ws
      node signaling-server.js          # escucha en :8080 (ws://)

  En producción, ponlo detrás de TLS (Caddy/Nginx) para obtener wss://, p.ej.:
      wss://senial.bethel.tu-dominio.com
  y en la app pon ese valor en Ajustes ▸ WebSocket propio, transporte "WS propio".

  Protocolo (cada mensaje es JSON):
    {type:'join',  room, from}          -> el servidor asocia este socket a la sala
    {type:'push-ready'|'hello'|'offer'|'answer'|'ice'|'bye', room, from, to?, ...}
    El servidor reenvía todo (excepto 'join') a los demás de la sala.
    Si trae 'to', se entrega solo a ese peer; si no, se difunde a la sala.
*/

const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

// room -> Set<ws>
const rooms = new Map();

function join(ws, room, id) {
  ws._room = room; ws._id = id;
  if (!rooms.has(room)) rooms.set(room, new Set());
  rooms.get(room).add(ws);
  console.log(`[${room}] + ${id}  (${rooms.get(room).size} en sala)`);
}

function leave(ws) {
  const r = ws._room && rooms.get(ws._room);
  if (!r) return;
  r.delete(ws);
  // avisar que se fue
  relay(ws._room, { type: 'bye', from: ws._id }, ws);
  if (r.size === 0) rooms.delete(ws._room);
  console.log(`[${ws._room}] - ${ws._id}`);
}

function relay(room, msg, sender) {
  const r = rooms.get(room);
  if (!r) return;
  const data = JSON.stringify(msg);
  for (const peer of r) {
    if (peer === sender) continue;                 // nunca al emisor
    if (msg.to && peer._id !== msg.to) continue;    // dirigido: solo al destino
    if (peer.readyState === peer.OPEN) peer.send(data);
  }
}

wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (msg.type === 'join') { join(ws, msg.room || 'default', msg.from || 'anon'); return; }
    if (!ws._room) return;                          // ignora hasta que se una
    relay(ws._room, msg, ws);
  });
  ws.on('close', () => leave(ws));
  ws.on('error', () => leave(ws));
});

console.log(`Señalización Bethel escuchando en ws://0.0.0.0:${PORT}`);
