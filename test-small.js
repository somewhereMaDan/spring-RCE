import ws from 'k6/ws';
import { check } from 'k6';
import { Trend } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

const runResponseTime = new Trend('run_response_time_ms');
const BASE_WS_URL = 'ws://localhost:8080/ws';

function randomDigits(n) { let s=''; for (let i=0;i<n;i++) s+=Math.floor(Math.random()*10); return s; }
function sockjsFrame(f) { return JSON.stringify([f]); }
function stompConnectFrame() { return `CONNECT\naccept-version:1.1,1.0\nheart-beat:0,0\n\n\u0000`; }
function stompSubscribeFrame(id, dest) { return `SUBSCRIBE\nid:${id}\ndestination:${dest}\n\n\u0000`; }
function stompSendFrame(dest, bodyObj) { return `SEND\ndestination:${dest}\ncontent-type:application/json\n\n${JSON.stringify(bodyObj)}\u0000`; }

export const options = {
  scenarios: {
    burst_2000: {
      executor: 'per-vu-iterations',
      vus: 2000,
      iterations: 1,
      maxDuration: '10m',
    },
  },
  thresholds: {
    checks: ['rate>0.95'],
  },
};

export default function () {
  const serverId = randomDigits(3);
  const sessionPart = uuidv4().replace(/-/g, '').substring(0, 8);
  const sessionId = uuidv4();
  const url = `${BASE_WS_URL}/${serverId}/${sessionPart}/websocket`;
  const startTime = Date.now();
  let connected = false, subscribed = false, gotResponse = false;

  const res = ws.connect(url, {}, function (socket) {
    socket.on('message', function (data) {
      if (data === 'o') { socket.send(sockjsFrame(stompConnectFrame())); return; }
      if (data === 'h') return;
      if (data.startsWith('a')) {
        const messages = JSON.parse(data.substring(1));
        messages.forEach((msg) => {
          if (msg.startsWith('CONNECTED')) {
            connected = true;
            socket.send(sockjsFrame(stompSubscribeFrame('sub-0', `/topic/output/${sessionId}`)));
            subscribed = true;
            socket.send(sockjsFrame(stompSendFrame('/app/run', {
              sessionId: sessionId, lang: 'python', code: 'print("hello world")', inputString: '',
            })));
          } else if (msg.startsWith('MESSAGE')) {
            const bodyMatch = msg.match(/\n\n([\s\S]*)\u0000?$/);
            const body = bodyMatch ? bodyMatch[1].replace(/\u0000$/, '') : '';
            try {
              const parsed = JSON.parse(body);
              if (parsed.type === 'exit' && !gotResponse) {
                gotResponse = true;
                const elapsed = Date.now() - startTime;
                runResponseTime.add(elapsed);
                socket.close();
              }
            } catch (e) {}
          }
        });
      }
    });
    socket.on('error', function (e) { console.log(`[error] ${e.error()}`); });
    socket.setTimeout(function () {
      if (!gotResponse) console.log(`[timeout] session ${sessionId} never got exit frame`);
      socket.close();
    }, 8 * 60 * 1000);
  });

  check(res, { 'ws handshake succeeded': (r) => r && r.status === 101 });
  check(null, { 'stomp connected': () => connected, 'subscribed': () => subscribed, 'got run response': () => gotResponse });
}