// One job end to end. Usage: k6 run k6/smoke.js
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE = __ENV.BASE || 'http://localhost:4000';

export const options = { vus: 1, iterations: 1 };

export default function () {
  const res = http.post(
    `${BASE}/runs`,
    JSON.stringify({
      lang: 'cpp',
      code: '#include <bits/stdc++.h>\nint main(){ std::cout << "ok\\n"; }',
      inputString: '',
    }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  check(res, { 'submit accepted': (r) => r.status === 200 || r.status === 201 || r.status === 202 });
  const id = res.json('jobId');

  let state = null;
  for (let i = 0; i < 60 && !state; i++) {
    sleep(0.5);
    const r = http.get(`${BASE}/runs/${id}`);
    const b = r.json();
    if (b.status === 'DONE' || b.status === 'ERROR' || b.status === 'CANCELLED') state = b;
  }
  check(state, {
    'finished': (s) => s !== null,
    'verdict OK': (s) => s && s.verdict === 'OK',
    'stdout ok': (s) => s && String(s.stdout).trim() === 'ok',
  });
}