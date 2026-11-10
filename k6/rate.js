// Open-loop load test: jobs arrive at a constant rate regardless of how fast they finish.
// Usage: k6 run -e RATE=5 k6/rate.js
// Env: RATE (jobs/s, default 3), DURATION (default 60s), PROG (heavy|light, default heavy),
//      BASE (default http://localhost:4000), MAX_WAIT_S (poll limit per job, default 120)
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const BASE = __ENV.BASE || 'http://localhost:4000';
const RATE = Number(__ENV.RATE || 3);
const DURATION = __ENV.DURATION || '60s';
const MAX_WAIT_S = Number(__ENV.MAX_WAIT_S || 120);

const PROGRAMS = {
  heavy: '#include <bits/stdc++.h>\nint main(){ long long s=0; for(int i=0;i<1000000;i++) s+=i; std::cout << s << "\\n"; }',
  light: '#include <iostream>\nint main(){ long long s=0; for(int i=0;i<1000000;i++) s+=i; std::cout << s << "\\n"; }',
};
const CODE = PROGRAMS[__ENV.PROG || 'heavy'];
const EXPECTED = '499999500000';

const e2e = new Trend('job_e2e_ms', true);       // submit -> terminal status, as seen by the client
const exec = new Trend('job_exec_ms', true);     // wallMs stored on the result (execution time)
const submitted = new Counter('jobs_submitted');
const done = new Counter('jobs_done');           // reached DONE with verdict OK and correct output
const failed = new Counter('jobs_failed');       // anything else, including poll timeout

export const options = {
  scenarios: {
    arrivals: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 200,
      maxVUs: 1000,
    },
  },
  // Summary is what matters; no pass/fail thresholds yet.
};

const JSON_HEADERS = { headers: { 'Content-Type': 'application/json' } };

export default function () {
  const t0 = Date.now();
  const res = http.post(
    `${BASE}/runs`,
    JSON.stringify({ lang: 'cpp', code: CODE, inputString: '' }),
    Object.assign({ tags: { name: 'submit' } }, JSON_HEADERS),
  );
  if (res.status !== 200 && res.status !== 201 && res.status !== 202) {
    failed.add(1);
    return;
  }
  const id = res.json('jobId');
  submitted.add(1);

  // Adaptive polling: 0.5s while young, then 1s, then 2s as the job ages.
  let state = null;
  let waited = 0;
  while (waited < MAX_WAIT_S) {
    const pause = waited < 3 ? 0.5 : waited < 15 ? 1 : 2;
    sleep(pause);
    waited += pause;
    const r = http.get(`${BASE}/runs/${id}`, { tags: { name: 'poll' } });
    if (r.status !== 200) continue;
    const body = r.json();
    if (body.status === 'DONE' || body.status === 'ERROR' || body.status === 'CANCELLED') {
      state = body;
      break;
    }
  }

  if (!state) {
    failed.add(1);
    return;
  }
  e2e.add(Date.now() - t0);
  if (state.wallMs) exec.add(Number(state.wallMs));

  const ok = check(state, {
    'status DONE': (s) => s.status === 'DONE',
    'verdict OK': (s) => s.verdict === 'OK',
    'correct output': (s) => String(s.stdout).trim() === EXPECTED,
  });
  if (ok) done.add(1);
  else failed.add(1);
}