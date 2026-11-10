#!/usr/bin/env bash
# Functional checks for the warm pool (cpp). Needs: curl, jq, docker. Run from the repo root.
# ASSUMPTIONS (edit if wrong): POST /runs takes {"lang","code","inputString"} and returns {"jobId": "..."};
# GET /runs/{id} returns flat fields status, verdict, stdout, stderr, wallMs.

BASE=${BASE:-http://localhost:4000}
pass=0; fail=0

check() { # name, expected, actual
  if [[ "$3" == "$2" ]]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1  (expected '$2', got '$3')"; fail=$((fail+1)); fi
}

submit() { # code -> jobId
  curl -s -X POST "$BASE/runs" -H 'Content-Type: application/json' \
    -d "$(jq -n --arg c "$1" '{lang:"cpp",code:$c,inputString:""}')" | jq -r '.jobId'
}

poll() { # jobId -> JSON of final state (waits up to 45s)
  local id=$1 out st
  for _ in $(seq 90); do
    out=$(curl -s "$BASE/runs/$id")
    st=$(echo "$out" | jq -r '.status')
    [[ "$st" == "DONE" || "$st" == "ERROR" || "$st" == "CANCELLED" ]] && { echo "$out"; return; }
    sleep 0.5
  done
  echo "$out"
}

pool_ids() { docker ps -q --filter label=rce.pool=1 | sort; }
new_since() { comm -13 <(echo "$1") <(pool_ids) | wc -l | tr -d ' '; }  # containers not in snapshot $1

echo "pool size: $(pool_ids | wc -l)"
BEFORE=$(pool_ids)

# 1. normal job
R=$(poll "$(submit '#include <iostream>
int main(){ std::cout << "hello\n"; }')")
check "normal: verdict" OK "$(echo "$R" | jq -r .verdict)"
check "normal: stdout" hello "$(echo "$R" | jq -r .stdout | tr -d '\n')"
echo "      wallMs=$(echo "$R" | jq -r .wallMs)"
check "normal: no slot recycled" 0 "$(new_since "$BEFORE")"

# 2. compile error
R=$(poll "$(submit 'int main(){ undefined_fn(); }')")
check "compile error: verdict" RE "$(echo "$R" | jq -r .verdict)"
check "compile error: no slot recycled" 0 "$(new_since "$BEFORE")"

# 3. timeout -> TLE and exactly one slot recycled
R=$(poll "$(submit 'int main(){ while(1); }')")
check "timeout: verdict" TLE "$(echo "$R" | jq -r .verdict)"
echo "      wallMs=$(echo "$R" | jq -r .wallMs)"
sleep 2
check "timeout: one slot recycled" 1 "$(new_since "$BEFORE")"
check "timeout: pool size still 18" 18 "$(pool_ids | wc -l | tr -d ' ')"

# 4. cancel -> CANCELLED and one more slot recycled
BEFORE=$(pool_ids)
ID=$(submit 'int main(){ while(1); }')
sleep 3
curl -s -X DELETE "$BASE/runs/$ID" > /dev/null
R=$(poll "$ID")
check "cancel: status" CANCELLED "$(echo "$R" | jq -r .status)"
sleep 2
check "cancel: one slot recycled" 1 "$(new_since "$BEFORE")"

# 5. leakage + reuse: 40 sequential jobs spread over 18 slots, so slots get reused.
# Each job writes a uniquely named file into /tmp and /run-code, then counts entries.
# Clean state means every job prints "1 2" (own file in /tmp; main + own file in /run-code).
LEAK='#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <filesystem>
namespace fs = std::filesystem;
int main(){
  std::string n = "m_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  std::ofstream("/tmp/" + n) << "x";
  std::ofstream("/run-code/" + n) << "x";
  int a = 0, b = 0;
  for (auto& e : fs::directory_iterator("/tmp")) a++;
  for (auto& e : fs::directory_iterator("/run-code")) b++;
  std::cout << a << " " << b << "\n";
}'
bad=0
for i in $(seq 40); do
  R=$(poll "$(submit "$LEAK")")
  out=$(echo "$R" | jq -r .stdout | tr -d '\n')
  [[ "$(echo "$R" | jq -r .verdict)" == "OK" && "$out" == "1 2" ]] || { bad=$((bad+1)); echo "      job $i: verdict=$(echo "$R" | jq -r .verdict) wallMs=$(echo "$R" | jq -r .wallMs) out='$out' err='$(echo "$R" | jq -r .stderr | head -c 300)'"; }
done
check "40 sequential jobs, no leakage, all OK" 0 "$bad"

echo; echo "passed=$pass failed=$fail"