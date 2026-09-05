#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
#
# Writes to $GITHUB_OUTPUT: matrix=<json> (unit/IT stages) and run_e2e=true|false.
# Unit/IT test-only -> owning stages, run_e2e=false. e2e-only -> empty matrix, run_e2e=true.
# Else (main code, non-push, unknown base, drift) -> full matrix, run_e2e=true (fail-safe).
#
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
source "${HERE}/stage.sh"

ALL_STAGES=(core python table connect tests misc)

emit() {
  local json="[" first=1
  for s in "$@"; do
    [ $first -eq 1 ] || json+=","
    json+="{\"module\":\"${s}\",\"stringified-module-name\":\"${s}\"}"
    first=0
  done
  echo "matrix=${json}]" >> "${GITHUB_OUTPUT}"
  echo "Selected stages: $*"
}
emit_run_e2e() { echo "run_e2e=$1" >> "${GITHUB_OUTPUT}"; echo "run_e2e=$1"; }
# full matrix always implies running e2e (uncertain / main-code / fail-safe paths).
full() { emit "${ALL_STAGES[@]}"; emit_run_e2e true; }

# Drift guard: a module added/removed inside an existing list is fine (lists are sourced),
# but a new/renamed/removed MODULES_* list or stage would misroute silently -> full matrix.
assert_no_stage_sh_drift() {
  local expected_lists actual_lists expected_stages actual_stages
  expected_lists="$(printf '%s\n' MODULES_CONNECTORS MODULES_CORE MODULES_TABLE MODULES_TESTS | sort)"
  actual_lists="$( { compgen -A variable | grep -E '^MODULES_[A-Za-z0-9_]+$'; } 2>/dev/null | sort || true)"

  expected_stages="$(printf '%s\n' "${ALL_STAGES[@]}" | sort)"
  actual_stages="$(
    for v in $( { compgen -A variable | grep -E '^STAGE_[A-Za-z0-9_]+$'; } 2>/dev/null || true); do
      # compile/cleanup are not part of the test matrix
      case "$v" in STAGE_COMPILE|STAGE_CLEANUP) continue ;; esac
      printf '%s\n' "${!v}"
    done | sort)"

  if [ "${expected_lists}" != "${actual_lists}" ] || [ "${expected_stages}" != "${actual_stages}" ]; then
    echo "stage.sh structure drift detected -> full matrix (fail-safe). Update detect_test_stages.sh."
    echo "  module lists expected: $(echo ${expected_lists})"
    echo "  module lists actual  : $(echo ${actual_lists})"
    echo "  test stages  expected: $(echo ${expected_stages})"
    echo "  test stages  actual  : $(echo ${actual_stages})"
    full; exit 0
  fi
}
assert_no_stage_sh_drift

# Only narrow on push events with a valid previous SHA.
if [ "${GITHUB_EVENT_NAME:-}" != "push" ] || [ -z "${BASE_SHA:-}" ] || [[ "${BASE_SHA}" =~ ^0+$ ]]; then
  echo "Non-push event or no base SHA -> full matrix."
  full; exit 0
fi

CHANGED="$(gh api "repos/${GITHUB_REPOSITORY}/compare/${BASE_SHA}...${HEAD_SHA}" --jq '.files[].filename' 2>/dev/null)" \
  || { echo "compare API failed -> full matrix."; full; exit 0; }
[ -z "${CHANGED}" ] && { echo "No changed files -> full matrix."; full; exit 0; }

# module-path -> stage map from stage.sh's lists (longest prefix wins).
declare -A MODULE_STAGE
add() { local stage=$1; IFS=',' read -ra a <<< "$2"; for m in "${a[@]}"; do [ -n "$m" ] && MODULE_STAGE["$m"]="$stage"; done; }
add core    "${MODULES_CORE}"
add table   "${MODULES_TABLE}"
add connect "${MODULES_CONNECTORS}"
add tests   "${MODULES_TESTS}"

declare -A SELECTED
RUN_E2E=false
while IFS= read -r f; do
  [ -z "$f" ] && continue
  # e2e test change -> run the e2e job, but no unit/IT stage.
  if [[ "$f" == flink-end-to-end-tests/* ]]; then RUN_E2E=true; continue; fi
  # PyFlink is a self-contained leaf and its tests live outside src/test (flink-python/pyflink/**),
  # so any flink-python change maps to the python stage (and only that). Checked before the
  # src/test gate below.
  if [[ "$f" == flink-python/* ]]; then SELECTED[python]=1; continue; fi
  # any non-test change forces the full matrix.
  [[ "$f" == *"/src/test/"* ]] || { echo "Non-test change ($f) -> full matrix."; full; exit 0; }
  best=""; len=0
  for m in "${!MODULE_STAGE[@]}"; do
    if [[ "$f" == "$m/"* ]] && [ ${#m} -gt $len ]; then best="$m"; len=${#m}; fi
  done
  if [ -n "$best" ]; then SELECTED[${MODULE_STAGE[$best]}]=1; else SELECTED[misc]=1; fi
done <<< "${CHANGED}"

# Empty matrix means every change was an e2e test -> run no unit/IT stage (e2e job still runs).
emit "${!SELECTED[@]}"
emit_run_e2e "${RUN_E2E}"
