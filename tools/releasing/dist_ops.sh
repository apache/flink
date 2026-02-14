#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

##
## SVN distribution operations for Flink releases.
## Manages artifacts on dist.apache.org (dev and release repositories).
##
## Usage:
##   tools $ RELEASE_VERSION=1.20.0 RC_NUM=1 releasing/dist_ops.sh stage
##   tools $ RELEASE_VERSION=1.20.0 RC_NUM=1 releasing/dist_ops.sh promote
##   tools $ RELEASE_VERSION=1.20.0 releasing/dist_ops.sh cleanup-rcs
##   tools $ RELEASE_VERSION=1.20.0 releasing/dist_ops.sh cleanup-old
##
## Environment:
##   RELEASE_VERSION — release version (required for stage/promote/cleanup)
##   RC_NUM          — release candidate number (required for stage/promote)
##   DRY_RUN=true    — print commands without executing
##

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

CURR_DIR=`pwd`
if [[ `basename $CURR_DIR` != "tools" ]] ; then
  echo "You have to call the script from the tools/ dir"
  exit 1
fi

DRY_RUN=${DRY_RUN:-false}
DIST_DEV_URL="https://dist.apache.org/repos/dist/dev/flink"
DIST_RELEASE_URL="https://dist.apache.org/repos/dist/release/flink"

COMMAND=${1:-help}

FLINK_DIR=`cd .. && pwd`
RELEASE_DIR=${FLINK_DIR}/tools/releasing/release

run_cmd() {
  if [ "$DRY_RUN" = "true" ]; then
    echo "[dry-run] $*"
    return 0
  fi
  "$@"
}

cmd_stage() {
  if [ -z "${RELEASE_VERSION:-}" ]; then echo "RELEASE_VERSION not set."; exit 1; fi
  if [ -z "${RC_NUM:-}" ]; then echo "RC_NUM not set."; exit 1; fi

  local rc_dir="flink-${RELEASE_VERSION}-rc${RC_NUM}"

  echo "=== Staging release artifacts to dist.apache.org/dev ==="
  echo ""

  if [ ! -d "$RELEASE_DIR" ]; then
    echo "ERROR: Release directory not found: $RELEASE_DIR"
    echo "Run create_source_release.sh and create_binary_release.sh first."
    exit 1
  fi

  local work_dir=`mktemp -d`

  echo "Checking out dist dev repo (shallow)..."
  run_cmd svn checkout "${DIST_DEV_URL}" --depth=immediates "$work_dir/flink"

  echo "Creating RC directory: ${rc_dir}"
  run_cmd mkdir -p "$work_dir/flink/${rc_dir}"

  # Copy release artifacts — handle glob directly, not through run_cmd
  echo "Copying release artifacts..."
  if [ "$DRY_RUN" = "true" ]; then
    echo "[dry-run] cp -r ${RELEASE_DIR}/* $work_dir/flink/${rc_dir}/"
  else
    cp -r "${RELEASE_DIR}/"* "$work_dir/flink/${rc_dir}/"
  fi

  echo "Adding to SVN..."
  run_cmd svn add "$work_dir/flink/${rc_dir}"

  echo "Committing..."
  run_cmd svn commit "$work_dir/flink" -m "Add flink-${RELEASE_VERSION}-rc${RC_NUM}"

  echo ""
  echo "Staged at: ${DIST_DEV_URL}/flink-${RELEASE_VERSION}-rc${RC_NUM}"
  echo "Verify at: https://dist.apache.org/repos/dist/dev/flink/"

  rm -rf "$work_dir"
}

cmd_promote() {
  if [ -z "${RELEASE_VERSION:-}" ]; then echo "RELEASE_VERSION not set."; exit 1; fi
  if [ -z "${RC_NUM:-}" ]; then echo "RC_NUM not set."; exit 1; fi

  local rc_dir="flink-${RELEASE_VERSION}-rc${RC_NUM}"

  echo "=== Promoting RC to release ==="
  echo ""
  echo "Source: ${DIST_DEV_URL}/${rc_dir}"
  echo "Target: ${DIST_RELEASE_URL}/flink-${RELEASE_VERSION}"
  echo ""
  echo "WARNING: This makes the release publicly available."
  read -r -p "Are you sure? [y/N] " confirm
  if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "Aborted."
    exit 0
  fi

  run_cmd svn move -m "Release Flink ${RELEASE_VERSION}" \
    "${DIST_DEV_URL}/${rc_dir}" \
    "${DIST_RELEASE_URL}/flink-${RELEASE_VERSION}"

  echo "Promoted. Verify at: https://dist.apache.org/repos/dist/release/flink/"
}

cmd_cleanup_rcs() {
  if [ -z "${RELEASE_VERSION:-}" ]; then echo "RELEASE_VERSION not set."; exit 1; fi

  echo "=== Cleaning up old RCs for ${RELEASE_VERSION} ==="
  echo ""

  local work_dir=`mktemp -d`

  run_cmd svn checkout "${DIST_DEV_URL}" --depth=immediates "$work_dir/flink"

  local found=0
  for rc_dir in "$work_dir"/flink/flink-"${RELEASE_VERSION}"-rc*; do
    if [ -d "$rc_dir" ]; then
      echo "Removing: `basename $rc_dir`"
      run_cmd svn remove "$rc_dir"
      found=$((found + 1))
    fi
  done

  if [ "$found" -eq 0 ]; then
    echo "No old RCs found."
  else
    run_cmd svn commit "$work_dir/flink" -m "Remove old release candidates for Apache Flink ${RELEASE_VERSION}"
    echo "Removed $found RC(s)."
  fi

  rm -rf "$work_dir"
}

cmd_cleanup_old() {
  if [ -z "${RELEASE_VERSION:-}" ]; then echo "RELEASE_VERSION not set."; exit 1; fi

  echo "=== Checking for outdated versions to remove ==="
  echo ""
  echo "Current release: ${RELEASE_VERSION}"
  echo ""

  local versions=`svn list "${DIST_RELEASE_URL}" 2>/dev/null | grep "^flink-" | sed 's/flink-//;s/\///'`

  echo "Versions in release repo:"
  echo "$versions" | while read -r v; do
    echo "  $v"
  done
  echo ""

  echo "You should remove versions per ASF policy:"
  echo "  - New major: remove releases older than 2 major versions"
  echo "  - New bugfix: remove previous bugfix in same series"
  echo ""
  echo "To remove a version:"
  echo "  svn remove ${DIST_RELEASE_URL}/flink-<version>"
  echo "  (requires PMC access)"
}

cmd_help() {
  echo "Usage: RELEASE_VERSION=x.y.z [RC_NUM=n] releasing/dist_ops.sh <command>"
  echo ""
  echo "Commands:"
  echo "  stage        Stage RC artifacts to dist.apache.org/dev"
  echo "  promote      Move RC from dev to release (irreversible)"
  echo "  cleanup-rcs  Remove old RCs for this version from dev"
  echo "  cleanup-old  Show outdated versions that should be removed"
  echo ""
  echo "Environment variables:"
  echo "  RELEASE_VERSION  Release version (required)"
  echo "  RC_NUM           Release candidate number (for stage/promote)"
  echo "  DRY_RUN          Set to 'true' to print commands without executing"
}

case "$COMMAND" in
  stage)       cmd_stage ;;
  promote)     cmd_promote ;;
  cleanup-rcs) cmd_cleanup_rcs ;;
  cleanup-old) cmd_cleanup_old ;;
  help|--help|-h) cmd_help ;;
  *)
    echo "Unknown command: $COMMAND"
    cmd_help
    exit 1
    ;;
esac
