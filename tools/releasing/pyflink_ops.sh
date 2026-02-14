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
## PyFlink wheel build and download operations.
## Automates GitHub Actions trigger, wait, download, and organization
## of wheel packages for a release.
##
## Usage:
##   tools $ releasing/pyflink_ops.sh trigger <owner/repo> <branch>
##   tools $ releasing/pyflink_ops.sh wait <owner/repo> <run_id>
##   tools $ releasing/pyflink_ops.sh download <owner/repo> <run_id>
##
## Prerequisites: gh CLI must be installed and authenticated.
##
## Note: PyPI storage quota must be checked manually at:
##   https://pypi.org/manage/project/apache-flink/settings/
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

COMMAND=${1:-help}

FLINK_DIR=`cd .. && pwd`
PYFLINK_DIST="${FLINK_DIR}/flink-python/dist"

cmd_trigger() {
  local repo=${2:-}
  local branch=${3:-}

  if [ -z "$repo" ] || [ -z "$branch" ]; then
    echo "Usage: pyflink_ops.sh trigger <owner/repo> <branch>"
    echo "Example: pyflink_ops.sh trigger myuser/flink release-1.20.0-rc1"
    exit 1
  fi

  echo "Triggering Nightly (beta) workflow on ${repo}@${branch}..."

  if [ "$DRY_RUN" = "true" ]; then
    echo "[dry-run] gh workflow run \"Nightly (beta)\" --repo $repo --ref $branch"
    return 0
  fi

  gh workflow run "Nightly (beta)" --repo "$repo" --ref "$branch"

  echo "Workflow triggered. Waiting 10 seconds for it to appear..."
  sleep 10

  local run_id=`gh run list --repo "$repo" --workflow="Nightly (beta)" --branch "$branch" --limit 1 --json databaseId --jq '.[0].databaseId'`

  echo "Run ID: ${run_id}"
  echo ""
  echo "To wait for completion: releasing/pyflink_ops.sh wait ${repo} ${run_id}"
  echo "To download artifacts:  releasing/pyflink_ops.sh download ${repo} ${run_id}"
}

cmd_wait() {
  local repo=${2:-}
  local run_id=${3:-}

  if [ -z "$repo" ] || [ -z "$run_id" ]; then
    echo "Usage: pyflink_ops.sh wait <owner/repo> <run_id>"
    exit 1
  fi

  echo "Waiting for workflow run ${run_id} to complete..."
  echo "(This may take a while — wheel builds typically take 1-2 hours)"
  echo ""

  gh run watch "$run_id" --repo "$repo" --exit-status

  echo ""
  echo "Workflow complete. Download artifacts with:"
  echo "  releasing/pyflink_ops.sh download ${repo} ${run_id}"
}

cmd_download() {
  local repo=${2:-}
  local run_id=${3:-}

  if [ -z "$repo" ] || [ -z "$run_id" ]; then
    echo "Usage: pyflink_ops.sh download <owner/repo> <run_id>"
    exit 1
  fi

  echo "Downloading wheel artifacts from run ${run_id}..."

  local download_dir=`mktemp -d`

  gh run download "$run_id" --repo "$repo" --dir "$download_dir"

  echo "Artifacts downloaded to: $download_dir"
  echo ""

  mkdir -p "$PYFLINK_DIST"

  # Move wheel files to flink-python/dist/
  echo "Organizing wheel files into ${PYFLINK_DIST}..."
  find "$download_dir" -name "*.whl" -exec mv {} "$PYFLINK_DIST/" \;
  local count=`find "$PYFLINK_DIST" -name "*.whl" | wc -l | tr -d ' '`

  echo ""
  echo "Done. ${count} wheel file(s) in ${PYFLINK_DIST}:"
  ls -la "$PYFLINK_DIST/"*.whl 2>/dev/null || echo "  (none found)"

  rm -rf "$download_dir"
}

cmd_help() {
  echo "Usage: releasing/pyflink_ops.sh <command> [args]"
  echo ""
  echo "Commands:"
  echo "  trigger <owner/repo> <branch>    Trigger wheel build workflow"
  echo "  wait <owner/repo> <run_id>       Wait for workflow to complete"
  echo "  download <owner/repo> <run_id>   Download wheel artifacts"
  echo ""
  echo "Example workflow:"
  echo "  releasing/pyflink_ops.sh trigger myuser/flink release-1.20.0-rc1"
  echo "  releasing/pyflink_ops.sh wait myuser/flink 12345678"
  echo "  releasing/pyflink_ops.sh download myuser/flink 12345678"
  echo ""
  echo "Note: PyPI storage quota must be checked manually at:"
  echo "  https://pypi.org/manage/project/apache-flink/settings/"
}

case "$COMMAND" in
  trigger)        cmd_trigger "$@" ;;
  wait)           cmd_wait "$@" ;;
  download)       cmd_download "$@" ;;
  help|--help|-h) cmd_help ;;
  *)
    echo "Unknown command: $COMMAND"
    cmd_help
    exit 1
    ;;
esac
