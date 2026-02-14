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
## JIRA operations for Flink releases.
## Wraps Apache JIRA REST API for common release management tasks.
##
## Usage:
##   tools $ releasing/jira_ops.sh blockers <version>
##   tools $ releasing/jira_ops.sh release-notes <version>
##   tools $ releasing/jira_ops.sh create-version <version>
##   tools $ releasing/jira_ops.sh release-version <version>
##
## Environment:
##   JIRA_USER     — JIRA username (required — even read ops are rate-limited without auth)
##   JIRA_TOKEN    — JIRA API token (required)
##   DRY_RUN=true  — print API calls without executing write operations
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
JIRA_BASE_URL="https://issues.apache.org/jira/rest/api/2"
PROJECT="FLINK"

COMMAND=${1:-help}

if [ "$COMMAND" != "help" ] && [ "$COMMAND" != "--help" ] && [ "$COMMAND" != "-h" ]; then
  if [ -z "${JIRA_USER:-}" ] || [ -z "${JIRA_TOKEN:-}" ]; then
    echo "JIRA_USER and JIRA_TOKEN must be set."
    echo "Get a token at: https://id.atlassian.com/manage/api-tokens"
    exit 1
  fi
fi

jira_get() {
  local path=$1
  curl -s -f -u "${JIRA_USER}:${JIRA_TOKEN}" "${JIRA_BASE_URL}${path}"
}

jira_post() {
  local path=$1
  local data=$2
  if [ "$DRY_RUN" = "true" ]; then
    echo "[dry-run] POST ${JIRA_BASE_URL}${path}"
    echo "[dry-run] Data: $data"
    return 0
  fi
  curl -s -f -X POST \
    -H "Content-Type: application/json" \
    -u "${JIRA_USER}:${JIRA_TOKEN}" \
    -d "$data" \
    "${JIRA_BASE_URL}${path}"
}

jira_put() {
  local path=$1
  local data=$2
  if [ "$DRY_RUN" = "true" ]; then
    echo "[dry-run] PUT ${JIRA_BASE_URL}${path}"
    echo "[dry-run] Data: $data"
    return 0
  fi
  curl -s -f -X PUT \
    -H "Content-Type: application/json" \
    -u "${JIRA_USER}:${JIRA_TOKEN}" \
    -d "$data" \
    "${JIRA_BASE_URL}${path}"
}

url_encode() {
  python3 -c "import sys, urllib.parse; print(urllib.parse.quote(sys.stdin.read().strip()))"
}

cmd_blockers() {
  local version=${2:-}
  if [ -z "$version" ]; then
    echo "Usage: jira_ops.sh blockers <version>"
    exit 1
  fi

  echo "=== Unresolved blockers for ${PROJECT} ${version} ==="
  echo ""

  local jql="project = ${PROJECT} AND fixVersion = ${version} AND resolution = Unresolved ORDER BY priority DESC"
  local encoded_jql=`echo "$jql" | url_encode`

  local result=`jira_get "/search?jql=${encoded_jql}&maxResults=200&fields=key,summary,priority,status,assignee,issuetype"`

  local total=`echo "$result" | jq '.total'`
  echo "Found $total unresolved issue(s):"
  echo ""

  echo "$result" | jq -r '.issues[] | "\(.key)\t\(.fields.priority.name)\t\(.fields.issuetype.name)\t\(.fields.assignee.displayName // "Unassigned")\t\(.fields.summary)"' | \
    column -t -s $'\t'
}

cmd_release_notes() {
  local version=${2:-}
  if [ -z "$version" ]; then
    echo "Usage: jira_ops.sh release-notes <version>"
    exit 1
  fi

  echo "=== Release Notes for ${PROJECT} ${version} ==="
  echo ""

  local jql="project = ${PROJECT} AND Release Note is not EMPTY AND fixVersion = ${version}"
  local encoded_jql=`echo "$jql" | url_encode`

  local result=`jira_get "/search?jql=${encoded_jql}&maxResults=200&fields=key,summary,issuetype,customfield_12310192"`

  local total=`echo "$result" | jq '.total'`
  echo "Found $total issue(s) with release notes:"
  echo ""

  for type in "New Feature" "Improvement" "Bug" "Task" "Sub-task"; do
    local items=`echo "$result" | jq -r ".issues[] | select(.fields.issuetype.name == \"$type\") | \"  * [\(.key)] - \(.fields.summary)\""`
    if [ -n "$items" ]; then
      echo "### $type"
      echo "$items"
      echo ""
    fi
  done
}

cmd_create_version() {
  local version=${2:-}
  if [ -z "$version" ]; then
    echo "Usage: jira_ops.sh create-version <version>"
    exit 1
  fi

  echo "Creating JIRA version: ${version}"

  local today=`date +%Y-%m-%d`
  local data=`jq -n \
    --arg name "$version" \
    --arg project "$PROJECT" \
    --arg startDate "$today" \
    '{name: $name, project: $project, startDate: $startDate}'`

  jira_post "/version" "$data"
  echo "Version ${version} created with start date ${today}."
}

cmd_release_version() {
  local version=${2:-}
  if [ -z "$version" ]; then
    echo "Usage: jira_ops.sh release-version <version>"
    exit 1
  fi

  echo "Marking JIRA version as released: ${version}"

  local version_id=`jira_get "/project/${PROJECT}/versions" | \
    jq -r ".[] | select(.name == \"$version\") | .id"`
  if [ -z "$version_id" ]; then
    echo "ERROR: Version ${version} not found in JIRA."
    exit 1
  fi

  local today=`date +%Y-%m-%d`
  local data=`jq -n \
    --arg releaseDate "$today" \
    '{released: true, releaseDate: $releaseDate}'`

  jira_put "/version/${version_id}" "$data"
  echo "Version ${version} marked as released (${today})."
}

cmd_help() {
  echo "Usage: releasing/jira_ops.sh <command> <version>"
  echo ""
  echo "Commands:"
  echo "  blockers <version>        List unresolved issues for a version"
  echo "  release-notes <version>   Extract release notes from JIRA"
  echo "  create-version <version>  Create a new JIRA version"
  echo "  release-version <version> Mark a version as released"
  echo ""
  echo "Environment variables:"
  echo "  JIRA_USER    JIRA username (required for all operations)"
  echo "  JIRA_TOKEN   JIRA API token (required for all operations)"
  echo "  DRY_RUN      Set to 'true' to print write calls without executing"
}

case "$COMMAND" in
  blockers)        cmd_blockers "$@" ;;
  release-notes)   cmd_release_notes "$@" ;;
  create-version)  cmd_create_version "$@" ;;
  release-version) cmd_release_version "$@" ;;
  help|--help|-h)  cmd_help ;;
  *)
    echo "Unknown command: $COMMAND"
    cmd_help
    exit 1
    ;;
esac
