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
## Nexus staging repository operations for Flink releases.
## Wraps Apache Nexus 2 REST API for managing staging repositories.
##
## Usage:
##   tools $ releasing/nexus_ops.sh list
##   tools $ releasing/nexus_ops.sh close <repo-id> "<description>"
##   tools $ releasing/nexus_ops.sh release <repo-id>
##   tools $ releasing/nexus_ops.sh drop <repo-id>
##
## Environment:
##   NEXUS_USER    — Nexus username (reads from ~/.m2/settings.xml if not set)
##   NEXUS_TOKEN   — Nexus token (reads from ~/.m2/settings.xml if not set)
##   DRY_RUN=true  — print API calls without executing
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
NEXUS_BASE_URL="https://repository.apache.org/service/local"

COMMAND=${1:-help}

# Try to read credentials from ~/.m2/settings.xml if not provided
# Disable xtrace to avoid printing credentials to terminal/logs
{ set +x; } 2>/dev/null
if [ -z "${NEXUS_USER:-}" ] || [ -z "${NEXUS_TOKEN:-}" ]; then
  settings_xml="${HOME}/.m2/settings.xml"
  if [ -f "$settings_xml" ]; then
    NEXUS_USER=${NEXUS_USER:-`python3 -c "
import sys, xml.etree.ElementTree as ET
tree = ET.parse(sys.argv[1])
for server in tree.findall('.//server'):
    sid = server.find('id')
    if sid is not None and sid.text == 'apache.releases.https':
        u = server.find('username')
        if u is not None: print(u.text)
        break
" "$settings_xml" 2>/dev/null || echo ""`}
    NEXUS_TOKEN=${NEXUS_TOKEN:-`python3 -c "
import sys, xml.etree.ElementTree as ET
tree = ET.parse(sys.argv[1])
for server in tree.findall('.//server'):
    sid = server.find('id')
    if sid is not None and sid.text == 'apache.releases.https':
        p = server.find('password')
        if p is not None: print(p.text)
        break
" "$settings_xml" 2>/dev/null || echo ""`}
  fi
fi
if [ "$COMMAND" != "help" ] && [ "$COMMAND" != "--help" ] && [ "$COMMAND" != "-h" ]; then
  if [ -z "${NEXUS_USER:-}" ] || [ -z "${NEXUS_TOKEN:-}" ]; then
    echo "ERROR: Nexus credentials not found."
    echo "Set NEXUS_USER/NEXUS_TOKEN or configure apache.releases.https in ~/.m2/settings.xml"
    exit 1
  fi
fi
set -o xtrace

nexus_get() {
  local path=$1
  { set +x; } 2>/dev/null
  curl -s -f -H "Accept: application/json" \
    -u "${NEXUS_USER}:${NEXUS_TOKEN}" \
    "${NEXUS_BASE_URL}${path}"
  local rc=$?
  set -o xtrace
  return $rc
}

nexus_post_xml() {
  local path=$1
  local data=$2
  if [ "$DRY_RUN" = "true" ]; then
    echo "[dry-run] POST ${NEXUS_BASE_URL}${path}"
    echo "[dry-run] Data: $data"
    return 0
  fi
  { set +x; } 2>/dev/null
  curl -s -f -X POST \
    -H "Content-Type: application/xml" \
    -u "${NEXUS_USER}:${NEXUS_TOKEN}" \
    -d "$data" \
    "${NEXUS_BASE_URL}${path}"
  local rc=$?
  set -o xtrace
  return $rc
}

# Discover the staging profile ID for org.apache.flink
get_staging_profile_id() {
  local profiles=`nexus_get "/staging/profiles"`
  local profile_id=`echo "$profiles" | jq -r '.data[] | select(.name == "org.apache.flink") | .id'`
  if [ -z "$profile_id" ]; then
    echo "ERROR: Could not find staging profile for org.apache.flink"
    echo "Verify your Nexus credentials and that org.apache.flink is in your staging profiles."
    exit 1
  fi
  echo "$profile_id"
}

make_promote_request_xml() {
  local repo_id=$1
  local description=$2
  # Use printf to avoid XML injection from special chars in description
  local safe_desc=`echo "$description" | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g'`
  cat <<XMLEOF
<promoteRequest>
  <data>
    <stagedRepositoryId>${repo_id}</stagedRepositoryId>
    <description>${safe_desc}</description>
  </data>
</promoteRequest>
XMLEOF
}

cmd_list() {
  echo "=== Open Staging Repositories for org.apache.flink ==="
  echo ""

  local profile_id=`get_staging_profile_id`
  local result=`nexus_get "/staging/profile_repositories/${profile_id}"`

  echo "$result" | jq -r '
    .data[]
    | "\(.repositoryId)\t\(.type)\t\(.description // "no description")"
  ' | column -t -s $'\t'
}

cmd_close() {
  local repo_id=${2:-}
  local description=${3:-"Apache Flink release candidate"}

  if [ -z "$repo_id" ]; then
    echo "Usage: nexus_ops.sh close <repo-id> [description]"
    exit 1
  fi

  echo "Closing staging repository: $repo_id"
  echo "Description: $description"

  local profile_id=`get_staging_profile_id`
  local data=`make_promote_request_xml "$repo_id" "$description"`

  nexus_post_xml "/staging/profiles/${profile_id}/finish" "$data"
  echo "Close request submitted. Check status at https://repository.apache.org/#stagingRepositories"
}

cmd_release() {
  local repo_id=${2:-}

  if [ -z "$repo_id" ]; then
    echo "Usage: nexus_ops.sh release <repo-id>"
    exit 1
  fi

  echo "Releasing staging repository to Maven Central: $repo_id"
  echo ""
  echo "WARNING: This action is irreversible. Artifacts will be published to Maven Central."
  if [ "$DRY_RUN" != "true" ]; then
    read -r -p "Are you sure? [y/N] " confirm
    if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
      echo "Aborted."
      exit 0
    fi
  fi

  local profile_id=`get_staging_profile_id`
  local data=`make_promote_request_xml "$repo_id" "Release"`

  nexus_post_xml "/staging/profiles/${profile_id}/promote" "$data"
  echo "Release request submitted. Artifacts should appear in Maven Central within ~24 hours."
}

cmd_drop() {
  local repo_id=${2:-}

  if [ -z "$repo_id" ]; then
    echo "Usage: nexus_ops.sh drop <repo-id>"
    exit 1
  fi

  echo "Dropping staging repository: $repo_id"

  local profile_id=`get_staging_profile_id`
  local data=`make_promote_request_xml "$repo_id" "Dropping failed release candidate"`

  nexus_post_xml "/staging/profiles/${profile_id}/drop" "$data"
  echo "Repository $repo_id dropped."
}

cmd_help() {
  echo "Usage: releasing/nexus_ops.sh <command> [args]"
  echo ""
  echo "Commands:"
  echo "  list                           List open staging repositories"
  echo "  close <repo-id> [description]  Close a staging repository"
  echo "  release <repo-id>              Release to Maven Central (irreversible!)"
  echo "  drop <repo-id>                 Drop a staging repository"
  echo ""
  echo "Environment variables:"
  echo "  NEXUS_USER   Nexus username (auto-reads from ~/.m2/settings.xml)"
  echo "  NEXUS_TOKEN  Nexus token (auto-reads from ~/.m2/settings.xml)"
  echo "  DRY_RUN      Set to 'true' to print API calls without executing"
}

case "$COMMAND" in
  list)    cmd_list ;;
  close)   cmd_close "$@" ;;
  release) cmd_release "$@" ;;
  drop)    cmd_drop "$@" ;;
  help|--help|-h) cmd_help ;;
  *)
    echo "Unknown command: $COMMAND"
    cmd_help
    exit 1
    ;;
esac
