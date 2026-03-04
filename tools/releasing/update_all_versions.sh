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
## Cross-repo version update operations for major Flink releases.
## Handles version-related updates beyond what update_branch_version.sh covers.
##
## Usage:
##   tools $ RELEASE_VERSION=1.20.0 releasing/update_all_versions.sh add-flink-version
##   tools $ RELEASE_VERSION=1.20.0 releasing/update_all_versions.sh update-nightly-workflow
##   tools $ RELEASE_VERSION=1.20.0 releasing/update_all_versions.sh show-cross-repo-tasks
##
## These operations are only needed for major releases (x.y.0).
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

cd ..
FLINK_DIR=`pwd`

cmd_add_flink_version() {
  if [ -z "${RELEASE_VERSION:-}" ]; then echo "RELEASE_VERSION not set."; exit 1; fi

  local MAJOR=`echo "$RELEASE_VERSION" | cut -d. -f1`
  local MINOR=`echo "$RELEASE_VERSION" | cut -d. -f2`

  echo "=== Adding FlinkVersion enum entry ==="

  local version_file=`find "$FLINK_DIR" -path "*/flink-annotations/src/main/java/org/apache/flink/FlinkVersion.java" -type f`

  if [ -z "$version_file" ]; then
    echo "ERROR: FlinkVersion.java not found"
    exit 1
  fi

  echo "File: $version_file"

  local version_enum="v${MAJOR}_${MINOR}"
  if grep -q "$version_enum" "$version_file"; then
    echo "Version ${version_enum} already exists in FlinkVersion.java"
    return 0
  fi

  local version_string="${MAJOR}.${MINOR}"

  if [ "$DRY_RUN" = "true" ]; then
    echo "[dry-run] Would add: ${version_enum}(\"${version_string}\");"
    echo "[dry-run] to: $version_file"
    return 0
  fi

  # Find the last version entry line number
  local last_version_line
  last_version_line=`grep -n 'v[0-9]*_[0-9]*("' "$version_file" | tail -1 | cut -d: -f1`

  if [ -z "$last_version_line" ]; then
    echo "ERROR: Could not find existing version entries in FlinkVersion.java"
    exit 1
  fi

  # Get indent from existing line
  local last_line_content
  last_line_content=`sed -n "${last_version_line}p" "$version_file"`
  local indent
  indent=`echo "$last_line_content" | sed 's/[^ ].*//'`

  # Replace trailing semicolon with comma on last entry using perl (macOS-safe)
  perl -pi -e "s/;$/,/ if \$. == ${last_version_line}" "$version_file"

  # Insert new version AFTER last entry by appending to the line's output
  perl -pi -e "\$_ .= \"${indent}${version_enum}(\\\"${version_string}\\\");\n\" if \$. == ${last_version_line}" "$version_file"

  echo "Added ${version_enum}(\"${version_string}\") to FlinkVersion.java"
}

cmd_update_nightly_workflow() {
  if [ -z "${RELEASE_VERSION:-}" ]; then echo "RELEASE_VERSION not set."; exit 1; fi

  local MAJOR=`echo "$RELEASE_VERSION" | cut -d. -f1`
  local MINOR=`echo "$RELEASE_VERSION" | cut -d. -f2`

  echo "=== Updating GitHub Actions nightly workflow ==="

  local workflow_file="$FLINK_DIR/.github/workflows/nightly-trigger.yml"

  if [ ! -f "$workflow_file" ]; then
    echo "WARNING: $workflow_file not found. Skipping."
    return 0
  fi

  echo "File: $workflow_file"
  echo ""
  echo "The nightly workflow should include the two most recent release branches and master."
  echo "Please manually verify the branch list in:"
  echo "  $workflow_file"
  echo ""
  echo "Expected branches: master, release-${MAJOR}.${MINOR}, release-${MAJOR}.$((MINOR - 1))"
}

cmd_show_cross_repo_tasks() {
  if [ -z "${RELEASE_VERSION:-}" ]; then echo "RELEASE_VERSION not set."; exit 1; fi

  local MAJOR=`echo "$RELEASE_VERSION" | cut -d. -f1`
  local MINOR=`echo "$RELEASE_VERSION" | cut -d. -f2`
  local PATCH=`echo "$RELEASE_VERSION" | cut -d. -f3`

  echo "=== Cross-Repository Tasks for Flink ${RELEASE_VERSION} ==="
  echo ""
  echo "The following updates must be made in OTHER repositories."
  echo ""

  echo "1. flink-docker repository:"
  echo "   git clone https://github.com/apache/flink-docker"
  echo "   git checkout dev-master"
  echo "   git checkout -b dev-${MAJOR}.${MINOR}"
  echo "   # Update .github/workflows/ci.yml to point to ${MAJOR}.${MINOR}-SNAPSHOT"
  echo "   # On dev-master: update to next SNAPSHOT version"
  echo ""

  echo "2. flink-benchmarks repository:"
  echo "   git clone https://github.com/apache/flink-benchmarks"
  echo "   git checkout master"
  echo "   git checkout -b dev-${MAJOR}.${MINOR}"
  echo "   # On master: update pom.xml flink.version to next SNAPSHOT"
  echo ""

  echo "3. flink-web (website) repository:"
  echo "   # Update docs/data/flink.yml"
  echo "   # Update docs/data/release_archive.yml"
  echo "   # Add blog post to _posts/"
  echo "   # (Major) Update FlinkStableVersion in docs/config.toml"
  echo ""

  if [ "$PATCH" = "0" ]; then
    echo "NOTE: This is a MAJOR release — all 3 repos need updates."
  else
    echo "NOTE: This is a BUGFIX release — only the website repo needs updates."
  fi
}

cmd_help() {
  echo "Usage: RELEASE_VERSION=x.y.z releasing/update_all_versions.sh <command>"
  echo ""
  echo "Commands:"
  echo "  add-flink-version         Add entry to FlinkVersion enum (major only)"
  echo "  update-nightly-workflow   Show nightly workflow update guidance"
  echo "  show-cross-repo-tasks    Show tasks needed in other repositories"
  echo ""
  echo "For POM/docs/pyflink version updates, use the existing scripts:"
  echo "  update_branch_version.sh  — update version on current branch"
  echo "  create_snapshot_branch.sh — create snapshot branch for major release"
}

case "$COMMAND" in
  add-flink-version)       cmd_add_flink_version ;;
  update-nightly-workflow)  cmd_update_nightly_workflow ;;
  show-cross-repo-tasks)   cmd_show_cross_repo_tasks ;;
  help|--help|-h)          cmd_help ;;
  *)
    echo "Unknown command: $COMMAND"
    cmd_help
    exit 1
    ;;
esac
