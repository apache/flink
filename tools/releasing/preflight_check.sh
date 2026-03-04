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
## Pre-flight checks for Flink release prerequisites.
## Verifies that all tools, credentials, and configurations are in place
## before starting the release process.
##
## Usage: tools $ releasing/preflight_check.sh [java_major_version]
## Example: tools $ releasing/preflight_check.sh 11
##

# Note: xtrace intentionally omitted for this script — output is a
# formatted report, not a command trace.
set -o errexit
set -o nounset
set -o pipefail

CURR_DIR=`pwd`
if [[ `basename $CURR_DIR` != "tools" ]] ; then
  echo "You have to call the script from the tools/ dir"
  exit 1
fi

EXPECTED_JAVA_MAJOR=${1:-11}
PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0

pass() { echo "  [PASS] $1"; PASS_COUNT=$((PASS_COUNT + 1)); }
fail() { echo "  [FAIL] $1"; FAIL_COUNT=$((FAIL_COUNT + 1)); }
warn() { echo "  [WARN] $1"; WARN_COUNT=$((WARN_COUNT + 1)); }

echo ""
echo "=== Flink Release Pre-flight Checks ==="
echo ""

# 1. GPG key
echo "GPG Configuration:"
if gpg_keys=`gpg --list-secret-keys --keyid-format SHORT 2>/dev/null` && [ -n "$gpg_keys" ]; then
  key_id=`echo "$gpg_keys" | grep -m1 'sec' | sed 's/.*\/\([A-Fa-f0-9]\{8,\}\).*/\1/'`
  if [ -z "$key_id" ]; then
    warn "GPG secret key exists but could not extract key ID from output"
  else
    pass "GPG secret key found: $key_id"

    # Check if key is in dist.apache.org KEYS file
    keys_content=`curl -s https://dist.apache.org/repos/dist/release/flink/KEYS 2>/dev/null || echo ""`
    if [ -n "$keys_content" ] && echo "$keys_content" | grep -qi "$key_id"; then
      pass "GPG key $key_id found in dist.apache.org KEYS file"
    else
      fail "GPG key $key_id NOT found in dist.apache.org KEYS file — publish it before releasing"
    fi
  fi
else
  fail "No GPG secret key found — run 'gpg --gen-key' to create one"
fi

# 2. Git signing key
if git_signing_key=`git config --global user.signingkey 2>/dev/null` && [ -n "$git_signing_key" ]; then
  pass "Git signing key configured: $git_signing_key"
else
  fail "Git signing key not configured — run 'git config --global user.signingkey <KEY_ID>'"
fi

echo ""
echo "Maven & Nexus Configuration:"

# 3. Nexus tokens in settings.xml
settings_xml="${HOME}/.m2/settings.xml"
if [ -f "$settings_xml" ]; then
  if grep -q "apache.releases.https" "$settings_xml"; then
    pass "Nexus release token found in $settings_xml"
  else
    fail "apache.releases.https not found in $settings_xml — configure Nexus user token"
  fi
  if grep -q "apache.snapshots.https" "$settings_xml"; then
    pass "Nexus snapshot token found in $settings_xml"
  else
    fail "apache.snapshots.https not found in $settings_xml — configure Nexus user token"
  fi
else
  fail "$settings_xml not found — create it with Nexus user tokens"
fi

echo ""
echo "Build Tools:"

# 4. Java version
if java_version=`java -version 2>&1 | head -1 | sed 's/.*"\(.*\)".*/\1/'`; then
  java_major=`echo "$java_version" | cut -d. -f1`
  if [ "$java_major" -ge "$EXPECTED_JAVA_MAJOR" ]; then
    pass "Java version: $java_version (required: ${EXPECTED_JAVA_MAJOR}+)"
  else
    fail "Java version: $java_version (required: ${EXPECTED_JAVA_MAJOR}+)"
  fi
else
  fail "Java not found"
fi

# 5. Maven version
if mvn_version=`mvn --version 2>/dev/null | head -1 | sed 's/.*Maven \([0-9.]*\).*/\1/'`; then
  pass "Maven version: $mvn_version"
else
  fail "Maven not found"
fi

# 6. GNU tar (macOS check)
if [ "`uname`" == "Darwin" ]; then
  if tar_version=`tar --version 2>/dev/null | head -1` && echo "$tar_version" | grep -q "GNU"; then
    pass "GNU tar available: $tar_version"
  else
    fail "GNU tar not found — run 'brew install gnu-tar && ln -s /opt/homebrew/bin/gtar /usr/local/bin/tar'"
  fi
else
  pass "tar: `tar --version 2>/dev/null | head -1`"
fi

echo ""
echo "CLI Tools:"

# 7. Required CLIs
for tool in svn jq curl python3; do
  if command -v "$tool" &> /dev/null; then
    pass "$tool available"
  else
    fail "$tool not found"
  fi
done

if command -v gh &> /dev/null; then
  if gh auth status &>/dev/null; then
    pass "gh CLI authenticated"
  else
    warn "gh CLI installed but not authenticated — run 'gh auth login'"
  fi
else
  fail "gh CLI not found — install with: brew install gh"
fi

echo ""
echo "PyPI (optional — needed for Phase 6):"

# 8. PyPI credentials
if [ -n "${TWINE_USERNAME:-}" ] && [ -n "${TWINE_PASSWORD:-}" ]; then
  pass "PyPI credentials found (TWINE_USERNAME / TWINE_PASSWORD)"
else
  warn "PyPI credentials not in environment — set TWINE_USERNAME and TWINE_PASSWORD before Phase 6"
fi

if command -v twine &> /dev/null; then
  pass "twine available"
else
  warn "twine not found — needed for Phase 6 (pip install twine)"
fi

echo ""
echo "=== Results ==="
echo "  ${PASS_COUNT} passed, ${FAIL_COUNT} failed, ${WARN_COUNT} warnings"
echo ""

if [ "$FAIL_COUNT" -gt 0 ]; then
  echo "Fix the failures above before starting the release."
  exit 1
else
  echo "All critical checks passed. Ready to release."
  exit 0
fi
