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
## Email template generator for Flink releases.
## Generates pre-filled email templates for vote, result, and announcement.
##
## Usage:
##   tools $ releasing/generate_email.sh vote <version> <rc_num> [gpg_fingerprint]
##   tools $ releasing/generate_email.sh vote-result <version> <rc_num>
##   tools $ releasing/generate_email.sh announce <version>
##
## Output goes to stdout. Redirect to a file if desired:
##   tools $ releasing/generate_email.sh vote 1.20.0 1 > /tmp/vote-email.txt
##

# Note: xtrace intentionally omitted — output is the email template itself,
# interleaving command traces would corrupt it.
set -o errexit
set -o nounset
set -o pipefail

CURR_DIR=`pwd`
if [[ `basename $CURR_DIR` != "tools" ]] ; then
  echo "You have to call the script from the tools/ dir"
  exit 1
fi

COMMAND=${1:-help}

cmd_vote() {
  local version=${2:-}
  local rc_num=${3:-}
  local gpg_fingerprint=${4:-FFFFFFFF}

  if [ -z "$version" ] || [ -z "$rc_num" ]; then
    echo "Usage: generate_email.sh vote <version> <rc_num> [gpg_fingerprint]" >&2
    exit 1
  fi

  local tag="release-${version}-rc${rc_num}"

  cat <<EOF
To: dev@flink.apache.org
Subject: [VOTE] Release ${version}, release candidate #${rc_num}

Hi everyone,
Please review and vote on the release candidate #${rc_num} for version ${version}, as follows:

[ ] +1, Approve the release
[ ] -1, Do not approve the release (please provide specific comments)

The complete staging area is available for your review, which includes:
* JIRA release notes [1],
* the official Apache source and binary convenience releases to be deployed to dist.apache.org [2], which are signed with the key with fingerprint ${gpg_fingerprint} [3],
* all artifacts to be deployed to the Maven Central Repository [4],
* source code tag "${tag}" [5],
* website pull request listing the new release [6].

The vote will be open for at least 72 hours. It is adopted by majority approval, with at least 3 PMC affirmative votes.

Thanks,
<Release Manager Name>

[1] https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315522&version=<JIRA_VERSION_ID>
[2] https://dist.apache.org/repos/dist/dev/flink/flink-${version}-rc${rc_num}
[3] https://dist.apache.org/repos/dist/release/flink/KEYS
[4] https://repository.apache.org/content/repositories/<STAGING_REPO_ID>
[5] https://github.com/apache/flink/releases/tag/${tag}
[6] <WEBSITE_PR_URL>
EOF
}

cmd_vote_result() {
  local version=${2:-}
  local rc_num=${3:-}

  if [ -z "$version" ] || [ -z "$rc_num" ]; then
    echo "Usage: generate_email.sh vote-result <version> <rc_num>" >&2
    exit 1
  fi

  cat <<EOF
To: dev@flink.apache.org
Subject: [RESULT] [VOTE] Release ${version}, release candidate #${rc_num}

Hi everyone,

I'm happy to announce that we have unanimously approved this release.

There are XXX approving votes, XXX of which are binding:
* <approver 1>
* <approver 2>
* <approver 3>

There are no disapproving votes.

Thanks everyone who has tested the release candidate and given their feedback.

<Release Manager Name>
EOF
}

cmd_announce() {
  local version=${2:-}

  if [ -z "$version" ]; then
    echo "Usage: generate_email.sh announce <version>" >&2
    exit 1
  fi

  local short_version=`echo "$version" | sed 's/\.[^.]*$//'`
  local patch=`echo "$version" | awk -F. '{print $3}'`

  local release_desc
  if [ "$patch" = "0" ]; then
    release_desc="the first release of the ${short_version} series"
  else
    release_desc="a bugfix release of the ${short_version} series"
  fi

  cat <<EOF
To: dev@flink.apache.org, user@flink.apache.org, user-zh@flink.apache.org, announce@apache.org
Subject: [ANNOUNCE] Apache Flink ${version} released

The Apache Flink community is very happy to announce the release of Apache Flink ${version}, which is ${release_desc}.

Apache Flink® is an open-source stream processing framework for distributed, high-performing, always-available, and accurate data streaming applications.

The release is available for download at:
https://flink.apache.org/downloads.html

The full release notes are available in Jira:
https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315522&version=<JIRA_VERSION_ID>

We would like to thank all contributors of the Apache Flink community who made this release possible!

Feel free to reach out to the release manager or the dev@flink.apache.org mailing list if you have any questions.

Regards,
<Release Manager Name>
EOF
}

cmd_help() {
  echo "Usage: releasing/generate_email.sh <command> <version> [args]"
  echo ""
  echo "Commands:"
  echo "  vote <version> <rc_num> [gpg_fingerprint]   Generate vote email"
  echo "  vote-result <version> <rc_num>               Generate vote result email"
  echo "  announce <version>                            Generate announcement email"
  echo ""
  echo "Output goes to stdout. Redirect to a file:"
  echo "  releasing/generate_email.sh vote 1.20.0 1 > /tmp/vote.txt"
  echo ""
  echo "Replace placeholders (<...>) with actual values before sending."
}

case "$COMMAND" in
  vote)        cmd_vote "$@" ;;
  vote-result) cmd_vote_result "$@" ;;
  announce)    cmd_announce "$@" ;;
  help|--help|-h) cmd_help ;;
  *)
    echo "Unknown command: $COMMAND"
    cmd_help
    exit 1
    ;;
esac
