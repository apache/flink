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
# Globals:
# TRAVIS_PULL_REQUEST
#   - Travis pull request number if current Travis job is triggered by a pull request, “false” otherwise.
#     See: https://docs.travis-ci.com/user/environment-variables/#default-environment-variables
# TRAVIS_COMMIT_RANGE
#   - Travis range of commits that were included in the push or pull request.
#     See: https://docs.travis-ci.com/user/environment-variables/#default-environment-variables
# PROFILE
#   - mvn "profile" parameters defined by invoking scripts
#

DIFF_LIST_FILEPATH=diff_files.txt
MVN_DEPENDENCIES_TREE_FILEPATH=mvn-dependencies-tree.txt

function is_pr_build() {
    # For non-PR runs, we want to run all tests.
    # Also, we can't tell what has been changed if we don't know git diff range ($TRAVIS_COMMIT_RANGE).
    if [ "$TRAVIS_PULL_REQUEST" == "false" ] || [ "x$TRAVIS_COMMIT_RANGE" == "x" ]; then
        return 1
    fi
    return 0
}

function prepare_dependencies_tree_if_pr_build() {
    # This is a helper, preparation step to enable skipping running some stages' tests.
    # If we know that the change doesn't affect some tests
    if ! is_pr_build ; then
        echo "Skipping Travis job optimization for non pull request build."
        return
    fi
    echo "git diff range: $TRAVIS_COMMIT_RANGE"
    git diff --name-only "$TRAVIS_COMMIT_RANGE" > "$DIFF_LIST_FILEPATH"

    mvn dependency:list -DincludeGroupIds=org.apache.flink $PROFILE > mvn-dependencies-list.txt
    mvn exec:exec -Dexec.executable="echo" -Dexec.args="[INFO] Basedir: \${project.basedir}" $PROFILE > mvn-basedir-list.txt

    python3 ./tools/travis/mvn_diff_test.py process-mvn-dependencies mvn-dependencies-list.txt mvn-basedir-list.txt > "$MVN_DEPENDENCIES_TREE_FILEPATH"
}

function can_skip_mvn_test_run_for_stage() {
    local stage=$1

    if ! is_pr_build ; then
        echo "Cannot skip non PR test run"
        return 1
    fi

    python3 ./tools/travis/mvn_diff_test.py can-skip-mvn-test "$stage" "$PROFILE" "$MVN_DEPENDENCIES_TREE_FILEPATH" "$DIFF_LIST_FILEPATH"
    return
}
