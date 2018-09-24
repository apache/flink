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

# Hex-encoded travis-interval ANSI escape sequences
# https://github.com/travis-ci/travis-build/blob/master/lib/travis/build/bash/travis_fold.bash
# https://github.com/travis-ci/travis-build/blob/master/lib/travis/build/bash/travis_setup_env.bash
#
# \x1b = \033 = ESC
# \x5b = [
# \x4b = K
# \x6d = m
# \x30 = 0
# \x31 = 1
# \x33 = 3
# \x3b = ;

COLOR_YELLOW="\x1b\x5b\x33\x33\x3b\x31\x6d"
ANSI_CLEAR="\x1b\x5b\x30\x6d"

function start_fold {
    local id=$1
    local message=$2
    echo -e "travis_fold:start:${id}\\r${ANSI_CLEAR}${COLOR_YELLOW}${message}${ANSI_CLEAR}"
}

function end_fold {
    local message=$1
	echo -en "travis_fold:end:${message}\\r${ANSI_CLEAR}"
}
