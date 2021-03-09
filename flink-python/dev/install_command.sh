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
if [[ "$@" =~ 'apache-flink-libraries' ]]; then
    pushd apache-flink-libraries
    python setup.py sdist
    pushd dist
    python -m pip install *
    popd
    popd
fi
retry_times=3
install_command="python -m pip install $@"
${install_command}
status=$?
while [[ ${status} -ne 0 ]] && [[ ${retry_times} -gt 0 ]]; do
    retry_times=$((retry_times-1))
    # sleep 3 seconds and then reinstall.
    sleep 3
    ${install_command}
    status=$?
done

exit ${status}
