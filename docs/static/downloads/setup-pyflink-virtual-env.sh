#!/usr/bin/env bash
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
set -e

sys_os=$(uname -s)
echo "Detected OS: ${sys_os}"
sys_machine=$(uname -m)
echo "Detected machine: ${sys_machine}"

wget "https://github.com/astral-sh/uv/releases/download/0.5.23/uv-installer.sh" -O "uv-installer.sh"
chmod +x uv-installer.sh

UV_UNMANAGED_INSTALL="uv-bin" ./uv-installer.sh

# create python virtual environment
uv-bin/uv venv venv

# activate the python virtual environment
source venv/bin/activate ""

# install PyFlink dependency
if [[ $1 = "" ]]; then
    # install the latest version of pyflink
    pip install apache-flink
else
    # install the specified version of pyflink
    pip install "apache-flink==$1"
fi

# deactivate the python virtual environment
deactivate

# package the prepared python virtual environment
zip -r venv.zip venv
