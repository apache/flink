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
# download miniconda.sh
sys_os=$(uname -s)
echo "Detected OS: ${sys_os}"
sys_machine=$(uname -m)
echo "Detected machine: ${sys_machine}"

if [[ ${sys_os} == "Darwin" ]]; then
    wget "https://repo.anaconda.com/miniconda/Miniconda3-py310_23.5.2-0-MacOSX-${sys_machine}.sh" -O "miniconda.sh"
elif [[ ${sys_os} == "Linux" ]]; then
    wget "https://repo.anaconda.com/miniconda/Miniconda3-py310_23.5.2-0-Linux-${sys_machine}.sh" -O "miniconda.sh"
else
    echo "Unsupported OS: ${sys_os}"
    exit 1
fi

# add the execution permission
chmod +x miniconda.sh

# create python virtual environment
./miniconda.sh -b -p venv

# activate the conda python virtual environment
source venv/bin/activate ""

# install PyFlink dependency
if [[ $1 = "" ]]; then
    # install the latest version of pyflink
    pip install apache-flink
else
    # install the specified version of pyflink
    pip install "apache-flink==$1"
fi

# deactivate the conda python virtual environment
conda deactivate

# remove the cached packages
rm -rf venv/pkgs

# package the prepared conda python virtual environment
zip -r venv.zip venv
