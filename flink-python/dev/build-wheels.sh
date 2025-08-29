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
set -e -x

## 1. Setup virtual environment with uv
dev/lint-python.sh -s basic

python_versions=("3.9" "3.10" "3.11" "3.12")

if [[ "$(uname)" != "Darwin" ]]; then
    # force the linker to use the older glibc version in Linux
    export CFLAGS="-I. -include dev/glibc_version_fix.h"
fi

source "$(pwd)"/dev/.uv/bin/activate

## 2. Build wheels for each Python version
for ((i=0;i<${#python_versions[@]};i++)) do
    echo "Building wheel for Python: ${python_versions[i]}"
    uv build --wheel --python "${python_versions[i]}"
done

## 3. Convert linux_x86_64 wheel to manylinux1 wheel in Linux
if [[ "$(uname)" != "Darwin" ]]; then
    echo "Converting linux_x86_64 wheel to manylinux1"
    for wheel_file in dist/*.whl; do
        uv run --group auditwheel auditwheel repair "${wheel_file}" -w dist
        rm -f "${wheel_file}"
    done
fi

deactivate

## 4. Output the result
echo "Build finished - created the following wheels:"
ls -al dist/
