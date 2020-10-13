#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

basedir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker_context_root=`mktemp -d 2>/dev/null || mktemp -d -t 'docker-context'`
docker_context_flink="${docker_context_root}/flink"
flink_version=1.12-SNAPSHOT
build_target=../../flink-dist/target/flink-${flink_version}-bin/flink-${flink_version}

if [ ! -d ${build_target} ]
then
    echo "Cannot find Flink binary in the workspace. Please make sure Flink project is built. "
    exit 1
fi

# Copy Flink binary into context
cp -r ${build_target} ${docker_context_flink}

# Copy Dockerfile and related files into context
cp ${basedir}/Dockerfile ${docker_context_root}
cp ${basedir}/docker-entrypoint.sh ${docker_context_root}

# Build docker image
cd ${docker_context_root}
docker build . --tag=flink:${flink_version}
