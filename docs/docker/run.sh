#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e -x -u

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

export IMAGE_NAME="flink/docs"

pushd ${SCRIPT_DIR}

docker build --rm=true -t ${IMAGE_NAME} .

popd

if [ "$(uname -s)" == "Linux" ]; then
  USER_NAME=${SUDO_USER:=$USER}
  USER_ID=$(id -u "${USER_NAME}")
  GROUP_ID=$(id -g "${USER_NAME}")
else # boot2docker uid and gid
  USER_NAME=$USER
  USER_ID=1000
  GROUP_ID=50
fi

docker build -t "${IMAGE_NAME}-${USER_NAME}" - <<UserSpecificDocker
FROM ${IMAGE_NAME}
RUN groupadd --non-unique -g ${GROUP_ID} ${USER_NAME} && \
  useradd -g ${GROUP_ID} -u ${USER_ID} -k /root -m ${USER_NAME}
ENV  HOME /home/${USER_NAME}
UserSpecificDocker

FLINK_DOC_ROOT=${SCRIPT_DIR}/..

CMD="
echo
echo 'Welcome to Apache Flink docs'
echo 'To build, execute'
echo '  ./build_docs.sh'
echo 'To watch and regenerate automatically'
echo '  ./build_docs.sh -p'
echo 'and access http://localhost:4000'
echo
bash
"

pushd ${FLINK_DOC_ROOT}

docker run -i -t \
  --rm=true \
  -w ${FLINK_DOC_ROOT} \
  -u "${USER}" \
  -v "${FLINK_DOC_ROOT}:${FLINK_DOC_ROOT}" \
  -p 4000:4000 \
  ${IMAGE_NAME}-${USER_NAME} \
  bash -c "${CMD}"

popd

