#!/bin/sh

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

usage() {
  cat <<HERE
Usage:
  build.sh --from-local-dist [--image-name <image>]
  build.sh --from-release --flink-version <x.x.x> --hadoop-version <x.x> --scala-version <x.xx> [--image-name <image>]
  build.sh --help

  If the --image-name flag is not used the built image name will be 'flink'.
HERE
  exit 1
}

while [[ $# -ge 1 ]]
do
key="$1"
  case $key in
    --from-local-dist)
    FROM_LOCAL="true"
    ;;
    --from-release)
    FROM_RELEASE="true"
    ;;
    --image-name)
    IMAGE_NAME="$2"
    shift
    ;;
    --flink-version)
    FLINK_VERSION="$2"
    shift
    ;;
    --hadoop-version)
    HADOOP_VERSION="$(echo "$2" | sed 's/\.//')"
    shift
    ;;
    --scala-version)
    SCALA_VERSION="$2"
    shift
    ;;
    --help)
    usage
    ;;
    *)
    # unknown option
    ;;
  esac
  shift
done

IMAGE_NAME=${IMAGE_NAME:-flink}

TMPDIR=_TMP_
mkdir -p "${TMPDIR}"

if [ -n "${FROM_RELEASE}" ]; then

  [[ -n "${FLINK_VERSION}" ]] && [[ -n "${HADOOP_VERSION}" ]] && [[ -n "${SCALA_VERSION}" ]] || usage

  FLINK_BASE_URL="$(curl -s https://www.apache.org/dyn/closer.cgi\?preferred\=true)flink/flink-${FLINK_VERSION}/"
  FLINK_DIST_FILE_NAME="flink-${FLINK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala_${SCALA_VERSION}.tgz"
  CURL_OUTPUT="${TMPDIR}/${FLINK_DIST_FILE_NAME}"

  echo "Downloading ${FLINK_DIST_FILE_NAME} from ${FLINK_BASE_URL}"
  curl -s ${FLINK_BASE_URL}${FLINK_DIST_FILE_NAME} --output ${CURL_OUTPUT}

  FLINK_DIST="${CURL_OUTPUT}"

elif [ -n "${FROM_LOCAL}" ]; then

    DIST_DIR="../../flink-dist/target/flink-*-bin"
    FLINK_DIST="${TMPDIR}/flink.tgz"
    echo "Using flink dist: ${DIST_DIR}"
    tar -C ${DIST_DIR} -cvzf "${FLINK_DIST}" .
else
  usage
fi

docker build --build-arg flink_dist="${FLINK_DIST}" -t "${IMAGE_NAME}" .

rm -rf "${TMPDIR}"
