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
  build.sh --from-release --flink-version <x.x.x> --scala-version <x.xx> --hadoop-version <x.x> [--image-name <image>]
  build.sh --help

  If the --image-name flag is not used the built image name will be 'flink'.
  Before Flink-1.8, the hadoop-version is required. And from Flink-1.8, the hadoop-version is optional and would download pre-bundled shaded Hadoop jar package if provided.
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
    HADOOP_VERSION="$2"
    HADOOP_MAJOR_VERSION="$(echo ${HADOOP_VERSION} | sed 's/\.//')"
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

# TMPDIR must be contained within the working directory so it is part of the
# Docker context. (i.e. it can't be mktemp'd in /tmp)
TMPDIR=_TMP_

cleanup() {
    rm -rf "${TMPDIR}"
}
trap cleanup EXIT

mkdir -p "${TMPDIR}"

checkUrlAvailable() {
    curl --output /dev/null --silent --head --fail $1
    ret=$?
    if [[ ${ret} -ne 0 ]]; then
        echo "The url $1 not available, please check your parameters, exit..."
        usage
        exit 2
    fi
}

if [ -n "${FROM_RELEASE}" ]; then

  [[ -n "${FLINK_VERSION}" ]] && [[ -n "${SCALA_VERSION}" ]] || usage

  FLINK_BASE_URL="$(curl -s https://www.apache.org/dyn/closer.cgi\?preferred\=true)flink/flink-${FLINK_VERSION}/"

  FLINK_MAJOR_VERSION=$(echo "$FLINK_VERSION" | sed -e 's/\.//;s/\(..\).*/\1/')

  if [[ $FLINK_MAJOR_VERSION -ge 18 ]]; then

  # After Flink-1.8 we would let release pre-built package with hadoop
    if [[ -n "${HADOOP_VERSION}" ]]; then
        echo "After Flink-1.8, we would download pre-bundle hadoop jar package."
        # list to get target pre-bundle package
        SHADED_HADOOP_BASE_URL="https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop2-uber/"
        SHADED_HADOOP_VERSION="$(curl -s ${SHADED_HADOOP_BASE_URL} | grep -o "title=\"[0-9.-]*/\"" | sed 's/title=\"//g; s/\/"//g' | grep ${HADOOP_VERSION} | head -1)"
        SHADED_HADOOP_FILE_NAME="flink-shaded-hadoop2-uber-${SHADED_HADOOP_VERSION}.jar"

        CURL_OUTPUT_SHADED_HADOOP="${TMPDIR}/${SHADED_HADOOP_FILE_NAME}"

        DOWNLOAD_SHADED_HADOOP_URL=${SHADED_HADOOP_BASE_URL}${SHADED_HADOOP_VERSION}/${SHADED_HADOOP_FILE_NAME}
        checkUrlAvailable ${DOWNLOAD_SHADED_HADOOP_URL}

        echo "Downloading ${SHADED_HADOOP_FILE_NAME} from ${DOWNLOAD_SHADED_HADOOP_URL}"

        curl -# ${DOWNLOAD_SHADED_HADOOP_URL} --output ${CURL_OUTPUT_SHADED_HADOOP}
        SHADED_HADOOP="${CURL_OUTPUT_SHADED_HADOOP}"
    fi
    FLINK_DIST_FILE_NAME="flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz"
  elif [[ -z "${HADOOP_VERSION}" ]]; then
    usage
  else
    FLINK_DIST_FILE_NAME="flink-${FLINK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}-scala_${SCALA_VERSION}.tgz"
  fi


  CURL_OUTPUT="${TMPDIR}/${FLINK_DIST_FILE_NAME}"

  DOWNLOAD_FLINK_URL=${FLINK_BASE_URL}${FLINK_DIST_FILE_NAME}
  checkUrlAvailable ${DOWNLOAD_FLINK_URL}

  echo "Downloading ${FLINK_DIST_FILE_NAME} from ${DOWNLOAD_FLINK_URL}"

  curl -# ${DOWNLOAD_FLINK_URL} --output ${CURL_OUTPUT}

  FLINK_DIST="${CURL_OUTPUT}"

elif [ -n "${FROM_LOCAL}" ]; then

  DIST_DIR="../../flink-dist/target/flink-*-bin"
  FLINK_DIST="${TMPDIR}/flink.tgz"
  echo "Using flink dist: ${DIST_DIR}"
  tar -C ${DIST_DIR} -cvzf "${FLINK_DIST}" .

else

  usage

fi

docker build --build-arg flink_dist="${FLINK_DIST}" --build-arg hadoop_jar="${SHADED_HADOOP}" -t "${IMAGE_NAME}" .
