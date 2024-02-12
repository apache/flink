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

# fail early in case of an error
set -e

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

source "${SCRIPT_DIR}/docs_utils.sh"

# configurable variables
HUGO_VERSION="0.110.0"

# script-internal variables
HUGO_COMMAND="hugo"
SOURCE_DIR="${SCRIPT_DIR}"

function print_usage_and_exit() {
  EXIT_CODE=0
  if [[ "$1" != "" ]]; then
    echo "[ERROR] $1"
    echo ""
    EXIT_CODE=1
  fi

  echo "Usage: (serve|build|hugo-version|help) [<other-parameters>]"
  echo "   hugo-version"
  echo "         Prints Hugo version that's used by this script (${HUGO_VERSION})."
  echo "   serve"
  echo "         Serves the docs from ${SOURCE_DIR} using local webserver."
  echo "   build"
  echo "         Builds the docs (${SOURCE_DIR}) into <target-dir> (--target is required!)."
  echo "   help"
  echo "         Prints this usage."
  echo ""
  echo "   Other parameters:"
  echo "         --docker runs the Hugo command in a Docker container."
  echo "         --skip-integrate-connector-docs makes the connector docs pulling being skipped."
  echo "         --target <target-dir> sets the target folder for the build command."
  exit "${EXIT_CODE}"
}

SCRIPT_COMMAND="$1"
[[ "$#" -gt 0 ]] && shift 1

# parse generic parameters
TARGET_DIR="target"
while [ "$#" -gt 0 ]; do
  case "$1" in
    "--docker")
      IN_DOCKER="true"
      shift 1
      ;;
    "--skip-integrate-connector-docs")
      SKIP_CONNECTOR_DOCS="true"
      shift 1
      ;;
    "--target")
      TARGET_DIR="$2"
      shift 2
      ;;
    *)
      print_usage_and_exit "Invalid parameter: $1"
  esac
done

# process parameters
if [[ "${IN_DOCKER}" == "true" ]]; then
  if [[ -n "${TARGET_DIR+x}" ]]; then
    IN_CONTAINER_TARGET_DIR="/target"
    ADDITIONAL_DOCKER_PARAMS="-v ${TARGET_DIR}:${IN_CONTAINER_TARGET_DIR}"
    TARGET_DIR="${IN_CONTAINER_TARGET_DIR}"
  fi

  SOURCE_DIR="/src"
  HUGO_COMMAND="docker run -v ${SCRIPT_DIR}:${SOURCE_DIR} ${ADDITIONAL_DOCKER_PARAMS} -p 1313:1313 jakejarvis/hugo-extended:${HUGO_VERSION}"
fi

# process command
case "${SCRIPT_COMMAND}" in
  "hugo-version")
    echo "${HUGO_VERSION}"
    exit 0
    ;;
  "serve")
    HUGO_PARAMETERS="--source ${SOURCE_DIR} server --buildDrafts --buildFuture --bind 0.0.0.0 --baseURL ''"
    ;;
  "build")
    HUGO_PARAMETERS="--verbose --source ${SOURCE_DIR} --destination ${TARGET_DIR}"
    ;;
  "help"|"--help")
    print_usage_and_exit
    ;;
  *)
    print_usage_and_exit "Invalid command: ${SCRIPT_COMMAND}"
    ;;
esac

# check local Hugo version if docker isn't used
[[ "${HUGO_COMMAND}" =~ docker ]] || assert_hugo_installation "${HUGO_VERSION}"

pushd "${SCRIPT_DIR}" || exit
git submodule update --init --recursive
if [[ "$SKIP_CONNECTOR_DOCS" != "true" ]]; then
  load_connector_docs_from_file "${SCRIPT_DIR}/connector_docs.conf" "${SCRIPT_DIR}/tmp" "${SCRIPT_DIR}/themes/connectors"
fi
popd || exit

# pulls most-recent dependencies
eval "${HUGO_COMMAND} mod get -u"

eval "${HUGO_COMMAND} ${HUGO_PARAMETERS}"
