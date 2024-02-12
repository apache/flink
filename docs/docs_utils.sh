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

# Loads the documentation from an external connectors repository into passed theme folder
function load_connector_docs {
  local connector_name connector_repo ref temp_dir theme_dir

  # expected parameters:
  connector_name=$1
  connector_repo="https://github.com/apache/flink-connector-${connector_name}"
  ref=$2
  temp_dir=$3
  theme_dir=$4

  pushd "${temp_dir}" || exit
  git clone --single-branch --branch "${ref}" "${connector_repo}"
  rsync -a flink-connector-${connector_name}/docs/* "${theme_dir}/"
  popd || exit
}

# Parses a connector docs config file (space-separated) for connector name
# and branch and loads the connector docs for each entry
function load_connector_docs_from_file {
  local config_file temp_dir theme_dir

  # expected parameters:
  config_file="$1"
  temp_dir="$2"
  theme_dir="$3"

  # clean any pre-existing data
  rm -rf "${theme_dir}"
  mkdir -p "${theme_dir}"
  rm -rf "${temp_dir}"
  mkdir -p "${temp_dir}"

  while read -r line; do
    if [[ "${line}" =~ ^# ]] || [[ "${line}" =~ ^[^a-zA-Z]*$ ]]; then
      # skip comments or empty lines
      continue
    fi

    connector_name="$(echo $line | cut -d' ' -f1)"
    ref="$(echo $line | cut -d' ' -f2)"
    load_connector_docs "${connector_name}" "${ref}" "${temp_dir}" "${theme_dir}"
  done < "${config_file}"
}

# assertion for the local Hugo binaries
function assert_hugo_installation {
  local expected_hugo_version
  expected_hugo_version="${1}"
  if ! which hugo &> /dev/null ; then
    echo "[ERROR] No hugo binaries installed."
    exit 1
  elif [[ "$(hugo version | grep -c "hugo v${expected_hugo_version}")" == 0 ]]; then
    echo "[WARN] Hugo version does not match the expected one (expected ${expected_hugo_version}):"
    hugo version
  fi
}
