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

HERE=` basename "$PWD"`
if [[ "$HERE" != "docs" ]]; then
    echo "Please only execute in the docs/ directory";
    exit 1;
fi

function integrate_connector_docs {
  connector=$1
  ref=$2
  git clone --single-branch --branch ${ref} https://github.com/apache/flink-connector-${connector}
  theme_dir="../themes/connectors"
  mkdir -p "${theme_dir}"
  rsync -a flink-connector-${connector}/docs/content* "${theme_dir}/"
}

# Integrate the connector documentation

rm -rf themes/connectors/*
rm -rf tmp
mkdir tmp
cd tmp

# Since there's no documentation yet available for a release branch,
# we only get the documentation from the main branch
integrate_connector_docs elasticsearch v3.0.0
integrate_connector_docs aws v4.0
integrate_connector_docs opensearch v1.0.0

cd ..
rm -rf tmp
