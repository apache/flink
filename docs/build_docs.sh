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

if ! command -v hugo &> /dev/null
then
	echo "Hugo must be installed to run the docs locally"
	echo "Please see docs/README.md for more details"
	exit 1
fi
git submodule update --init --recursive

# whether to skip integrate connector docs. If contains arg '--skip-integrate-connector-docs' and
# the connectors directory is not empty, the external connector docs will not be generated.
connectors_dir="./themes/connectors"
SKIP_INTEGRATE_CONNECTOR_DOCS=""
for arg in "$@"; do
  if [ -d "$connectors_dir" ] && [ "$arg" == "--skip-integrate-connector-docs" ]; then
    SKIP_INTEGRATE_CONNECTOR_DOCS="--skip-integrate-connector-docs"
    break
  fi
done

./setup_docs.sh $SKIP_INTEGRATE_CONNECTOR_DOCS

hugo mod get -u
hugo -b "" serve 
