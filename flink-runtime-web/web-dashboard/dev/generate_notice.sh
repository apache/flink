#!/bin/bash
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
if ! npm list -g @wbmnky/license-report-generator > /dev/null
then
  npm install -g @wbmnky/license-report-generator
fi

devDir=$(cd "$(dirname "$0")" && pwd)
(cd "${devDir}/.." && license-report-generator --depth Infinity \
                         --template-file "notice-template" --template-dir "${devDir}" \
                         --out-dir "${devDir}/../../src/main/resources/META-INF" --out-file "NOTICE")
