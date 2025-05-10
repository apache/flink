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


if ! [ -e $FLINK_ARTIFACT_DIR ]; then
    echo "Cached flink dir $FLINK_ARTIFACT_DIR does not exist. Exiting build."
    exit 1
fi

echo "Merging cache"
if [ -z "${FLINK_ARTIFACT_FILENAME}" ]; then
  # for Azure Pipelines
  cp -RT "$FLINK_ARTIFACT_DIR" "."
else
  # for GitHub Actions
  echo "Extract build artifacts ${FLINK_ARTIFACT_DIR}/${FLINK_ARTIFACT_FILENAME} into local directory."
  tar -xzf "${FLINK_ARTIFACT_DIR}/${FLINK_ARTIFACT_FILENAME}"
fi

echo "Adjusting timestamps"
# adjust timestamps of proto file to avoid re-generation
find . -type f -name '*.proto' -exec touch {} \;
# wait a bit for better odds of different timestamps
sleep 5

# adjust timestamps to prevent recompilation
find . -type f -name '*.java' -exec touch {} \;
find . -type f -name '*.scala' -exec touch {} \;
# wait a bit for better odds of different timestamps
sleep 5
find . -type f -name '*.class' -exec touch {} \;
find . -type f -name '*.timestamp' -exec touch {} \;
