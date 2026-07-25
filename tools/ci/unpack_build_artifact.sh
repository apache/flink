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
# Set timestamps: proto < source < compiled, so Maven skips recompilation
# 5-second gaps ensure reliable ordering on filesystems with low resolution
BASE_TIME=$(date +%s)

# T+0: proto files (oldest - won't trigger regeneration)
PROTO_TIME=$(date -d "@$BASE_TIME" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -r "$BASE_TIME" '+%Y-%m-%d %H:%M:%S')
find . -type f -name '*.proto' -exec touch -d "$PROTO_TIME" {} +

# T+5: source files
SOURCE_TIME=$(date -d "@$((BASE_TIME + 5))" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -r "$((BASE_TIME + 5))" '+%Y-%m-%d %H:%M:%S')
find . -type f \( -name '*.java' -o -name '*.scala' \) -exec touch -d "$SOURCE_TIME" {} +

# T+10: compiled files (newest - Maven sees "up-to-date", skips recompile)
COMPILED_TIME=$(date -d "@$((BASE_TIME + 10))" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -r "$((BASE_TIME + 10))" '+%Y-%m-%d %H:%M:%S')
find . -type f \( -name '*.class' -o -name '*.timestamp' \) -exec touch -d "$COMPILED_TIME" {} +
