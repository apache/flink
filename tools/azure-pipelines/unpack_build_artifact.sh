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
cp -RT "$FLINK_ARTIFACT_DIR" "."

echo "Adjusting timestamps"
# adjust timestamps to prevent recompilation
find . -type f -name '*.java' | xargs touch
find . -type f -name '*.scala' | xargs touch
# wait a bit for better odds of different timestamps
sleep 5
find . -type f -name '*.class' | xargs touch
find . -type f -name '*.timestamp' | xargs touch

