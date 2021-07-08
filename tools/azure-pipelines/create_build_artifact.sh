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

echo "Creating build artifact dir $FLINK_ARTIFACT_DIR"

cp -r . "$FLINK_ARTIFACT_DIR"

echo "Minimizing artifact files"

# reduces the size of the artifact directory to speed up
# the packing&upload / download&unpacking process
# by removing files not required for subsequent stages

# jars are re-built in subsequent stages, so no need to cache them (cannot be avoided)
find "$FLINK_ARTIFACT_DIR" -maxdepth 8 -type f -name '*.jar' | xargs rm -rf

# .git directory
# not deleting this can cause build stability issues
# merging the cached version sometimes fails
rm -rf "$FLINK_ARTIFACT_DIR/.git"

# AZ Pipelines has a problem with links.
rm "$FLINK_ARTIFACT_DIR/build-target"

