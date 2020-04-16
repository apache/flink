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


echo "Merging cache"
cp -RT "$CACHE_FLINK_DIR" "."
echo "Adjusting timestamps"
# adjust timestamps to prevent recompilation
find . -type f -name '*.java' | xargs touch
find . -type f -name '*.scala' | xargs touch
# wait a bit for better odds of different timestamps
sleep 5
find . -type f -name '*.class' | xargs touch
find . -type f -name '*.timestamp' | xargs touch


export M2_HOME=/home/vsts/maven_cache/apache-maven-3.2.5/ 
export PATH=/home/vsts/maven_cache/apache-maven-3.2.5/bin:$PATH
run_mvn -version
MVN_CALL="run_mvn install -DskipTests -Drat.skip"
$MVN_CALL
EXIT_CODE=$?

if [ $EXIT_CODE != 0 ]; then
	echo "=============================================================================="
	echo "Build error. Exit code: $EXIT_CODE. Failing build"
	echo "=============================================================================="
	exit $EXIT_CODE
fi

chmod -R +x build-target
chmod -R +x flink-end-to-end-tests
