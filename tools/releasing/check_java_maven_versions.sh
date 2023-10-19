#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Define the required versions
required_java_version="1.8"
required_maven_version="3.8.6"

# Check Java version
java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
if [[ "$java_version" == "$required_java_version"* ]]; then
    echo "Java version is correct: $java_version"
else
    echo "Java version is incorrect. Required version: $required_java_version, but it is $java_version"
    exit 1
fi

# Check Maven version
maven_version=$(mvn -v | grep -oE 'Apache Maven [0-9]+\.[0-9]+\.[0-9]+' | awk '{print $3}')
if [[ "$maven_version" == "$required_maven_version" ]]; then
    echo "Maven version is correct: $maven_version"
else
    echo "Maven version is incorrect. Required version: $required_maven_version, but it is $maven_version"
    exit 1
fi
