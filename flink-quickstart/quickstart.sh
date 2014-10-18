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


PACKAGE=quickstart

mvn archetype:generate 								\
  -DarchetypeGroupId=org.apache.flink 				\
  -DarchetypeArtifactId=flink-quickstart-java		\
  -DarchetypeVersion=0.6.1-incubating				\
  -DgroupId=org.apache.flink 						\
  -DartifactId=$PACKAGE								\
  -Dversion=0.1										\
  -Dpackage=org.apache.flink 						\
  -DinteractiveMode=false

#
# Give some guidance
#
echo -e "\\n\\n"
echo -e "\\tA sample quickstart Flink Job has been created."
echo -e "\\tSwitch into the directory using"
echo -e "\\t\\t cd $PACKAGE"
echo -e "\\tImport the project there using your favorite IDE (Import it as a maven project)"
echo -e "\\tBuild a jar inside the directory using"
echo -e "\\t\\t mvn clean package"
echo -e "\\tYou will find the runnable jar in $PACKAGE/target"
echo -e "\\tConsult our website if you have any troubles: http://flink.incubator.apache.org/community.html#mailing-lists"
echo -e "\\n\\n"


