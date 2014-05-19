#!/usr/bin/env bash

########################################################################################################################
# Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
########################################################################################################################

PACKAGE=quickstart

mvn archetype:generate								\
  -DarchetypeGroupId=eu.stratosphere 				\
  -DarchetypeArtifactId=quickstart-scala-SNAPSHOT			\
  -DarchetypeVersion=0.5-SNAPSHOT					\
  -DgroupId=eu.stratoshere 						\
  -DartifactId=$PACKAGE								\
  -Dversion=0.1										\
  -Dpackage=eu.stratosphere.quickstart 				\
  -DinteractiveMode=false							\
  -DarchetypeCatalog=https://oss.sonatype.org/content/repositories/snapshots/

#
# Give some guidance
#
echo -e "\\n\\n"
echo -e "\\tA sample quickstart Stratosphere Job has been created."
echo -e "\\tSwitch into the directory using"
echo -e "\\t\\t cd $PACKAGE"
echo -e "\\tImport the project there using your favorite IDE (Import it as a maven project)"
echo -e "\\tBuild a jar inside the directory using"
echo -e "\\t\\t mvn clean package"
echo -e "\\tYou will find the runnable jar in $PACKAGE/target"
echo -e "\\tConsult our mailing list if you have any troubles: https://groups.google.com/forum/#!forum/stratosphere-dev"
echo -e "\\n\\n"


# Use this command if you want to specify the coordinates of your generated artifact
# in an interactive menu:
#
# mvn archetype:generate								\
#   -DarchetypeGroupId=eu.stratosphere 				\
#   -DarchetypeArtifactId=quickstart-scala		\
#   -DarchetypeVersion=0.4-SNAPSHOT					\
#   -DarchetypeCatalog=https://oss.sonatype.org/content/repositories/snapshots/
