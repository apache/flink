########################################################################################################################
# 
#  Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
# 
#  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
#  the License. You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
#  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
#  specific language governing permissions and limitations under the License.
# 
########################################################################################################################

#!/bin/bash

# Resolve links
this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# define JAVA_HOME if it is not already set
if [ -z "${JAVA_HOME+x}" ]; then
        JAVA_HOME=/usr/lib/jvm/java-6-sun/
fi

# define the main directory of the Nephele installation
NEPHELE_ROOT_DIR=`dirname "$this"`/..
NEPHELE_CONF_DIR=$NEPHELE_ROOT_DIR/conf
NEPHELE_BIN_DIR=$NEPHELE_ROOT_DIR/bin
NEPHELE_LIB_DIR=$NEPHELE_ROOT_DIR/lib
NEPHELE_LOG_DIR=$NEPHELE_ROOT_DIR/log

# calling options 
NEPHELE_OPTS=""
#NEPHELE_OPTS=

# jobmanager Java heap size in MB
JM_JHEAP=256

# taskmanager Java heap size in MB
TM_JHEAP=512

# arguments for the JVM. Used for job manager and task manager JVMs
# DO NOT USE FOR MEMORY SETTINGS! Use JM_JHEAP and TM_JHEAP for that!
JVM_ARGS="-Djava.net.preferIPv4Stack=true"

# classpath 
CLASSPATH=$( echo $NEPHELE_LIB_DIR/*.jar . | sed 's/ /:/g' )

# auxilliary function which extracts the name of host from a line which
# also potentialy includes topology information and the instance type
function extractHostName() {

        # extract first part of string (before any whitespace characters)
        SLAVE=$1
        # Remove types and possible comments
        if [[ "$SLAVE" =~ ^([0-9a-zA-Z/.-]+).*$ ]]; then
                SLAVE=${BASH_REMATCH[1]}
        fi
        # Extract the hostname from the network hierarchy
        if [[ "$SLAVE" =~ ^.*/([0-9a-zA-Z.-]+)$ ]]; then
                SLAVE=${BASH_REMATCH[1]}
        fi

        echo $SLAVE
}

