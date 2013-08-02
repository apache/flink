#!/bin/bash
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

# These are used to mangle paths that are passed to java when using 
# cygwin. Cygwin paths are like linux paths, i.e. /path/to/somewhere
# but the windows java version expects them in Windows Format, i.e. C:\bla\blub.
# "cygpath" can do the conversion.
manglePath() {
    if [ "$(expr substr $(uname -s) 1 6)" == "CYGWIN" ]; then
        echo `cygpath -w $1`
    else
        echo $1
    fi
}

manglePathList() {
    # a path list, for example a java classpath
    if [[ "$OS" =~ Windows ]]; then
        echo `cygpath -wp $1`
    else
        echo $1
    fi
}

# The default Java heap size for the Nephele Job Manager in MB
DEFAULT_NEPHELE_JM_HEAP=256

# The default Java heap size for the Nephele Task Manager in MB
DEFAULT_NEPHELE_TM_HEAP=512

# Optional Nephele parameters
#NEPHELE_OPTS=""

# Optional Nephele SSH parameters
#NEPHELE_SSH_OPTS=""

# The maximum number of old log files to keep
MAX_LOG_FILE_NUMBER=5

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

# Convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# Define JAVA_HOME if it is not already set
if [ -z "${JAVA_HOME+x}" ]; then
        JAVA_HOME=/usr/lib/jvm/java-6-sun/
fi

if [ "$(expr substr $(uname -s) 1 6)" == "CYGWIN" ]; then
    JAVA_RUN=java
else
    JAVA_RUN=$JAVA_HOME/bin/java
fi

# Define HOSTNAME if it is not already set
if [ -z "${HOSTNAME+x}" ]; then
        HOSTNAME=`hostname`
fi

# Define NEPHELE_JM_HEAP if it is not already set
if [ -z "${NEPHELE_JM_HEAP+x}" ]; then
	NEPHELE_JM_HEAP=$DEFAULT_NEPHELE_JM_HEAP
fi

# Define NEPHELE_TM_HEAP if it is not already set
if [ -z "${NEPHELE_TM_HEAP+x}" ]; then
	NEPHELE_TM_HEAP=$DEFAULT_NEPHELE_TM_HEAP
fi

# Define the main directory of the Nephele installation
NEPHELE_ROOT_DIR=`dirname "$this"`/..
NEPHELE_LIB_DIR=$NEPHELE_ROOT_DIR/lib
NEPHELE_ROOT_DIR=`manglePath $NEPHELE_ROOT_DIR`
NEPHELE_CONF_DIR=$NEPHELE_ROOT_DIR/conf
NEPHELE_BIN_DIR=$NEPHELE_ROOT_DIR/bin
NEPHELE_LOG_DIR=$NEPHELE_ROOT_DIR/log

# Arguments for the JVM. Used for job manager and task manager JVMs
# DO NOT USE FOR MEMORY SETTINGS! Use DEFAULT_NEPHELE_JM_HEAP and
# DEFAULT_NEPHELE_TM_HEAP for that!
JVM_ARGS="-Djava.net.preferIPv4Stack=true"

# Default classpath 
CLASSPATH=`manglePathList $( echo $NEPHELE_LIB_DIR/*.jar . | sed 's/ /:/g' )`

# Auxilliary function which extracts the name of host from a line which
# also potentialy includes topology information and the instance type
extractHostName() {

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

# Auxilliary function for log file rotation
rotateLogFile() {

        log=$1;
        num=$MAX_LOG_FILE_NUMBER
        if [ -f "$log" -a "$num" -gt 0 ]; then
                while [ $num -gt 1 ]; do
                        prev=`expr $num - 1`
                        [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
                        num=$prev
                done
                mv "$log" "$log.$num";
        fi
}
