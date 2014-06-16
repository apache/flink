#!/bin/bash
########################################################################################################################
# 
#  Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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
    UNAME=$(uname -s)
    if [ "${UNAME:0:6}" == "CYGWIN" ]; then
        echo `cygpath -w $1`
    else
        echo $1
    fi
}

manglePathList() {
    UNAME=$(uname -s)
    # a path list, for example a java classpath
    if [ "${UNAME:0:6}" == "CYGWIN" ]; then
        echo `cygpath -wp $1`
    else
        echo $1
    fi
}

# Looks up a config value by key from a simple YAML-style key-value map.
# $1: key to look up
# $2: default value to return if key does not exist
# $3: config file to read from
readFromConfig() {
    local key=$1
    local defaultValue=$2
    local configFile=$3
    
    # first extract the value with the given key (1st sed), then trim the result (2nd sed)
    # if a key exists multiple times, take the "last" one (tail)
    local value=`sed -n "s/^[ ]*${key}[ ]*: \([^#]*\).*$/\1/p" "${configFile}" | sed "s/^ *//;s/ *$//" | tail -n 1`
    
    [ -z "$value" ] && echo "$defaultValue" || echo "$value"
}

########################################################################################################################
# DEFAULT CONFIG VALUES: These values will be used when nothing has been specified in conf/stratosphere-conf.yaml
# -or- the respective environment variables are not set.
########################################################################################################################


# WARNING !!! , these values are only used if there is nothing else is specified in
# conf/stratosphere-conf.yaml

DEFAULT_ENV_PID_DIR="/tmp"                          # Directory to store *.pid files to
DEFAULT_ENV_LOG_MAX=5                               # Maximum number of old log files to keep
DEFAULT_ENV_JAVA_OPTS=""                            # Optional JVM args
DEFAULT_ENV_SSH_OPTS=""                             # Optional SSH parameters running in cluster mode

########################################################################################################################
# CONFIG KEYS: The default values can be overwritten by the following keys in conf/stratosphere-conf.yaml
########################################################################################################################

KEY_JOBM_HEAP_MB="jobmanager.heap.mb"
KEY_TASKM_HEAP_MB="taskmanager.heap.mb"
KEY_ENV_PID_DIR="env.pid.dir"
KEY_ENV_LOG_MAX="env.log.max"
KEY_ENV_JAVA_HOME="env.java.home"
KEY_ENV_JAVA_OPTS="env.java.opts"
KEY_ENV_SSH_OPTS="env.ssh.opts"

########################################################################################################################
# PATHS AND CONFIG
########################################################################################################################

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

# Define the main directory of the stratosphere installation
STRATOSPHERE_ROOT_DIR=`dirname "$this"`/..
STRATOSPHERE_LIB_DIR=$STRATOSPHERE_ROOT_DIR/lib

# These need to be mangled because they are directly passed to java.
# The above lib path is used by the shell script to retrieve jars in a 
# directory, so it needs to be unmangled.
STRATOSPHERE_ROOT_DIR_MANGLED=`manglePath "$STRATOSPHERE_ROOT_DIR"`
STRATOSPHERE_CONF_DIR=$STRATOSPHERE_ROOT_DIR_MANGLED/conf
STRATOSPHERE_BIN_DIR=$STRATOSPHERE_ROOT_DIR_MANGLED/bin
STRATOSPHERE_LOG_DIR=$STRATOSPHERE_ROOT_DIR_MANGLED/log
YAML_CONF=${STRATOSPHERE_CONF_DIR}/stratosphere-conf.yaml

########################################################################################################################
# ENVIRONMENT VARIABLES
########################################################################################################################

# read JAVA_HOME from config with no default value
MY_JAVA_HOME=$(readFromConfig ${KEY_ENV_JAVA_HOME} "" "${YAML_CONF}")  
# check if config specified JAVA_HOME
if [ -z "${MY_JAVA_HOME}" ]; then
    # config did not specify JAVA_HOME. Use system JAVA_HOME
    MY_JAVA_HOME=${JAVA_HOME} 
fi
# check if we have a valid JAVA_HOME and if java is not available
if [ -z "${MY_JAVA_HOME}" ] && ! type java > /dev/null 2> /dev/null; then
    echo "Please specify JAVA_HOME. Either in Stratosphere config ./conf/stratosphere-conf.yaml or as system-wide JAVA_HOME."
    exit 1
else
    JAVA_HOME=${MY_JAVA_HOME}
fi

UNAME=$(uname -s)
if [ "${UNAME:0:6}" == "CYGWIN" ]; then
    JAVA_RUN=java
else
    if [[ -d $JAVA_HOME ]]; then
        JAVA_RUN=$JAVA_HOME/bin/java
    else
        JAVA_RUN=java
    fi
fi

# Define HOSTNAME if it is not already set
if [ -z "${HOSTNAME}" ]; then
    HOSTNAME=`hostname`
fi

# Define STRATOSPHERE_JM_HEAP if it is not already set
if [ -z "${STRATOSPHERE_JM_HEAP}" ]; then
    STRATOSPHERE_JM_HEAP=$(readFromConfig ${KEY_JOBM_HEAP_MB} 0 "${YAML_CONF}")
fi

# Define STRATOSPHERE_TM_HEAP if it is not already set
if [ -z "${STRATOSPHERE_TM_HEAP}" ]; then
    STRATOSPHERE_TM_HEAP=$(readFromConfig ${KEY_TASKM_HEAP_MB} 0 "${YAML_CONF}")
fi

if [ -z "${MAX_LOG_FILE_NUMBER}" ]; then
    MAX_LOG_FILE_NUMBER=$(readFromConfig ${KEY_ENV_LOG_MAX} ${DEFAULT_ENV_LOG_MAX} "${YAML_CONF}")
fi

if [ -z "${STRATOSPHERE_PID_DIR}" ]; then
    STRATOSPHERE_PID_DIR=$(readFromConfig ${KEY_ENV_PID_DIR} "${DEFAULT_ENV_PID_DIR}" "${YAML_CONF}")
fi

if [ -z "${STRATOSPHERE_ENV_JAVA_OPTS}" ]; then
    STRATOSPHERE_ENV_JAVA_OPTS=$(readFromConfig ${KEY_ENV_JAVA_OPTS} "${DEFAULT_ENV_JAVA_OPTS}" "${YAML_CONF}")
fi

if [ -z "${STRATOSPHERE_SSH_OPTS}" ]; then
    STRATOSPHERE_SSH_OPTS=$(readFromConfig ${KEY_ENV_SSH_OPTS} "${DEFAULT_ENV_SSH_OPTS}" "${YAML_CONF}")
fi

# Arguments for the JVM. Used for job and task manager JVMs.
# DO NOT USE FOR MEMORY SETTINGS! Use conf/stratosphere-conf.yaml with keys
# KEY_JOBM_HEAP_MB and KEY_TASKM_HEAP_MB for that!
JVM_ARGS=""

# Default classpath 
CLASSPATH=`manglePathList $( echo $STRATOSPHERE_LIB_DIR/*.jar . | sed 's/ /:/g' )`

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
