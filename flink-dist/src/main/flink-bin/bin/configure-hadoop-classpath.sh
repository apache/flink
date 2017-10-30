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

target="$0"
# For the case, the executable has been directly symlinked, figure out
# the correct bin path by following its symlink up to an upper bound.
# Note: we can't use the readlink utility here if we want to be POSIX
# compatible.
iteration=0
while [ -L "$target" ]; do
    if [ "$iteration" -gt 100 ]; then
        echo "Cannot resolve path: You have a cyclic symlink in $target."
        break
    fi
    ls=`ls -ld -- "$target"`
    target=`expr "$ls" : '.* -> \(.*\)$'`
    iteration=$((iteration + 1))
done

# Convert relative path to absolute path
bin=`dirname "$target"`
SYMLINK_RESOLVED_BIN=`cd "$bin"; pwd -P`

# Define the main directory of the flink installation
FLINK_ROOT_DIR=`dirname "$SYMLINK_RESOLVED_BIN"`
FLINK_LIB_DIR=$FLINK_ROOT_DIR/lib
FLINK_OPT_DIR=$FLINK_ROOT_DIR/opt

case "$1" in
        use-system-hadoop)

            # check if the "hadoop" binary is available, if yes, use that to augment the CLASSPATH
            if command -v hadoop >/dev/null 2>&1; then
                echo "Using the result of 'hadoop classpath' to augment the Hadoop classpath: `hadoop classpath`"
                INTERNAL_HADOOP_CLASSPATHS="${INTERNAL_HADOOP_CLASSPATHS}:`hadoop classpath`"
            fi

            if [ -n "${HBASE_CONF_DIR}" ]; then
                # Look for hbase command in HBASE_HOME or search PATH.
                if [ -n "${HBASE_HOME}" ]; then
                    HBASE_PATH="${HBASE_HOME}/bin"
                    HBASE_COMMAND=`command -v "${HBASE_PATH}/hbase"`
                else
                    HBASE_PATH=$PATH
                    HBASE_COMMAND=`command -v hbase`
                fi

                # Whether the hbase command was found.
                if [[ $? -eq 0 ]]; then
                    # Setup the HBase classpath. We add the HBASE_CONF_DIR last to ensure the right config directory is used.
                    INTERNAL_HADOOP_CLASSPATHS="${INTERNAL_HADOOP_CLASSPATHS}:`${HBASE_COMMAND} classpath`:${HBASE_CONF_DIR}"
                else
                    echo "HBASE_CONF_DIR=${HBASE_CONF_DIR} is set but 'hbase' command was not found in ${HBASE_PATH} so classpath could not be updated."
                fi

                unset HBASE_COMMAND HBASE_PATH
            fi

            echo "Configured Hadoop classpath: $INTERNAL_HADOOP_CLASSPATHS"
            echo "Writing to $bin/hadoop-classpath"
            echo $INTERNAL_HADOOP_CLASSPATHS > $bin/hadoop-classpath

            echo "Moving bundled Hadoop jar from lib directory to opt directory if it exists."
            if [ -f $FLINK_LIB_DIR/flink-shaded-hadoop2* ]; then
                mv -v $FLINK_LIB_DIR/flink-shaded-hadoop2* $FLINK_OPT_DIR/
            fi

            ;;

        use-bundled-hadoop)
            echo "Removing $bin/hadoop-classpath if it exists"
            if [ -f $bin/hadoop-classpath ]; then
                rm -v $bin/hadoop-classpath
            fi

            echo "Moving bundled Hadoop jar from opt directory to lib directory if it exists."
            if [ -f $FLINK_OPT_DIR/flink-shaded-hadoop2* ]; then
                mv -v $FLINK_OPT_DIR/flink-shaded-hadoop2* $FLINK_LIB_DIR/
            fi

            ;;

        no-hadoop)

            echo "Removing $bin/hadoop-classpath if it exits"
            if [ -f $bin/hadoop-classpath ]; then
                rm -v $bin/hadoop-classpath
            fi

            echo "Moving bundled Hadoop jar from lib directory to opt directory if it exists."
            if [ -f $FLINK_LIB_DIR/flink-shaded-hadoop2* ]; then
                mv -v $FLINK_LIB_DIR/flink-shaded-hadoop2* $FLINK_OPT_DIR/
            fi

            ;;

        *)
            echo $"Usage: $0 {use-system-hadoop|use-bundled-hadoop|no-hadoop}"
            exit 1

esac