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

################################################################################
# Adopted from "flink" bash script
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

# get flink config
. "$bin"/config.sh

if [ "$FLINK_IDENT_STRING" = "" ]; then
        FLINK_IDENT_STRING="$USER"
fi

CC_CLASSPATH=`constructFlinkClassPath`

################################################################################
# SQL client specific logic
################################################################################

log=$FLINK_LOG_DIR/flink-$FLINK_IDENT_STRING-sql-client-$HOSTNAME.log
log_setting=(-Dlog.file="$log" -Dlog4j.configuration=file:"$FLINK_CONF_DIR"/log4j-cli.properties -Dlog4j.configurationFile=file:"$FLINK_CONF_DIR"/log4j-cli.properties -Dlogback.configurationFile=file:"$FLINK_CONF_DIR"/logback.xml)

# get path of jar in /opt if it exist
FLINK_SQL_CLIENT_JAR=$(find "$FLINK_OPT_DIR" -regex ".*flink-sql-client.*.jar")

# add flink-python jar to the classpath
if [[ ! "$CC_CLASSPATH" =~ .*flink-python.*.jar ]]; then
    FLINK_PYTHON_JAR=$(find "$FLINK_OPT_DIR" -regex ".*flink-python.*.jar")
    if [ -n "$FLINK_PYTHON_JAR" ]; then
        CC_CLASSPATH="$CC_CLASSPATH:$FLINK_PYTHON_JAR"
    fi
fi

# add flink-sql-gateway jar to the classpath
if [[ ! "$CC_CLASSPATH" =~ .*flink-sql-gateway.*.jar ]]; then
    FLINK_SQL_GATEWAY_JAR=$(find "$FLINK_OPT_DIR" -regex ".*flink-sql-gateway.*.jar")
    if [ -n "$FLINK_SQL_GATEWAY_JAR" ]; then
        CC_CLASSPATH="$CC_CLASSPATH:$FLINK_SQL_GATEWAY_JAR"
    fi
fi

FLINK_ENV_JAVA_OPTS="${FLINK_ENV_JAVA_OPTS} ${FLINK_ENV_JAVA_OPTS_CLI}"

# check if SQL client is already in classpath and must not be shipped manually
if [[ "$CC_CLASSPATH" =~ .*flink-sql-client.*.jar ]]; then

    # start client without jar
    exec "$JAVA_RUN" $FLINK_ENV_JAVA_OPTS $JVM_ARGS "${log_setting[@]}" -classpath "`manglePathList "$CC_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS"`" org.apache.flink.table.client.SqlClient "$@"

# check if SQL client jar is in /opt
elif [ -n "$FLINK_SQL_CLIENT_JAR" ]; then

    # start client with jar
    exec "$JAVA_RUN" $FLINK_ENV_JAVA_OPTS $JVM_ARGS "${log_setting[@]}" -classpath "`manglePathList "$CC_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS:$FLINK_SQL_CLIENT_JAR"`" org.apache.flink.table.client.SqlClient "$@" --jar "`manglePath $FLINK_SQL_CLIENT_JAR`"

# write error message to stderr
else
    (>&2 echo "[ERROR] Flink SQL Client JAR file 'flink-sql-client*.jar' neither found in classpath nor /opt directory should be located in $FLINK_OPT_DIR.")

    # exit to force process failure
    exit 1
fi
