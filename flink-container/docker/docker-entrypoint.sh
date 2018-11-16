#!/bin/sh

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

### If unspecified, the hostname of the container is taken as the JobManager address
FLINK_HOME=${FLINK_HOME:-"/opt/flink/bin"}

JOB_CLUSTER="job-cluster"
TASK_MANAGER="task-manager"

CMD="$1"
shift;

if [ "${CMD}" == "--help" -o "${CMD}" == "-h" ]; then
    echo "Usage: $(basename $0) (${JOB_CLUSTER}|${TASK_MANAGER})"
    exit 0
elif [ "${CMD}" == "${JOB_CLUSTER}" -o "${CMD}" == "${TASK_MANAGER}" ]; then
    echo "Starting the ${CMD}"

    if [ "${CMD}" == "${TASK_MANAGER}" ]; then
        exec $FLINK_HOME/bin/taskmanager.sh start-foreground "$@"
    else
        exec $FLINK_HOME/bin/standalone-job.sh start-foreground "$@"
    fi
fi

exec "$@"
