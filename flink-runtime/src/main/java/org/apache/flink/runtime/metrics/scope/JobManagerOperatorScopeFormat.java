/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.metrics.scope;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.util.AbstractID;

/**
 * The scope format for the {@link
 * org.apache.flink.runtime.metrics.groups.JobManagerOperatorMetricGroup}.
 */
public class JobManagerOperatorScopeFormat extends ScopeFormat {
    public JobManagerOperatorScopeFormat(String format, JobManagerJobScopeFormat parentFormat) {
        super(
                format,
                parentFormat,
                new String[] {
                    SCOPE_HOST,
                    SCOPE_JOB_ID,
                    SCOPE_JOB_NAME,
                    SCOPE_TASK_VERTEX_ID,
                    SCOPE_TASK_NAME,
                    SCOPE_OPERATOR_ID,
                    SCOPE_OPERATOR_NAME
                });
    }

    public String[] formatScope(
            JobManagerJobMetricGroup parent,
            AbstractID vertexId,
            String taskName,
            OperatorID operatorID,
            String operatorName) {

        final String[] template = copyTemplate();
        final String[] values = {
            parent.parent().hostname(),
            valueOrNull(parent.jobId()),
            valueOrNull(parent.jobName()),
            valueOrNull(vertexId),
            valueOrNull(taskName),
            valueOrNull(operatorID),
            valueOrNull(operatorName)
        };
        return bindVariables(template, values);
    }
}
