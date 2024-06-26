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

package org.apache.flink.streaming.runtime.execution;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.lineage.LineageGraph;

/** Default implementation for {@link JobCreatedEvent}. */
@Internal
public class DefaultJobCreatedEvent implements JobCreatedEvent {
    private final JobID jobId;
    private final String jobName;
    private final LineageGraph lineageGraph;
    private final RuntimeExecutionMode executionMode;

    public DefaultJobCreatedEvent(
            JobID jobId,
            String jobName,
            LineageGraph lineageGraph,
            RuntimeExecutionMode executionMode) {
        this.jobId = jobId;
        this.jobName = jobName;
        this.lineageGraph = lineageGraph;
        this.executionMode = executionMode;
    }

    @Override
    public JobID jobId() {
        return jobId;
    }

    @Override
    public String jobName() {
        return jobName;
    }

    @Override
    public LineageGraph lineageGraph() {
        return lineageGraph;
    }

    @Override
    public RuntimeExecutionMode executionMode() {
        return executionMode;
    }
}
