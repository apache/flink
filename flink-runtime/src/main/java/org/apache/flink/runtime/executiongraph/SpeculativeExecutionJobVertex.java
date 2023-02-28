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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.util.SerializedValue;

/** The ExecutionJobVertex which supports speculative execution. */
public class SpeculativeExecutionJobVertex extends ExecutionJobVertex {

    public SpeculativeExecutionJobVertex(
            InternalExecutionGraphAccessor graph,
            JobVertex jobVertex,
            VertexParallelismInformation parallelismInfo)
            throws JobException {
        super(graph, jobVertex, parallelismInfo);
    }

    @Override
    protected ExecutionVertex createExecutionVertex(
            ExecutionJobVertex jobVertex,
            int subTaskIndex,
            IntermediateResult[] producedDataSets,
            Time timeout,
            long createTimestamp,
            int executionHistorySizeLimit,
            int initialAttemptCount) {
        return new SpeculativeExecutionVertex(
                jobVertex,
                subTaskIndex,
                producedDataSets,
                timeout,
                createTimestamp,
                executionHistorySizeLimit,
                initialAttemptCount);
    }

    @Override
    protected OperatorCoordinatorHolder createOperatorCoordinatorHolder(
            SerializedValue<OperatorCoordinator.Provider> provider,
            ClassLoader classLoader,
            CoordinatorStore coordinatorStore,
            JobManagerJobMetricGroup jobManagerJobMetricGroup)
            throws Exception {
        return OperatorCoordinatorHolder.create(
                provider,
                this,
                classLoader,
                coordinatorStore,
                true,
                getTaskInformation(),
                jobManagerJobMetricGroup);
    }

    /** Factory to create {@link SpeculativeExecutionJobVertex}. */
    public static class Factory extends ExecutionJobVertex.Factory {
        @Override
        ExecutionJobVertex createExecutionJobVertex(
                InternalExecutionGraphAccessor graph,
                JobVertex jobVertex,
                VertexParallelismInformation parallelismInfo)
                throws JobException {
            return new SpeculativeExecutionJobVertex(graph, jobVertex, parallelismInfo);
        }
    }
}
