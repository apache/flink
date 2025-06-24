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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.Collection;
import java.util.Map;

/** A dummy implementation of the {@link BatchJobRecoveryHandler}. */
public class DummyBatchJobRecoveryHandler implements BatchJobRecoveryHandler {
    @Override
    public void initialize(BatchJobRecoveryContext batchJobRecoveryContext) {}

    @Override
    public void startRecovering() {}

    @Override
    public void stop(boolean cleanUp) {}

    @Override
    public boolean needRecover() {
        return false;
    }

    @Override
    public boolean isRecovering() {
        return false;
    }

    @Override
    public void onExecutionVertexReset(Collection<ExecutionVertexID> vertices) {}

    @Override
    public void onExecutionJobVertexInitialization(
            JobVertexID jobVertexId,
            int parallelism,
            Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos) {}

    @Override
    public void onExecutionFinished(ExecutionVertexID executionVertexId) {}
}
