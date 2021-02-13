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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link SchedulingExecutionVertex}. */
class DefaultExecutionVertex implements SchedulingExecutionVertex {

    private final ExecutionVertexID executionVertexId;

    private final List<DefaultResultPartition> consumedResults;

    private final List<DefaultResultPartition> producedResults;

    private final Supplier<ExecutionState> stateSupplier;

    DefaultExecutionVertex(
            ExecutionVertexID executionVertexId,
            List<DefaultResultPartition> producedPartitions,
            Supplier<ExecutionState> stateSupplier) {
        this.executionVertexId = checkNotNull(executionVertexId);
        this.consumedResults = new ArrayList<>();
        this.stateSupplier = checkNotNull(stateSupplier);
        this.producedResults = checkNotNull(producedPartitions);
    }

    @Override
    public ExecutionVertexID getId() {
        return executionVertexId;
    }

    @Override
    public ExecutionState getState() {
        return stateSupplier.get();
    }

    @Override
    public Iterable<DefaultResultPartition> getConsumedResults() {
        return consumedResults;
    }

    @Override
    public Iterable<DefaultResultPartition> getProducedResults() {
        return producedResults;
    }

    void addConsumedResult(DefaultResultPartition result) {
        consumedResults.add(result);
    }
}
