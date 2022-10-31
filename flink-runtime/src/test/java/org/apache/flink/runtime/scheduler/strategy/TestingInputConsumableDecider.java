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

package org.apache.flink.runtime.scheduler.strategy;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Mock {@link InputConsumableDecider} for testing. */
public class TestingInputConsumableDecider implements InputConsumableDecider {

    private final Set<ExecutionVertexID> inputConsumableExecutionVertices = new HashSet<>();

    private final Set<ExecutionVertexID> sourceVertices = new HashSet<>();

    private ExecutionVertexID lastExecutionToDecideInputConsumable;

    @Override
    public boolean isInputConsumable(
            ExecutionVertexID executionVertexID,
            Set<ExecutionVertexID> verticesToDeploy,
            Map<ConsumedPartitionGroup, Boolean> consumableStatusCache) {
        lastExecutionToDecideInputConsumable = executionVertexID;
        return sourceVertices.contains(executionVertexID)
                || inputConsumableExecutionVertices.contains(executionVertexID);
    }

    public void setInputConsumable(ExecutionVertexID executionVertex) {
        inputConsumableExecutionVertices.add(executionVertex);
    }

    public void addSourceVertices(Collection<ExecutionVertexID> sourceVertices) {
        this.sourceVertices.addAll(sourceVertices);
    }

    public ExecutionVertexID getLastExecutionToDecideInputConsumable() {
        return lastExecutionToDecideInputConsumable;
    }
}
