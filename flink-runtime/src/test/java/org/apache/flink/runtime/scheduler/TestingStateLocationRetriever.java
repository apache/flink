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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Implementation of {@link StateLocationRetriever} for test purposes. */
class TestingStateLocationRetriever implements StateLocationRetriever {

    private final Map<ExecutionVertexID, TaskManagerLocation> stateLocations = new HashMap<>();

    @Override
    public Optional<TaskManagerLocation> getStateLocation(
            final ExecutionVertexID executionVertexId) {
        return Optional.ofNullable(stateLocations.get(executionVertexId));
    }

    void setStateLocation(
            final ExecutionVertexID executionVertexId, final TaskManagerLocation location) {
        stateLocations.put(executionVertexId, location);
    }
}
