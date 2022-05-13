/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Records modifications of {@link org.apache.flink.runtime.executiongraph.ExecutionVertex
 * ExecutionVertices}, and allows for checking whether a vertex was modified.
 *
 * <p>Examples for modifications include:
 *
 * <ul>
 *   <li>cancellation of the underlying execution
 *   <li>deployment of the execution vertex
 * </ul>
 *
 * @see DefaultScheduler
 */
public class ExecutionVertexVersioner {

    private final Map<ExecutionVertexID, Long> executionVertexToVersion = new HashMap<>();

    public ExecutionVertexVersion recordModification(final ExecutionVertexID executionVertexId) {
        final Long newVersion = executionVertexToVersion.merge(executionVertexId, 1L, Long::sum);
        return new ExecutionVertexVersion(executionVertexId, newVersion);
    }

    public Map<ExecutionVertexID, ExecutionVertexVersion> recordVertexModifications(
            final Collection<ExecutionVertexID> vertices) {
        return vertices.stream()
                .map(this::recordModification)
                .collect(
                        Collectors.toMap(
                                ExecutionVertexVersion::getExecutionVertexId, Function.identity()));
    }

    public boolean isModified(final ExecutionVertexVersion executionVertexVersion) {
        final Long currentVersion =
                getCurrentVersion(executionVertexVersion.getExecutionVertexId());
        return currentVersion != executionVertexVersion.getVersion();
    }

    private Long getCurrentVersion(ExecutionVertexID executionVertexId) {
        final Long currentVersion = executionVertexToVersion.get(executionVertexId);
        Preconditions.checkState(
                currentVersion != null,
                "Execution vertex %s does not have a recorded version",
                executionVertexId);
        return currentVersion;
    }

    public Set<ExecutionVertexID> getUnmodifiedExecutionVertices(
            final Set<ExecutionVertexVersion> executionVertexVersions) {
        return executionVertexVersions.stream()
                .filter(executionVertexVersion -> !isModified(executionVertexVersion))
                .map(ExecutionVertexVersion::getExecutionVertexId)
                .collect(Collectors.toSet());
    }

    ExecutionVertexVersion getExecutionVertexVersion(ExecutionVertexID executionVertexId) {
        final long currentVersion = getCurrentVersion(executionVertexId);
        return new ExecutionVertexVersion(executionVertexId, currentVersion);
    }
}
