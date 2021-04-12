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

package org.apache.flink.runtime.scheduler.exceptionhistory;

import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.function.QuadFunction;

import java.util.Map;

/**
 * {@code ExceptionHistoryEntryExtractor} extracts all the necessary information from given
 * executions to create corresponding {@link RootExceptionHistoryEntry RootExceptionHistoryEntries}.
 */
public class ExceptionHistoryEntryExtractor {

    /**
     * Extracts a {@link RootExceptionHistoryEntry} based on the passed local failure information.
     *
     * @param executionJobVertices The {@link ExecutionJobVertex} instances registry.
     * @param failedExecutionVertexId The {@link ExecutionVertexID} referring to the {@link
     *     ExecutionVertex} that is the root of the failure.
     * @param otherAffectedVertices The {@code ExecutionVertexID}s of other affected {@code
     *     ExecutionVertices} that, if failed as well, would be added as concurrent failures.
     * @return The {@code RootExceptionHistoryEntry}.
     * @throws IllegalArgumentException if one of the passed {@code ExecutionVertexID}s cannot be
     *     resolved into an {@code ExecutionVertex}.
     * @throws IllegalArgumentException if the {@code failedExecutionVertexID} refers to an {@code
     *     ExecutionVertex} that didn't fail.
     */
    public RootExceptionHistoryEntry extractLocalFailure(
            Map<JobVertexID, ExecutionJobVertex> executionJobVertices,
            ExecutionVertexID failedExecutionVertexId,
            Iterable<ExecutionVertexID> otherAffectedVertices) {
        final ExecutionVertex rootCauseExecutionVertex =
                getExecutionVertex(executionJobVertices, failedExecutionVertexId);

        final RootExceptionHistoryEntry root =
                createLocalExceptionHistoryEntry(
                        RootExceptionHistoryEntry::fromLocalFailure, rootCauseExecutionVertex);

        for (ExecutionVertexID otherExecutionVertexId : otherAffectedVertices) {
            final ExecutionVertex executionVertex =
                    getExecutionVertex(executionJobVertices, otherExecutionVertexId);
            if (executionVertex.getFailureInfo().isPresent()) {
                root.add(
                        createLocalExceptionHistoryEntry(
                                ExceptionHistoryEntry::new, executionVertex));
            }
        }

        return root;
    }

    /**
     * Extracts a {@link RootExceptionHistoryEntry} based on the global failure information.
     *
     * @param executionVertices The {@link ExecutionVertex ExecutionVertices} that are affected by
     *     the global failure and, if failed as well, would be added as concurrent failures to the
     *     entry.
     * @param rootCause The {@code Throwable} causing the failure.
     * @param timestamp The timestamp the failure occurred.
     * @return The {@code RootExceptionHistoryEntry}.
     * @throws IllegalArgumentException if one of the passed {@code ExecutionVertexID}s cannot be *
     *     resolved into an {@code ExecutionVertex}.
     */
    public RootExceptionHistoryEntry extractGlobalFailure(
            Iterable<ExecutionVertex> executionVertices, Throwable rootCause, long timestamp) {
        final RootExceptionHistoryEntry root =
                RootExceptionHistoryEntry.fromGlobalFailure(rootCause, timestamp);

        for (ExecutionVertex executionVertex : executionVertices) {
            if (!executionVertex.getFailureInfo().isPresent()) {
                continue;
            }

            final ExceptionHistoryEntry exceptionHistoryEntry =
                    createLocalExceptionHistoryEntry(ExceptionHistoryEntry::new, executionVertex);
            if (exceptionHistoryEntry != null) {
                root.add(exceptionHistoryEntry);
            }
        }

        return root;
    }

    private static ExecutionVertex getExecutionVertex(
            Map<JobVertexID, ExecutionJobVertex> executionJobVertices,
            ExecutionVertexID executionVertexID) {
        final ExecutionJobVertex executionJobVertex =
                executionJobVertices.get(executionVertexID.getJobVertexId());

        Preconditions.checkArgument(
                executionJobVertex != null,
                "The passed ExecutionVertexID does not correspond to any ExecutionJobVertex provided.");
        final ExecutionVertex[] executionVertices = executionJobVertex.getTaskVertices();

        Preconditions.checkArgument(
                executionVertices.length > executionVertexID.getSubtaskIndex(),
                "The ExecutionJobVertex referred by the passed ExecutionVertexID does not have the right amount of subtasks (expected subtask ID: {}; actual number of subtasks: {}).",
                executionVertexID.getSubtaskIndex(),
                executionJobVertex.getTaskVertices().length);
        return executionVertices[executionVertexID.getSubtaskIndex()];
    }

    private static <E extends ExceptionHistoryEntry> E createLocalExceptionHistoryEntry(
            QuadFunction<SerializedThrowable, Long, String, TaskManagerLocation, E>
                    exceptionHistoryEntryCreator,
            ExecutionVertex executionVertex) {
        return executionVertex
                .getFailureInfo()
                .map(
                        failureInfo ->
                                exceptionHistoryEntryCreator.apply(
                                        failureInfo.getException(),
                                        failureInfo.getTimestamp(),
                                        executionVertex.getTaskNameWithSubtaskIndex(),
                                        executionVertex
                                                .getCurrentExecutionAttempt()
                                                .getAssignedResourceLocation()))
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "The selected ExecutionVertex "
                                                + executionVertex.getID()
                                                + " didn't fail."));
    }
}
