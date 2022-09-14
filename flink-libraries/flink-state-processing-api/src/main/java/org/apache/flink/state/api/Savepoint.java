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

package org.apache.flink.state.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadata;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM;

/**
 * This class provides entry points for loading an existing savepoint, or a new empty savepoint.
 *
 * @see ExistingSavepoint
 * @see NewSavepoint
 * @see SavepointReader
 * @see SavepointWriter
 * @deprecated For creating a new savepoint, use {@link SavepointWriter} and the data stream api
 *     under batch execution. For reading a savepoint, use {@link SavepointReader} and the data
 *     stream api under batch execution.
 */
@PublicEvolving
@Deprecated
public final class Savepoint {

    private Savepoint() {}

    /**
     * Loads an existing savepoint. Useful if you want to query, modify, or extend the state of an
     * existing application. The savepoint will be read using the state backend defined via the
     * clusters configuration.
     *
     * @param env The execution environment used to transform the savepoint.
     * @param path The path to an existing savepoint on disk.
     * @see #load(ExecutionEnvironment, String, StateBackend)
     */
    public static ExistingSavepoint load(ExecutionEnvironment env, String path) throws IOException {
        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(path);

        int maxParallelism =
                metadata.getOperatorStates().stream()
                        .map(OperatorState::getMaxParallelism)
                        .max(Comparator.naturalOrder())
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "Savepoint must contain at least one operator state."));

        SavepointMetadata savepointMetadata =
                new SavepointMetadata(
                        maxParallelism, metadata.getMasterStates(), metadata.getOperatorStates());
        return new ExistingSavepoint(env, savepointMetadata, null);
    }

    /**
     * Loads an existing savepoint. Useful if you want to query, modify, or extend the state of an
     * existing application.
     *
     * @param env The execution environment used to transform the savepoint.
     * @param path The path to an existing savepoint on disk.
     * @param stateBackend The state backend of the savepoint.
     * @see #load(ExecutionEnvironment, String)
     */
    public static ExistingSavepoint load(
            ExecutionEnvironment env, String path, StateBackend stateBackend) throws IOException {
        Preconditions.checkNotNull(stateBackend, "The state backend must not be null");
        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(path);

        int maxParallelism =
                metadata.getOperatorStates().stream()
                        .map(OperatorState::getMaxParallelism)
                        .max(Comparator.naturalOrder())
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "Savepoint must contain at least one operator state."));

        SavepointMetadata savepointMetadata =
                new SavepointMetadata(
                        maxParallelism, metadata.getMasterStates(), metadata.getOperatorStates());
        return new ExistingSavepoint(env, savepointMetadata, stateBackend);
    }

    /**
     * Creates a new savepoint. The savepoint will be read using the state backend defined via the
     * clusters configuration.
     *
     * @param maxParallelism The max parallelism of the savepoint.
     * @return A new savepoint.
     * @see #create(StateBackend, int)
     */
    public static NewSavepoint create(int maxParallelism) {
        Preconditions.checkArgument(
                maxParallelism > 0 && maxParallelism <= UPPER_BOUND_MAX_PARALLELISM,
                "Maximum parallelism must be between 1 and "
                        + UPPER_BOUND_MAX_PARALLELISM
                        + ". Found: "
                        + maxParallelism);

        SavepointMetadata metadata =
                new SavepointMetadata(
                        maxParallelism, Collections.emptyList(), Collections.emptyList());
        return new NewSavepoint(metadata, null);
    }

    /**
     * Creates a new savepoint.
     *
     * @param stateBackend The state backend of the savepoint used for keyed state.
     * @param maxParallelism The max parallelism of the savepoint.
     * @return A new savepoint.
     * @see #create(int)
     */
    public static NewSavepoint create(StateBackend stateBackend, int maxParallelism) {
        Preconditions.checkNotNull(stateBackend, "The state backend must not be null");
        Preconditions.checkArgument(
                maxParallelism > 0 && maxParallelism <= UPPER_BOUND_MAX_PARALLELISM,
                "Maximum parallelism must be between 1 and "
                        + UPPER_BOUND_MAX_PARALLELISM
                        + ". Found: "
                        + maxParallelism);

        SavepointMetadata metadata =
                new SavepointMetadata(
                        maxParallelism, Collections.emptyList(), Collections.emptyList());
        return new NewSavepoint(metadata, stateBackend);
    }
}
