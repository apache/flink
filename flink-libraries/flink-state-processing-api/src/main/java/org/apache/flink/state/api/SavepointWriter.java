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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.output.FileCopyFunction;
import org.apache.flink.state.api.output.MergeOperatorStates;
import org.apache.flink.state.api.output.SavepointOutputFormat;
import org.apache.flink.state.api.output.StatePathExtractor;
import org.apache.flink.state.api.output.operators.GroupReduceOperator;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.state.api.runtime.StateBootstrapTransformationWithID;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadataV2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM;

/**
 * A {@code SavepointWriter} can create new savepoints from bounded data streams. This can allow for
 * boostrapping state for new applications or modifying the savepoints of existing jobs.
 */
@PublicEvolving
public class SavepointWriter {

    /**
     * Loads an existing savepoint. Useful if you want to modify or extend the state of an existing
     * application. The savepoint will be written using the state backend defined via the clusters
     * configuration.
     *
     * @param path The path to an existing savepoint on disk.
     * @return A {@link SavepointWriter}.
     * @see #fromExistingSavepoint(String, StateBackend)
     * @see #withConfiguration(ConfigOption, Object)
     */
    public static SavepointWriter fromExistingSavepoint(String path) throws IOException {
        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(path);

        int maxParallelism =
                metadata.getOperatorStates().stream()
                        .map(OperatorState::getMaxParallelism)
                        .max(Comparator.naturalOrder())
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "Savepoint must contain at least one operator state."));

        SavepointMetadataV2 savepointMetadata =
                new SavepointMetadataV2(
                        maxParallelism, metadata.getMasterStates(), metadata.getOperatorStates());
        return new SavepointWriter(savepointMetadata, null);
    }

    /**
     * Loads an existing savepoint. Useful if you want to modify or extend the state of an existing
     * application.
     *
     * @param path The path to an existing savepoint on disk.
     * @param stateBackend The state backend of the savepoint.
     * @return A {@link SavepointWriter}.
     * @see #fromExistingSavepoint(String)
     */
    public static SavepointWriter fromExistingSavepoint(String path, StateBackend stateBackend)
            throws IOException {
        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(path);

        int maxParallelism =
                metadata.getOperatorStates().stream()
                        .map(OperatorState::getMaxParallelism)
                        .max(Comparator.naturalOrder())
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "Savepoint must contain at least one operator state."));

        SavepointMetadataV2 savepointMetadata =
                new SavepointMetadataV2(
                        maxParallelism, metadata.getMasterStates(), metadata.getOperatorStates());
        return new SavepointWriter(savepointMetadata, stateBackend);
    }

    /**
     * Creates a new savepoint. The savepoint will be written using the state backend defined via
     * the clusters configuration.
     *
     * @param maxParallelism The max parallelism of the savepoint.
     * @return A {@link SavepointWriter}.
     * @see #newSavepoint(StateBackend, int)
     * @see #withConfiguration(ConfigOption, Object)
     */
    public static SavepointWriter newSavepoint(int maxParallelism) {
        Preconditions.checkArgument(
                maxParallelism > 0 && maxParallelism <= UPPER_BOUND_MAX_PARALLELISM,
                "Maximum parallelism must be between 1 and "
                        + UPPER_BOUND_MAX_PARALLELISM
                        + ". Found: "
                        + maxParallelism);

        SavepointMetadataV2 metadata =
                new SavepointMetadataV2(
                        maxParallelism, Collections.emptyList(), Collections.emptyList());
        return new SavepointWriter(metadata, null);
    }

    /**
     * Creates a new savepoint.
     *
     * @param stateBackend The state backend of the savepoint used for keyed state.
     * @param maxParallelism The max parallelism of the savepoint.
     * @return A {@link SavepointWriter}.
     * @see #newSavepoint(int)
     */
    public static SavepointWriter newSavepoint(StateBackend stateBackend, int maxParallelism) {
        Preconditions.checkArgument(
                maxParallelism > 0 && maxParallelism <= UPPER_BOUND_MAX_PARALLELISM,
                "Maximum parallelism must be between 1 and "
                        + UPPER_BOUND_MAX_PARALLELISM
                        + ". Found: "
                        + maxParallelism);

        SavepointMetadataV2 metadata =
                new SavepointMetadataV2(
                        maxParallelism, Collections.emptyList(), Collections.emptyList());
        return new SavepointWriter(metadata, stateBackend);
    }

    /**
     * The savepoint metadata, which maintains the current set of existing / newly added operator
     * states.
     */
    protected final SavepointMetadataV2 metadata;

    /** The state backend to use when writing this savepoint. */
    @Nullable protected final StateBackend stateBackend;

    private final Configuration configuration;

    private SavepointWriter(SavepointMetadataV2 metadata, @Nullable StateBackend stateBackend) {
        Preconditions.checkNotNull(metadata, "The savepoint metadata must not be null");
        this.metadata = metadata;
        this.stateBackend = stateBackend;
        this.configuration = new Configuration();
    }

    /**
     * Drop an existing operator from the savepoint.
     *
     * @param uid The uid of the operator.
     * @return A modified savepoint.
     */
    public SavepointWriter removeOperator(String uid) {
        metadata.removeOperator(uid);
        return this;
    }

    /**
     * Adds a new operator to the savepoint.
     *
     * @param uid The uid of the operator.
     * @param transformation The operator to be included.
     * @return The modified savepoint.
     */
    public <T> SavepointWriter withOperator(
            String uid, StateBootstrapTransformation<T> transformation) {
        metadata.addOperator(uid, transformation);
        return this;
    }

    /**
     * Sets a configuration that will be applied to the stream operators used to bootstrap a new
     * savepoint.
     *
     * @param option metadata information
     * @param value value to be stored
     * @param <T> type of the value to be stored
     * @return The modified savepoint.
     */
    public <T> SavepointWriter withConfiguration(ConfigOption<T> option, T value) {
        configuration.set(option, value);
        return this;
    }

    /**
     * Write out a new or updated savepoint.
     *
     * @param path The path to where the savepoint should be written.
     */
    public final void write(String path) {
        final Path savepointPath = new Path(path);

        List<StateBootstrapTransformationWithID<?>> newOperatorTransformations =
                metadata.getNewOperators();
        DataStream<OperatorState> newOperatorStates =
                writeOperatorStates(newOperatorTransformations, configuration, savepointPath);

        List<OperatorState> existingOperators = metadata.getExistingOperators();

        DataStream<OperatorState> finalOperatorStates;
        if (existingOperators.isEmpty()) {
            finalOperatorStates = newOperatorStates;
        } else {
            DataStream<OperatorState> existingOperatorStates =
                    newOperatorStates
                            .getExecutionEnvironment()
                            .fromCollection(existingOperators)
                            .name("existingOperatorStates");

            existingOperatorStates
                    .flatMap(new StatePathExtractor())
                    .setParallelism(1)
                    .addSink(new OutputFormatSinkFunction<>(new FileCopyFunction(path)));

            finalOperatorStates = newOperatorStates.union(existingOperatorStates);
        }
        finalOperatorStates
                .transform(
                        "reduce(OperatorState)",
                        TypeInformation.of(CheckpointMetadata.class),
                        new GroupReduceOperator<>(
                                new MergeOperatorStates(metadata.getMasterStates())))
                .forceNonParallel()
                .addSink(new OutputFormatSinkFunction<>(new SavepointOutputFormat(savepointPath)))
                .setParallelism(1)
                .name(path);
    }

    private DataStream<OperatorState> writeOperatorStates(
            List<StateBootstrapTransformationWithID<?>> newOperatorStates,
            Configuration config,
            Path savepointWritePath) {
        return newOperatorStates.stream()
                .map(
                        newOperatorState ->
                                newOperatorState
                                        .getBootstrapTransformation()
                                        .writeOperatorState(
                                                newOperatorState.getOperatorID(),
                                                stateBackend,
                                                config,
                                                metadata.getMaxParallelism(),
                                                savepointWritePath))
                .reduce(DataStream::union)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Savepoint must contain at least one operator"));
    }
}
