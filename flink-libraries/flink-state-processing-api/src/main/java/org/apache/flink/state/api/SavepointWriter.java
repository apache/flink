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
import org.apache.flink.api.common.functions.RichMapFunction;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM;

/**
 * A {@code SavepointWriter} can create new savepoints from bounded data streams. This can allow for
 * boostrapping state for new applications or modifying the savepoints of existing jobs.
 */
@PublicEvolving
public class SavepointWriter {

    @Nullable private final StreamExecutionEnvironment executionEnvironment;

    private final Map<OperatorIdentifier, OperatorIdentifier> uidTransformationMap =
            new HashMap<>();

    /** @deprecated use {@link #fromExistingSavepoint(StreamExecutionEnvironment, String)} */
    @Deprecated
    public static SavepointWriter fromExistingSavepoint(String path) throws IOException {
        return new SavepointWriter(readSavepointMetadata(path), null, null);
    }

    /**
     * Loads an existing savepoint. Useful if you want to modify or extend the state of an existing
     * application. The savepoint will be written using the state backend defined via the clusters
     * configuration.
     *
     * @param path The path to an existing savepoint on disk.
     * @return A {@link SavepointWriter}.
     * @see #fromExistingSavepoint(StreamExecutionEnvironment, String, StateBackend)
     * @see #withConfiguration(ConfigOption, Object)
     */
    public static SavepointWriter fromExistingSavepoint(
            StreamExecutionEnvironment executionEnvironment, String path) throws IOException {
        return new SavepointWriter(readSavepointMetadata(path), null, executionEnvironment);
    }

    /**
     * @deprecated use {@link #fromExistingSavepoint(StreamExecutionEnvironment, String,
     *     StateBackend)}
     */
    @Deprecated
    public static SavepointWriter fromExistingSavepoint(String path, StateBackend stateBackend)
            throws IOException {
        return new SavepointWriter(readSavepointMetadata(path), stateBackend, null);
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
    public static SavepointWriter fromExistingSavepoint(
            StreamExecutionEnvironment executionEnvironment, String path, StateBackend stateBackend)
            throws IOException {
        return new SavepointWriter(readSavepointMetadata(path), stateBackend, executionEnvironment);
    }

    private static SavepointMetadataV2 readSavepointMetadata(String path) throws IOException {
        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(path);

        int maxParallelism =
                metadata.getOperatorStates().stream()
                        .map(OperatorState::getMaxParallelism)
                        .max(Comparator.naturalOrder())
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "Savepoint must contain at least one operator state."));

        return new SavepointMetadataV2(
                maxParallelism, metadata.getMasterStates(), metadata.getOperatorStates());
    }

    /** @deprecated use {@link #newSavepoint(StreamExecutionEnvironment, int)} */
    @Deprecated
    public static SavepointWriter newSavepoint(int maxParallelism) {
        return new SavepointWriter(createSavepointMetadata(maxParallelism), null, null);
    }

    /**
     * Creates a new savepoint. The savepoint will be written using the state backend defined via
     * the clusters configuration.
     *
     * @param maxParallelism The max parallelism of the savepoint.
     * @return A {@link SavepointWriter}.
     * @see #newSavepoint(StreamExecutionEnvironment, StateBackend, int)
     * @see #withConfiguration(ConfigOption, Object)
     */
    public static SavepointWriter newSavepoint(
            StreamExecutionEnvironment executionEnvironment, int maxParallelism) {
        return new SavepointWriter(
                createSavepointMetadata(maxParallelism), null, executionEnvironment);
    }

    /** @deprecated use {@link #newSavepoint(StreamExecutionEnvironment, StateBackend, int)} */
    @Deprecated
    public static SavepointWriter newSavepoint(StateBackend stateBackend, int maxParallelism) {
        return new SavepointWriter(createSavepointMetadata(maxParallelism), stateBackend, null);
    }

    /**
     * Creates a new savepoint.
     *
     * @param stateBackend The state backend of the savepoint used for keyed state.
     * @param maxParallelism The max parallelism of the savepoint.
     * @return A {@link SavepointWriter}.
     * @see #newSavepoint(StreamExecutionEnvironment, int)
     */
    public static SavepointWriter newSavepoint(
            StreamExecutionEnvironment executionEnvironment,
            StateBackend stateBackend,
            int maxParallelism) {
        return new SavepointWriter(
                createSavepointMetadata(maxParallelism), stateBackend, executionEnvironment);
    }

    private static SavepointMetadataV2 createSavepointMetadata(int maxParallelism) {
        Preconditions.checkArgument(
                maxParallelism > 0 && maxParallelism <= UPPER_BOUND_MAX_PARALLELISM,
                "Maximum parallelism must be between 1 and "
                        + UPPER_BOUND_MAX_PARALLELISM
                        + ". Found: "
                        + maxParallelism);

        return new SavepointMetadataV2(
                maxParallelism, Collections.emptyList(), Collections.emptyList());
    }

    /**
     * The savepoint metadata, which maintains the current set of existing / newly added operator
     * states.
     */
    protected final SavepointMetadataV2 metadata;

    /** The state backend to use when writing this savepoint. */
    @Nullable protected final StateBackend stateBackend;

    private final Configuration configuration;

    private SavepointWriter(
            SavepointMetadataV2 metadata,
            @Nullable StateBackend stateBackend,
            @Nullable StreamExecutionEnvironment executionEnvironment) {
        Preconditions.checkNotNull(metadata, "The savepoint metadata must not be null");
        this.metadata = metadata;
        this.stateBackend = stateBackend;
        this.configuration = new Configuration();
        this.executionEnvironment = executionEnvironment;
    }

    /** @deprecated use {@link #removeOperator(OperatorIdentifier)} */
    @Deprecated
    public SavepointWriter removeOperator(String uid) {
        return removeOperator(OperatorIdentifier.forUid(uid));
    }

    /**
     * Drop an existing operator from the savepoint.
     *
     * @param identifier The identifier of the operator.
     * @return A modified savepoint.
     */
    public SavepointWriter removeOperator(OperatorIdentifier identifier) {
        metadata.removeOperator(identifier);
        return this;
    }

    @Deprecated
    public <T> SavepointWriter withOperator(
            String uid, StateBootstrapTransformation<T> transformation) {
        return withOperator(OperatorIdentifier.forUid(uid), transformation);
    }

    /**
     * Adds a new operator to the savepoint.
     *
     * @param identifier The identifier of the operator.
     * @param transformation The operator to be included.
     * @return The modified savepoint.
     */
    public <T> SavepointWriter withOperator(
            OperatorIdentifier identifier, StateBootstrapTransformation<T> transformation) {
        metadata.addOperator(identifier, transformation);
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
     * Changes the identifier of an operator.
     *
     * <p>This method is comparatively cheap since it only modifies savepoint metadata without
     * reading the entire savepoint data.
     *
     * <p>Use-cases include, but are not limited to:
     *
     * <ul>
     *   <li>assigning a UID to an operator that did not have a UID assigned before
     *   <li>changing the UID of an operator
     *   <li>swapping the states of 2 operators
     * </ul>
     *
     * <p>Identifier changes are applied after all other operations; in the following example the
     * savepoint will only contain UID_2.
     *
     * <pre>
     *     SavepointWriter savepoint = ...
     *     savepoint.withOperator(UID_1, ...)
     *     savepoint.changeOperatorIdentifier(UID_1, UID_2)
     *     savepoint.write(...)
     * </pre>
     *
     * <p>You cannot define a chain of changes; in the following example the savepoint will only
     * contain UID_2.
     *
     * <pre>
     *     SavepointWriter savepoint = ...
     *     savepoint.withOperator(UID_1, ...)
     *     savepoint.changeOperatorIdentifier(UID_1, UID_2)
     *     savepoint.changeOperatorIdentifier(UID_2, UID_3)
     *     savepoint.write(...)
     * </pre>
     *
     * @param from operator whose identifier should be changed
     * @param to desired identifier
     * @return The modified savepoint.
     */
    public SavepointWriter changeOperatorIdentifier(
            OperatorIdentifier from, OperatorIdentifier to) {
        this.uidTransformationMap.put(from, to);
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
        Optional<DataStream<OperatorState>> newOperatorStates =
                writeOperatorStates(newOperatorTransformations, configuration, savepointPath);

        if (executionEnvironment == null && !newOperatorStates.isPresent()) {
            throw new IllegalStateException(
                    "Savepoint must contain at least one operator if no execution environment was provided.");
        }

        List<OperatorState> existingOperators = metadata.getExistingOperators();

        if (!newOperatorStates.isPresent() && existingOperators.isEmpty()) {
            throw new IllegalStateException(
                    "Savepoint must contain at least one operator to be created.");
        }

        getFinalOperatorStates(
                        executionEnvironment != null
                                ? executionEnvironment
                                : newOperatorStates.get().getExecutionEnvironment(),
                        existingOperators,
                        newOperatorStates.orElse(null),
                        path)
                .transform(
                        "reduce(OperatorState)",
                        TypeInformation.of(CheckpointMetadata.class),
                        new GroupReduceOperator<>(
                                new MergeOperatorStates(metadata.getMasterStates())))
                .forceNonParallel()
                .map(new CheckpointMetadataCheckpointMetadataMapFunction(this.uidTransformationMap))
                .setParallelism(1)
                .addSink(new OutputFormatSinkFunction<>(new SavepointOutputFormat(savepointPath)))
                .setParallelism(1)
                .name(path);
    }

    private static DataStream<OperatorState> getFinalOperatorStates(
            StreamExecutionEnvironment executionEnvironment,
            List<OperatorState> existingOperators,
            @Nullable DataStream<OperatorState> newOperatorStates,
            String path) {
        if (existingOperators.isEmpty()) {
            return newOperatorStates;
        }

        DataStream<OperatorState> existingOperatorStates =
                executionEnvironment
                        .fromCollection(existingOperators)
                        .name("existingOperatorStates");

        existingOperatorStates
                .flatMap(new StatePathExtractor())
                .setParallelism(1)
                .addSink(new OutputFormatSinkFunction<>(new FileCopyFunction(path)));

        return newOperatorStates != null
                ? newOperatorStates.union(existingOperatorStates)
                : existingOperatorStates;
    }

    private Optional<DataStream<OperatorState>> writeOperatorStates(
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
                .reduce(DataStream::union);
    }

    private static class CheckpointMetadataCheckpointMetadataMapFunction
            extends RichMapFunction<CheckpointMetadata, CheckpointMetadata> {
        private static final long serialVersionUID = 1L;

        private final Map<OperatorIdentifier, OperatorIdentifier> uidTransformationMap;

        public CheckpointMetadataCheckpointMetadataMapFunction(
                Map<OperatorIdentifier, OperatorIdentifier> uidTransformationMap) {
            this.uidTransformationMap = new HashMap<>(uidTransformationMap);
        }

        @Override
        public CheckpointMetadata map(CheckpointMetadata value) throws Exception {
            final List<OperatorState> mapped =
                    value.getOperatorStates().stream()
                            .map(
                                    operatorState -> {
                                        OperatorIdentifier operatorIdentifier =
                                                OperatorIdentifier.forUidHash(
                                                        operatorState
                                                                .getOperatorID()
                                                                .toHexString());

                                        final OperatorIdentifier transformedIdentifier =
                                                uidTransformationMap.remove(operatorIdentifier);
                                        if (transformedIdentifier != null) {
                                            return operatorState.copyWithNewOperatorID(
                                                    transformedIdentifier.getOperatorId());
                                        }
                                        return operatorState;
                                    })
                            .collect(Collectors.toList());
            return new CheckpointMetadata(value.getCheckpointId(), mapped, value.getMasterStates());
        }

        @Override
        public void close() throws Exception {
            if (!uidTransformationMap.isEmpty()) {
                throw new FlinkRuntimeException(
                        "Some identifier changes were never applied!"
                                + uidTransformationMap.entrySet().stream()
                                        .map(Map.Entry::toString)
                                        .collect(Collectors.joining("\n\t", "\n\t", "")));
            }
        }
    }
}
