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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.output.BootstrapStreamTaskRunner;
import org.apache.flink.state.api.output.OperatorSubtaskStateReducer;
import org.apache.flink.state.api.output.TaggedOperatorSubtaskState;
import org.apache.flink.state.api.output.operators.BroadcastStateBootstrapOperator;
import org.apache.flink.state.api.output.operators.GroupReduceOperator;
import org.apache.flink.state.api.runtime.MutableConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.util.TernaryBoolean;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.OptionalInt;

/**
 * A {@code StateBootstrapTransformation} represents a procedure of writing new operator state into
 * a {@code Savepoint}. It is defined by a {@code DataStream} containing the data to bootstrap with,
 * a factory for a stream operator that consumes the elements of the {@code DataStream} and
 * generates state to be snapshotted, as well as an optional key selector if the new operator state
 * is partitioned.
 *
 * @see OperatorTransformation
 * @see OneInputStateTransformation
 * @param <T> The input type of the transformation.
 */
@PublicEvolving
@SuppressWarnings("WeakerAccess")
public class StateBootstrapTransformation<T> {

    /** The data set containing the data to bootstrap the operator state with. */
    private final DataStream<T> stream;

    /**
     * Factory for the {@link StreamOperator} to consume and snapshot the bootstrapping data set.
     */
    private final SavepointWriterOperatorFactory factory;

    /**
     * Partitioner for the bootstrapping data set. Only relevant if this bootstraps partitioned
     * state.
     */
    @Nullable private final KeySelector<T, ?> keySelector;

    /**
     * Type information for the key of the bootstrapped state. Only relevant if this bootstraps
     * partitioned state.
     */
    @Nullable private final TypeInformation<?> keyType;

    /** Local max parallelism for the bootstrapped operator. */
    private final OptionalInt operatorMaxParallelism;

    StateBootstrapTransformation(
            DataStream<T> stream,
            OptionalInt operatorMaxParallelism,
            SavepointWriterOperatorFactory factory) {
        this.stream = stream;
        this.operatorMaxParallelism = operatorMaxParallelism;
        this.factory = factory;
        this.keySelector = null;
        this.keyType = null;
    }

    <K> StateBootstrapTransformation(
            DataStream<T> stream,
            OptionalInt operatorMaxParallelism,
            SavepointWriterOperatorFactory factory,
            @Nonnull KeySelector<T, K> keySelector,
            @Nonnull TypeInformation<K> keyType) {
        this.stream = stream;
        this.operatorMaxParallelism = operatorMaxParallelism;
        this.factory = factory;
        this.keySelector = keySelector;
        this.keyType = keyType;
    }

    /** @return The max parallelism for this operator. */
    int getMaxParallelism(int globalMaxParallelism) {
        return operatorMaxParallelism.orElse(globalMaxParallelism);
    }

    /**
     * @param operatorID The operator id for the stream operator.
     * @param stateBackend The state backend for the job.
     * @param config Additional configurations applied to the bootstrap stream tasks.
     * @param globalMaxParallelism Global max parallelism set for the savepoint.
     * @param savepointPath The path where the savepoint will be written.
     * @return The operator subtask states for this bootstrap transformation.
     */
    DataStream<OperatorState> writeOperatorState(
            OperatorID operatorID,
            StateBackend stateBackend,
            Configuration config,
            int globalMaxParallelism,
            Path savepointPath) {
        int localMaxParallelism = getMaxParallelism(globalMaxParallelism);

        return writeOperatorSubtaskStates(
                        operatorID, stateBackend, config, savepointPath, localMaxParallelism)
                .transform(
                        "reduce(OperatorSubtaskState)",
                        TypeInformation.of(OperatorState.class),
                        new GroupReduceOperator<>(
                                new OperatorSubtaskStateReducer(operatorID, localMaxParallelism)))
                .forceNonParallel();
    }

    @VisibleForTesting
    SingleOutputStreamOperator<TaggedOperatorSubtaskState> writeOperatorSubtaskStates(
            OperatorID operatorID,
            StateBackend stateBackend,
            Path savepointPath,
            int localMaxParallelism) {
        return writeOperatorSubtaskStates(
                operatorID, stateBackend, new Configuration(), savepointPath, localMaxParallelism);
    }

    private SingleOutputStreamOperator<TaggedOperatorSubtaskState> writeOperatorSubtaskStates(
            OperatorID operatorID,
            StateBackend stateBackend,
            Configuration additionalConfig,
            Path savepointPath,
            int localMaxParallelism) {
        StreamOperator<TaggedOperatorSubtaskState> operator =
                factory.createOperator(System.currentTimeMillis(), savepointPath);

        operator = stream.getExecutionEnvironment().clean(operator);

        final StreamConfig config = getConfig(operatorID, stateBackend, additionalConfig, operator);

        BootstrapStreamTaskRunner<T> operatorRunner =
                new BootstrapStreamTaskRunner<>(config, localMaxParallelism);

        DataStream<T> input = stream;
        if (keySelector != null) {
            input = stream.keyBy(this.keySelector);
        }

        SingleOutputStreamOperator<TaggedOperatorSubtaskState> subtaskStates =
                input.transform(
                                operatorID.toHexString(),
                                TypeInformation.of(TaggedOperatorSubtaskState.class),
                                operatorRunner)
                        .setMaxParallelism(localMaxParallelism);

        if (operator instanceof BroadcastStateBootstrapOperator) {
            subtaskStates = subtaskStates.setParallelism(1);
        } else {
            int currentParallelism = getParallelism(subtaskStates);
            if (currentParallelism > localMaxParallelism) {
                subtaskStates.setParallelism(localMaxParallelism);
            }
        }
        return subtaskStates;
    }

    @VisibleForTesting
    StreamConfig getConfig(
            OperatorID operatorID,
            StateBackend stateBackend,
            Configuration additionalConfig,
            StreamOperator<TaggedOperatorSubtaskState> operator) {
        // Eagerly perform a deep copy of the configuration, otherwise it will result in undefined
        // behavior
        // when deploying with multiple bootstrap transformations.
        Configuration deepCopy =
                new Configuration(
                        MutableConfig.of(stream.getExecutionEnvironment().getConfiguration()));
        deepCopy.addAll(additionalConfig);

        final StreamConfig config = new StreamConfig(deepCopy);
        config.setChainStart();
        config.setCheckpointingEnabled(true);
        config.setCheckpointMode(CheckpointingMode.EXACTLY_ONCE);

        if (keyType != null) {
            TypeSerializer<?> keySerializer =
                    keyType.createSerializer(stream.getExecutionEnvironment().getConfig());

            config.setStateKeySerializer(keySerializer);
            config.setStatePartitioner(0, keySelector);
        }

        config.setStreamOperator(operator);
        config.setOperatorName(operatorID.toHexString());
        config.setOperatorID(operatorID);
        config.setStateBackend(stateBackend);
        // This means leaving this stateBackend unwrapped.
        config.setChangelogStateBackendEnabled(TernaryBoolean.FALSE);
        config.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.STATE_BACKEND, 1.0);
        return config;
    }

    private static int getParallelism(
            SingleOutputStreamOperator<TaggedOperatorSubtaskState> subtaskStates) {
        int parallelism = subtaskStates.getParallelism();
        if (parallelism == ExecutionConfig.PARALLELISM_DEFAULT) {
            parallelism = subtaskStates.getExecutionEnvironment().getParallelism();
        }

        return parallelism;
    }
}
