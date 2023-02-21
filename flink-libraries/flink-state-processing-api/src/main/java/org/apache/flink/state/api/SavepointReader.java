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
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.api.input.BroadcastStateInputFormat;
import org.apache.flink.state.api.input.KeyedStateInputFormat;
import org.apache.flink.state.api.input.ListStateInputFormat;
import org.apache.flink.state.api.input.SourceBuilder;
import org.apache.flink.state.api.input.UnionStateInputFormat;
import org.apache.flink.state.api.input.operator.KeyedStateReaderOperator;
import org.apache.flink.state.api.runtime.MutableConfig;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadataV2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;

/** The entry point for reading state from a Flink savepoint. */
@PublicEvolving
public class SavepointReader {

    /**
     * Loads an existing savepoint. Useful if you want to query the state of an existing
     * application. The savepoint will be read using the state backend defined via the clusters
     * configuration.
     *
     * @param env The execution environment used to transform the savepoint.
     * @param path The path to an existing savepoint on disk.
     * @return A {@link SavepointReader}.
     */
    public static SavepointReader read(StreamExecutionEnvironment env, String path)
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
        return new SavepointReader(env, savepointMetadata, null);
    }

    /**
     * Loads an existing savepoint. Useful if you want to query the state of an existing
     * application.
     *
     * @param env The execution environment used to transform the savepoint.
     * @param path The path to an existing savepoint on disk.
     * @param stateBackend The state backend of the savepoint.
     * @return A {@link SavepointReader}.
     */
    public static SavepointReader read(
            StreamExecutionEnvironment env, String path, StateBackend stateBackend)
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
        return new SavepointReader(env, savepointMetadata, stateBackend);
    }

    /** The execution environment. Used for creating inputs for reading state. */
    private final StreamExecutionEnvironment env;

    /**
     * The savepoint metadata, which maintains the current set of existing / newly added operator
     * states.
     */
    private final SavepointMetadataV2 metadata;

    /**
     * The state backend that was previously used to write existing operator states in this
     * savepoint. If null, the reader will use the state backend defined via the cluster
     * configuration.
     */
    @Nullable private final StateBackend stateBackend;

    SavepointReader(
            StreamExecutionEnvironment env,
            SavepointMetadataV2 metadata,
            @Nullable StateBackend stateBackend) {
        Preconditions.checkNotNull(env, "The execution environment must not be null");
        Preconditions.checkNotNull(metadata, "The savepoint metadata must not be null");

        this.env = env;
        this.metadata = metadata;
        this.stateBackend = stateBackend;
    }

    /** @deprecated use {@link #readListState(OperatorIdentifier, String, TypeInformation)} */
    @Deprecated
    public <T> DataStream<T> readListState(String uid, String name, TypeInformation<T> typeInfo)
            throws IOException {
        return readListState(OperatorIdentifier.forUid(uid), name, typeInfo);
    }

    /**
     * Read operator {@code ListState} from a {@code Savepoint}.
     *
     * @param identifier The identifier of the operator.
     * @param name The (unique) name for the state.
     * @param typeInfo The type of the elements in the state.
     * @param <T> The type of the values that are in the list state.
     * @return A {@code DataStream} representing the elements in state.
     * @throws IOException If the savepoint path is invalid or the uid does not exist.
     */
    public <T> DataStream<T> readListState(
            OperatorIdentifier identifier, String name, TypeInformation<T> typeInfo)
            throws IOException {
        return readListState(identifier, typeInfo, new ListStateDescriptor<>(name, typeInfo));
    }

    /**
     * @deprecated use {@link #readListState(OperatorIdentifier, String, TypeInformation,
     *     TypeSerializer)}
     */
    @Deprecated
    public <T> DataStream<T> readListState(
            String uid, String name, TypeInformation<T> typeInfo, TypeSerializer<T> serializer)
            throws IOException {
        return readListState(OperatorIdentifier.forUid(uid), name, typeInfo, serializer);
    }

    /**
     * Read operator {@code ListState} from a {@code Savepoint} when a custom serializer was used;
     * e.g., a different serializer than the one returned by {@code
     * TypeInformation#createSerializer}.
     *
     * @param identifier The identifier of the operator.
     * @param name The (unique) name for the state.
     * @param typeInfo The type of the elements in the state.
     * @param serializer The serializer used to write the elements into state.
     * @param <T> The type of the values that are in the list state.
     * @return A {@code DataStream} representing the elements in state.
     * @throws IOException If the savepoint path is invalid or the uid does not exist.
     */
    public <T> DataStream<T> readListState(
            OperatorIdentifier identifier,
            String name,
            TypeInformation<T> typeInfo,
            TypeSerializer<T> serializer)
            throws IOException {
        return readListState(identifier, typeInfo, new ListStateDescriptor<>(name, serializer));
    }

    private <T> DataStream<T> readListState(
            OperatorIdentifier identifier,
            TypeInformation<T> typeInfo,
            ListStateDescriptor<T> descriptor)
            throws IOException {

        OperatorState operatorState = metadata.getOperatorState(identifier);
        ListStateInputFormat<T> inputFormat =
                new ListStateInputFormat<>(
                        operatorState,
                        MutableConfig.of(env.getConfiguration()),
                        stateBackend,
                        descriptor);
        return SourceBuilder.fromFormat(env, inputFormat, typeInfo);
    }

    /** @deprecated use {@link #readUnionState(OperatorIdentifier, String, TypeInformation)} */
    @Deprecated
    public <T> DataStream<T> readUnionState(String uid, String name, TypeInformation<T> typeInfo)
            throws IOException {
        return readListState(OperatorIdentifier.forUid(uid), name, typeInfo);
    }

    /**
     * Read operator {@code UnionState} from a {@code Savepoint}.
     *
     * @param identifier The identifier of the operator.
     * @param name The (unique) name for the state.
     * @param typeInfo The type of the elements in the state.
     * @param <T> The type of the values that are in the union state.
     * @return A {@code DataStream} representing the elements in state.
     * @throws IOException If the savepoint path is invalid or the uid does not exist.
     */
    public <T> DataStream<T> readUnionState(
            OperatorIdentifier identifier, String name, TypeInformation<T> typeInfo)
            throws IOException {
        return readUnionState(identifier, typeInfo, new ListStateDescriptor<>(name, typeInfo));
    }

    /**
     * @deprecated use {@link #readUnionState(OperatorIdentifier, String, TypeInformation,
     *     TypeSerializer)}
     */
    public <T> DataStream<T> readUnionState(
            String uid, String name, TypeInformation<T> typeInfo, TypeSerializer<T> serializer)
            throws IOException {
        return readUnionState(OperatorIdentifier.forUid(uid), name, typeInfo, serializer);
    }

    /**
     * Read operator {@code UnionState} from a {@code Savepoint} when a custom serializer was used;
     * e.g., a different serializer than the one returned by {@code
     * TypeInformation#createSerializer}.
     *
     * @param identifier The identifier of the operator.
     * @param name The (unique) name for the state.
     * @param typeInfo The type of the elements in the state.
     * @param serializer The serializer used to write the elements into state.
     * @param <T> The type of the values that are in the union state.
     * @return A {@code DataStream} representing the elements in state.
     * @throws IOException If the savepoint path is invalid or the uid does not exist.
     */
    public <T> DataStream<T> readUnionState(
            OperatorIdentifier identifier,
            String name,
            TypeInformation<T> typeInfo,
            TypeSerializer<T> serializer)
            throws IOException {
        return readUnionState(identifier, typeInfo, new ListStateDescriptor<>(name, serializer));
    }

    private <T> DataStream<T> readUnionState(
            OperatorIdentifier identifier,
            TypeInformation<T> typeInfo,
            ListStateDescriptor<T> descriptor)
            throws IOException {

        OperatorState operatorState = metadata.getOperatorState(identifier);
        UnionStateInputFormat<T> inputFormat =
                new UnionStateInputFormat<>(
                        operatorState,
                        MutableConfig.of(env.getConfiguration()),
                        stateBackend,
                        descriptor);
        return SourceBuilder.fromFormat(env, inputFormat, typeInfo);
    }

    /**
     * @deprecated use {@link #readBroadcastState(OperatorIdentifier, String, TypeInformation,
     *     TypeInformation)}
     */
    @Deprecated
    public <K, V> DataStream<Tuple2<K, V>> readBroadcastState(
            String uid,
            String name,
            TypeInformation<K> keyTypeInfo,
            TypeInformation<V> valueTypeInfo)
            throws IOException {
        return readBroadcastState(OperatorIdentifier.forUid(uid), name, keyTypeInfo, valueTypeInfo);
    }

    /**
     * Read operator {@code BroadcastState} from a {@code Savepoint}.
     *
     * @param identifier The identifier of the operator.
     * @param name The (unique) name for the state.
     * @param keyTypeInfo The type information for the keys in the state.
     * @param valueTypeInfo The type information for the values in the state.
     * @param <K> The type of keys in state.
     * @param <V> The type of values in state.
     * @return A {@code DataStream} of key-value pairs from state.
     * @throws IOException If the savepoint does not contain the specified uid.
     */
    public <K, V> DataStream<Tuple2<K, V>> readBroadcastState(
            OperatorIdentifier identifier,
            String name,
            TypeInformation<K> keyTypeInfo,
            TypeInformation<V> valueTypeInfo)
            throws IOException {
        return readBroadcastState(
                identifier,
                keyTypeInfo,
                valueTypeInfo,
                new MapStateDescriptor<>(name, keyTypeInfo, valueTypeInfo));
    }

    /**
     * @deprecated use {@link #readBroadcastState(OperatorIdentifier, String, TypeInformation,
     *     TypeInformation, TypeSerializer, TypeSerializer)}
     */
    @Deprecated
    public <K, V> DataStream<Tuple2<K, V>> readBroadcastState(
            String uid,
            String name,
            TypeInformation<K> keyTypeInfo,
            TypeInformation<V> valueTypeInfo,
            TypeSerializer<K> keySerializer,
            TypeSerializer<V> valueSerializer)
            throws IOException {
        return readBroadcastState(
                OperatorIdentifier.forUid(uid),
                name,
                keyTypeInfo,
                valueTypeInfo,
                keySerializer,
                valueSerializer);
    }

    /**
     * Read operator {@code BroadcastState} from a {@code Savepoint} when a custom serializer was
     * used; e.g., a different serializer than the one returned by {@code
     * TypeInformation#createSerializer}.
     *
     * @param identifier The identifier of the operator.
     * @param name The (unique) name for the state.
     * @param keyTypeInfo The type information for the keys in the state.
     * @param valueTypeInfo The type information for the values in the state.
     * @param keySerializer The type serializer used to write keys into the state.
     * @param valueSerializer The type serializer used to write values into the state.
     * @param <K> The type of keys in state.
     * @param <V> The type of values in state.
     * @return A {@code DataStream} of key-value pairs from state.
     * @throws IOException If the savepoint path is invalid or the uid does not exist.
     */
    public <K, V> DataStream<Tuple2<K, V>> readBroadcastState(
            OperatorIdentifier identifier,
            String name,
            TypeInformation<K> keyTypeInfo,
            TypeInformation<V> valueTypeInfo,
            TypeSerializer<K> keySerializer,
            TypeSerializer<V> valueSerializer)
            throws IOException {
        return readBroadcastState(
                identifier,
                keyTypeInfo,
                valueTypeInfo,
                new MapStateDescriptor<>(name, keySerializer, valueSerializer));
    }

    private <K, V> DataStream<Tuple2<K, V>> readBroadcastState(
            OperatorIdentifier identifier,
            TypeInformation<K> keyTypeInfo,
            TypeInformation<V> valueTypeInfo,
            MapStateDescriptor<K, V> descriptor)
            throws IOException {

        OperatorState operatorState = metadata.getOperatorState(identifier);
        BroadcastStateInputFormat<K, V> inputFormat =
                new BroadcastStateInputFormat<>(
                        operatorState,
                        MutableConfig.of(env.getConfiguration()),
                        stateBackend,
                        descriptor);
        return SourceBuilder.fromFormat(
                env, inputFormat, new TupleTypeInfo<>(keyTypeInfo, valueTypeInfo));
    }

    /** @deprecated use {@link #readKeyedState(OperatorIdentifier, KeyedStateReaderFunction)} */
    @Deprecated
    public <K, OUT> DataStream<OUT> readKeyedState(
            String uid, KeyedStateReaderFunction<K, OUT> function) throws IOException {
        return readKeyedState(OperatorIdentifier.forUid(uid), function);
    }

    /**
     * Read keyed state from an operator in a {@code Savepoint}.
     *
     * @param identifier The identifier of the operator.
     * @param function The {@link KeyedStateReaderFunction} that is called for each key in state.
     * @param <K> The type of the key in state.
     * @param <OUT> The output type of the transform function.
     * @return A {@code DataStream} of objects read from keyed state.
     * @throws IOException If the savepoint does not contain operator state with the given uid.
     */
    public <K, OUT> DataStream<OUT> readKeyedState(
            OperatorIdentifier identifier, KeyedStateReaderFunction<K, OUT> function)
            throws IOException {

        TypeInformation<K> keyTypeInfo;
        TypeInformation<OUT> outType;

        try {
            keyTypeInfo =
                    TypeExtractor.createTypeInfo(
                            KeyedStateReaderFunction.class, function.getClass(), 0, null, null);
        } catch (InvalidTypesException e) {
            throw new InvalidProgramException(
                    "The key type of the KeyedStateReaderFunction could not be automatically determined. Please use "
                            + "Savepoint#readKeyedState(String, KeyedStateReaderFunction, TypeInformation, TypeInformation) instead.",
                    e);
        }

        try {
            outType =
                    TypeExtractor.getUnaryOperatorReturnType(
                            function,
                            KeyedStateReaderFunction.class,
                            0,
                            1,
                            TypeExtractor.NO_INDEX,
                            keyTypeInfo,
                            Utils.getCallLocationName(),
                            false);
        } catch (InvalidTypesException e) {
            throw new InvalidProgramException(
                    "The output type of the KeyedStateReaderFunction could not be automatically determined. Please use "
                            + "Savepoint#readKeyedState(String, KeyedStateReaderFunction, TypeInformation, TypeInformation) instead.",
                    e);
        }

        return readKeyedState(identifier, function, keyTypeInfo, outType);
    }

    /**
     * @deprecated use {@link #readKeyedState(OperatorIdentifier, KeyedStateReaderFunction,
     *     TypeInformation, TypeInformation)}
     */
    @Deprecated
    public <K, OUT> DataStream<OUT> readKeyedState(
            String uid,
            KeyedStateReaderFunction<K, OUT> function,
            TypeInformation<K> keyTypeInfo,
            TypeInformation<OUT> outTypeInfo)
            throws IOException {
        return readKeyedState(OperatorIdentifier.forUid(uid), function, keyTypeInfo, outTypeInfo);
    }

    /**
     * Read keyed state from an operator in a {@code Savepoint}.
     *
     * @param identifier The identifier of the operator.
     * @param function The {@link KeyedStateReaderFunction} that is called for each key in state.
     * @param keyTypeInfo The type information of the key in state.
     * @param outTypeInfo The type information of the output of the transform reader function.
     * @param <K> The type of the key in state.
     * @param <OUT> The output type of the transform function.
     * @return A {@code DataStream} of objects read from keyed state.
     * @throws IOException If the savepoint does not contain operator state with the given uid.
     */
    public <K, OUT> DataStream<OUT> readKeyedState(
            OperatorIdentifier identifier,
            KeyedStateReaderFunction<K, OUT> function,
            TypeInformation<K> keyTypeInfo,
            TypeInformation<OUT> outTypeInfo)
            throws IOException {

        OperatorState operatorState = metadata.getOperatorState(identifier);
        KeyedStateInputFormat<K, VoidNamespace, OUT> inputFormat =
                new KeyedStateInputFormat<>(
                        operatorState,
                        stateBackend,
                        MutableConfig.of(env.getConfiguration()),
                        new KeyedStateReaderOperator<>(function, keyTypeInfo));

        return SourceBuilder.fromFormat(env, inputFormat, outTypeInfo);
    }

    /**
     * Read window state from an operator in a {@code Savepoint}. This method supports reading from
     * any type of window.
     *
     * @param assigner The {@link WindowAssigner} used to write out the operator.
     * @return A {@link WindowSavepointReader}.
     */
    public <W extends Window> WindowSavepointReader<W> window(WindowAssigner<?, W> assigner) {
        Preconditions.checkNotNull(assigner, "The window assigner must not be null");
        TypeSerializer<W> windowSerializer = assigner.getWindowSerializer(env.getConfig());
        return window(windowSerializer);
    }

    /**
     * Read window state from an operator in a {@code Savepoint}. This method supports reading from
     * any type of window.
     *
     * @param windowSerializer The serializer used for the window type.
     * @return A {@link WindowSavepointReader}.
     */
    public <W extends Window> WindowSavepointReader<W> window(TypeSerializer<W> windowSerializer) {
        Preconditions.checkNotNull(windowSerializer, "The window serializer must not be null");
        return new WindowSavepointReader<>(env, metadata, stateBackend, windowSerializer);
    }
}
