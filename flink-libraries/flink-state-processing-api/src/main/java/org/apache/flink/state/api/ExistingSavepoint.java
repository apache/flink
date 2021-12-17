/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.api.input.BroadcastStateInputFormat;
import org.apache.flink.state.api.input.KeyedStateInputFormat;
import org.apache.flink.state.api.input.ListStateInputFormat;
import org.apache.flink.state.api.input.UnionStateInputFormat;
import org.apache.flink.state.api.input.operator.KeyedStateReaderOperator;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadata;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * An existing savepoint. This class provides the entry points for reading previous existing
 * operator states in savepoints. Operator states can be removed from and added to the set of
 * existing operator states, and eventually, written to distributed storage as a new savepoint.
 *
 * <p>New savepoints written using this class are based on the previous existing savepoint. This
 * means that for existing operators that remain untouched, the new savepoint only contains a
 * shallow copy of pointers to state data that resides in the previous existing savepoint paths.
 * This means that both savepoints share state and one cannot be deleted without corrupting the
 * other!
 *
 * @see WritableSavepoint
 */
@PublicEvolving
@SuppressWarnings("WeakerAccess")
public class ExistingSavepoint extends WritableSavepoint<ExistingSavepoint> {

    /** The batch execution environment. Used for creating inputs for reading state. */
    private final ExecutionEnvironment env;

    /**
     * The savepoint metadata, which maintains the current set of existing / newly added operator
     * states.
     */
    private final SavepointMetadata metadata;

    /**
     * The state backend that was previously used to write existing operator states in this
     * savepoint. This is also the state backend that will be used when writing again this existing
     * savepoint.
     */
    private final StateBackend stateBackend;

    ExistingSavepoint(
            ExecutionEnvironment env, SavepointMetadata metadata, StateBackend stateBackend)
            throws IOException {
        super(metadata, stateBackend);
        Preconditions.checkNotNull(env, "The execution environment must not be null");
        Preconditions.checkNotNull(metadata, "The savepoint metadata must not be null");
        Preconditions.checkNotNull(stateBackend, "The state backend must not be null");

        this.env = env;
        this.metadata = metadata;
        this.stateBackend = stateBackend;
    }

    /**
     * Read operator {@code ListState} from a {@code Savepoint}.
     *
     * @param uid The uid of the operator.
     * @param name The (unique) name for the state.
     * @param typeInfo The type of the elements in the state.
     * @param <T> The type of the values that are in the list state.
     * @return A {@code DataSet} representing the elements in state.
     * @throws IOException If the savepoint path is invalid or the uid does not exist.
     */
    public <T> DataSource<T> readListState(String uid, String name, TypeInformation<T> typeInfo)
            throws IOException {
        OperatorState operatorState = metadata.getOperatorState(uid);
        ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, typeInfo);
        ListStateInputFormat<T> inputFormat = new ListStateInputFormat<>(operatorState, descriptor);
        return env.createInput(inputFormat, typeInfo);
    }

    /**
     * Read operator {@code ListState} from a {@code Savepoint} when a custom serializer was used;
     * e.g., a different serializer than the one returned by {@code
     * TypeInformation#createSerializer}.
     *
     * @param uid The uid of the operator.
     * @param name The (unique) name for the state.
     * @param typeInfo The type of the elements in the state.
     * @param serializer The serializer used to write the elements into state.
     * @param <T> The type of the values that are in the list state.
     * @return A {@code DataSet} representing the elements in state.
     * @throws IOException If the savepoint path is invalid or the uid does not exist.
     */
    public <T> DataSource<T> readListState(
            String uid, String name, TypeInformation<T> typeInfo, TypeSerializer<T> serializer)
            throws IOException {

        OperatorState operatorState = metadata.getOperatorState(uid);
        ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, serializer);
        ListStateInputFormat<T> inputFormat = new ListStateInputFormat<>(operatorState, descriptor);
        return env.createInput(inputFormat, typeInfo);
    }

    /**
     * Read operator {@code UnionState} from a {@code Savepoint}.
     *
     * @param uid The uid of the operator.
     * @param name The (unique) name for the state.
     * @param typeInfo The type of the elements in the state.
     * @param <T> The type of the values that are in the union state.
     * @return A {@code DataSet} representing the elements in state.
     * @throws IOException If the savepoint path is invalid or the uid does not exist.
     */
    public <T> DataSource<T> readUnionState(String uid, String name, TypeInformation<T> typeInfo)
            throws IOException {
        OperatorState operatorState = metadata.getOperatorState(uid);
        ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, typeInfo);
        UnionStateInputFormat<T> inputFormat =
                new UnionStateInputFormat<>(operatorState, descriptor);
        return env.createInput(inputFormat, typeInfo);
    }

    /**
     * Read operator {@code UnionState} from a {@code Savepoint} when a custom serializer was used;
     * e.g., a different serializer than the one returned by {@code
     * TypeInformation#createSerializer}.
     *
     * @param uid The uid of the operator.
     * @param name The (unique) name for the state.
     * @param typeInfo The type of the elements in the state.
     * @param serializer The serializer used to write the elements into state.
     * @param <T> The type of the values that are in the union state.
     * @return A {@code DataSet} representing the elements in state.
     * @throws IOException If the savepoint path is invalid or the uid does not exist.
     */
    public <T> DataSource<T> readUnionState(
            String uid, String name, TypeInformation<T> typeInfo, TypeSerializer<T> serializer)
            throws IOException {

        OperatorState operatorState = metadata.getOperatorState(uid);
        ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, serializer);
        UnionStateInputFormat<T> inputFormat =
                new UnionStateInputFormat<>(operatorState, descriptor);
        return env.createInput(inputFormat, typeInfo);
    }

    /**
     * Read operator {@code BroadcastState} from a {@code Savepoint}.
     *
     * @param uid The uid of the operator.
     * @param name The (unique) name for the state.
     * @param keyTypeInfo The type information for the keys in the state.
     * @param valueTypeInfo The type information for the values in the state.
     * @param <K> The type of keys in state.
     * @param <V> The type of values in state.
     * @return A {@code DataSet} of key-value pairs from state.
     * @throws IOException If the savepoint does not contain the specified uid.
     */
    public <K, V> DataSource<Tuple2<K, V>> readBroadcastState(
            String uid,
            String name,
            TypeInformation<K> keyTypeInfo,
            TypeInformation<V> valueTypeInfo)
            throws IOException {

        OperatorState operatorState = metadata.getOperatorState(uid);
        MapStateDescriptor<K, V> descriptor =
                new MapStateDescriptor<>(name, keyTypeInfo, valueTypeInfo);
        BroadcastStateInputFormat<K, V> inputFormat =
                new BroadcastStateInputFormat<>(operatorState, descriptor);
        return env.createInput(inputFormat, new TupleTypeInfo<>(keyTypeInfo, valueTypeInfo));
    }

    /**
     * Read operator {@code BroadcastState} from a {@code Savepoint} when a custom serializer was
     * used; e.g., a different serializer than the one returned by {@code
     * TypeInformation#createSerializer}.
     *
     * @param uid The uid of the operator.
     * @param name The (unique) name for the state.
     * @param keyTypeInfo The type information for the keys in the state.
     * @param valueTypeInfo The type information for the values in the state.
     * @param keySerializer The type serializer used to write keys into the state.
     * @param valueSerializer The type serializer used to write values into the state.
     * @param <K> The type of keys in state.
     * @param <V> The type of values in state.
     * @return A {@code DataSet} of key-value pairs from state.
     * @throws IOException If the savepoint path is invalid or the uid does not exist.
     */
    public <K, V> DataSource<Tuple2<K, V>> readBroadcastState(
            String uid,
            String name,
            TypeInformation<K> keyTypeInfo,
            TypeInformation<V> valueTypeInfo,
            TypeSerializer<K> keySerializer,
            TypeSerializer<V> valueSerializer)
            throws IOException {

        OperatorState operatorState = metadata.getOperatorState(uid);
        MapStateDescriptor<K, V> descriptor =
                new MapStateDescriptor<>(name, keySerializer, valueSerializer);
        BroadcastStateInputFormat<K, V> inputFormat =
                new BroadcastStateInputFormat<>(operatorState, descriptor);
        return env.createInput(inputFormat, new TupleTypeInfo<>(keyTypeInfo, valueTypeInfo));
    }

    /**
     * Read keyed state from an operator in a {@code Savepoint}.
     *
     * @param uid The uid of the operator.
     * @param function The {@link KeyedStateReaderFunction} that is called for each key in state.
     * @param <K> The type of the key in state.
     * @param <OUT> The output type of the transform function.
     * @return A {@code DataSet} of objects read from keyed state.
     * @throws IOException If the savepoint does not contain operator state with the given uid.
     */
    public <K, OUT> DataSource<OUT> readKeyedState(
            String uid, KeyedStateReaderFunction<K, OUT> function) throws IOException {

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

        return readKeyedState(uid, function, keyTypeInfo, outType);
    }

    /**
     * Read keyed state from an operator in a {@code Savepoint}.
     *
     * @param uid The uid of the operator.
     * @param function The {@link KeyedStateReaderFunction} that is called for each key in state.
     * @param keyTypeInfo The type information of the key in state.
     * @param outTypeInfo The type information of the output of the transform reader function.
     * @param <K> The type of the key in state.
     * @param <OUT> The output type of the transform function.
     * @return A {@code DataSet} of objects read from keyed state.
     * @throws IOException If the savepoint does not contain operator state with the given uid.
     */
    public <K, OUT> DataSource<OUT> readKeyedState(
            String uid,
            KeyedStateReaderFunction<K, OUT> function,
            TypeInformation<K> keyTypeInfo,
            TypeInformation<OUT> outTypeInfo)
            throws IOException {

        OperatorState operatorState = metadata.getOperatorState(uid);
        KeyedStateInputFormat<K, VoidNamespace, OUT> inputFormat =
                new KeyedStateInputFormat<>(
                        operatorState,
                        stateBackend,
                        env.getConfiguration(),
                        new KeyedStateReaderOperator<>(function, keyTypeInfo));

        return env.createInput(inputFormat, outTypeInfo);
    }

    /**
     * Read window state from an operator in a {@code Savepoint}. This method supports reading from
     * any type of window.
     *
     * @param assigner The {@link WindowAssigner} used to write out the operator.
     * @return A {@link WindowReader}.
     */
    public <W extends Window> WindowReader<W> window(WindowAssigner<?, W> assigner) {
        Preconditions.checkNotNull(assigner, "The window assigner must not be null");
        TypeSerializer<W> windowSerializer = assigner.getWindowSerializer(env.getConfig());
        return window(windowSerializer);
    }

    /**
     * Read window state from an operator in a {@code Savepoint}. This method supports reading from
     * any type of window.
     *
     * @param windowSerializer The serializer used for the window type.
     * @return A {@link WindowReader}.
     */
    public <W extends Window> WindowReader<W> window(TypeSerializer<W> windowSerializer) {
        Preconditions.checkNotNull(windowSerializer, "The window serializer must not be null");
        return new WindowReader<>(env, metadata, stateBackend, windowSerializer);
    }
}
