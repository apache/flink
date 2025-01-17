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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.v2.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.state.InternalStateFuture;
import org.apache.flink.runtime.asyncprocessing.EpochManager.Epoch;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.v2.internal.InternalPartitionedState;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.forstdb.ColumnFamilyDescriptor;
import org.forstdb.ColumnFamilyHandle;
import org.forstdb.ColumnFamilyOptions;
import org.forstdb.RocksDB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Base class for {@link ForStDBOperation} tests. */
public class ForStDBOperationTestBase {
    @TempDir private Path tmpDbDir;
    protected RocksDB db;

    protected StateRequestHandler stateRequestHandler;

    @BeforeEach
    public void setUp() throws Exception {
        db = RocksDB.open(tmpDbDir.toAbsolutePath().toString());
        stateRequestHandler = buildMockStateRequestHandler();
    }

    @AfterEach
    public void tearDown() {
        if (db != null) {
            db.close();
        }
    }

    protected ColumnFamilyHandle createColumnFamilyHandle(String columnFamilyName)
            throws Exception {
        byte[] nameBytes = columnFamilyName.getBytes(ConfigConstants.DEFAULT_CHARSET);
        ColumnFamilyDescriptor columnFamilyDescriptor =
                new ColumnFamilyDescriptor(
                        nameBytes,
                        ForStOperationUtils.createColumnFamilyOptions(
                                (e) -> new ColumnFamilyOptions(), columnFamilyName));
        return db.createColumnFamily(columnFamilyDescriptor);
    }

    StateRequestHandler buildMockStateRequestHandler() {

        return new StateRequestHandler() {
            @Override
            public <IN, OUT> InternalStateFuture<OUT> handleRequest(
                    @Nullable State state, StateRequestType type, @Nullable IN payload) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <IN, OUT> OUT handleRequestSync(
                    State state,
                    StateRequestType type,
                    @org.jetbrains.annotations.Nullable IN payload) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <N> void setCurrentNamespaceForState(
                    @Nonnull InternalPartitionedState<N> state, N namespace) {
                throw new UnsupportedOperationException();
            }
        };
    }

    protected ContextKey<Integer, VoidNamespace> buildContextKey(int i) {
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(i, 128);
        RecordContext<Integer> recordContext =
                new RecordContext<>(i, i, t -> {}, keyGroup, new Epoch(0), 0);
        return new ContextKey<>(recordContext, VoidNamespace.INSTANCE, null);
    }

    protected ForStValueState<Integer, VoidNamespace, String> buildForStValueState(String stateName)
            throws Exception {
        ColumnFamilyHandle cf = createColumnFamilyHandle(stateName);
        ValueStateDescriptor<String> valueStateDescriptor =
                new ValueStateDescriptor<>(stateName, StringSerializer.INSTANCE);
        Supplier<SerializedCompositeKeyBuilder<Integer>> serializedKeyBuilder =
                () -> new SerializedCompositeKeyBuilder<>(IntSerializer.INSTANCE, 2, 32);
        Supplier<DataOutputSerializer> valueSerializerView = () -> new DataOutputSerializer(32);
        Supplier<DataInputDeserializer> valueDeserializerView =
                () -> new DataInputDeserializer(new byte[128]);
        return new ForStValueState<>(
                stateRequestHandler,
                cf,
                valueStateDescriptor,
                serializedKeyBuilder,
                VoidNamespace.INSTANCE,
                () -> VoidNamespaceSerializer.INSTANCE,
                valueSerializerView,
                valueDeserializerView);
    }

    protected ForStListState<Integer, VoidNamespace, String> buildForStListState(String stateName)
            throws Exception {
        ColumnFamilyHandle cf = createColumnFamilyHandle(stateName);
        ListStateDescriptor<String> valueStateDescriptor =
                new ListStateDescriptor<>(stateName, StringSerializer.INSTANCE);
        Supplier<SerializedCompositeKeyBuilder<Integer>> serializedKeyBuilder =
                () -> new SerializedCompositeKeyBuilder<>(IntSerializer.INSTANCE, 2, 32);
        Supplier<DataOutputSerializer> valueSerializerView = () -> new DataOutputSerializer(32);
        Supplier<DataInputDeserializer> valueDeserializerView =
                () -> new DataInputDeserializer(new byte[128]);
        return new ForStListState<>(
                buildMockStateRequestHandler(),
                cf,
                valueStateDescriptor,
                serializedKeyBuilder,
                VoidNamespace.INSTANCE,
                () -> VoidNamespaceSerializer.INSTANCE,
                valueSerializerView,
                valueDeserializerView);
    }

    protected ForStAggregatingState<String, ?, Integer, Integer, Integer>
            buildForStSumAggregateState(String stateName) throws Exception {
        ColumnFamilyHandle cf = createColumnFamilyHandle(stateName);
        AggregatingStateDescriptor<Integer, Integer, Integer> valueStateDescriptor =
                new AggregatingStateDescriptor<>(
                        stateName,
                        new AggregateFunction<Integer, Integer, Integer>() {
                            @Override
                            public Integer createAccumulator() {
                                return 0;
                            }

                            @Override
                            public Integer add(Integer value, Integer accumulator) {
                                return value + accumulator;
                            }

                            @Override
                            public Integer getResult(Integer accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Integer merge(Integer a, Integer b) {
                                return a + b;
                            }
                        },
                        IntSerializer.INSTANCE);
        Supplier<SerializedCompositeKeyBuilder<String>> serializedKeyBuilder =
                () -> new SerializedCompositeKeyBuilder<>(StringSerializer.INSTANCE, 2, 32);
        Supplier<DataOutputSerializer> valueSerializerView = () -> new DataOutputSerializer(32);
        Supplier<DataInputDeserializer> valueDeserializerView =
                () -> new DataInputDeserializer(new byte[128]);

        return new ForStAggregatingState<>(
                valueStateDescriptor,
                buildMockStateRequestHandler(),
                cf,
                serializedKeyBuilder,
                VoidNamespace.INSTANCE,
                () -> VoidNamespaceSerializer.INSTANCE,
                valueSerializerView,
                valueDeserializerView);
    }

    protected ForStMapState<Integer, VoidNamespace, String, String> buildForStMapState(
            String stateName) throws Exception {
        ColumnFamilyHandle cf = createColumnFamilyHandle(stateName);
        MapStateDescriptor<String, String> mapStateDescriptor =
                new MapStateDescriptor<>(
                        stateName, StringSerializer.INSTANCE, StringSerializer.INSTANCE);
        Supplier<SerializedCompositeKeyBuilder<Integer>> serializedKeyBuilder =
                () -> new SerializedCompositeKeyBuilder<>(IntSerializer.INSTANCE, 2, 32);
        Supplier<DataOutputSerializer> valueSerializerView = () -> new DataOutputSerializer(32);
        Supplier<DataInputDeserializer> keyDeserializerView =
                () -> new DataInputDeserializer(new byte[128]);
        Supplier<DataInputDeserializer> valueDeserializerView =
                () -> new DataInputDeserializer(new byte[128]);
        return new ForStMapState<>(
                stateRequestHandler,
                cf,
                mapStateDescriptor,
                serializedKeyBuilder,
                VoidNamespace.INSTANCE,
                () -> VoidNamespaceSerializer.INSTANCE,
                valueSerializerView,
                keyDeserializerView,
                valueDeserializerView,
                1);
    }

    static class TestStateFuture<T> implements InternalStateFuture<T> {

        public CompletableFuture<T> future = new CompletableFuture<>();

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public T get() {
            T t;
            try {
                t = future.get();
            } catch (Exception e) {
                throw new RuntimeException("Error while getting future's result.", e);
            }
            return t;
        }

        @Override
        public void complete(T result) {
            future.complete(result);
        }

        @Override
        public void completeExceptionally(String message, Throwable ex) {
            // do nothing
        }

        @Override
        public void thenSyncAccept(ThrowingConsumer<? super T, ? extends Exception> action) {
            throw new UnsupportedOperationException();
        }

        public T getCompletedResult() throws Exception {
            return future.get();
        }

        @Override
        public <U> StateFuture<U> thenApply(
                FunctionWithException<? super T, ? extends U, ? extends Exception> fn) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StateFuture<Void> thenAccept(
                ThrowingConsumer<? super T, ? extends Exception> action) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <U> StateFuture<U> thenCompose(
                FunctionWithException<? super T, ? extends StateFuture<U>, ? extends Exception>
                        action) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <U, V> StateFuture<V> thenCombine(
                StateFuture<? extends U> other,
                BiFunctionWithException<? super T, ? super U, ? extends V, ? extends Exception>
                        fn) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <U, V> StateFuture<Tuple2<Boolean, Object>> thenConditionallyApply(
                FunctionWithException<? super T, Boolean, ? extends Exception> condition,
                FunctionWithException<? super T, ? extends U, ? extends Exception> actionIfTrue,
                FunctionWithException<? super T, ? extends V, ? extends Exception> actionIfFalse) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <U> StateFuture<Tuple2<Boolean, U>> thenConditionallyApply(
                FunctionWithException<? super T, Boolean, ? extends Exception> condition,
                FunctionWithException<? super T, ? extends U, ? extends Exception> actionIfTrue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StateFuture<Boolean> thenConditionallyAccept(
                FunctionWithException<? super T, Boolean, ? extends Exception> condition,
                ThrowingConsumer<? super T, ? extends Exception> actionIfTrue,
                ThrowingConsumer<? super T, ? extends Exception> actionIfFalse) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StateFuture<Boolean> thenConditionallyAccept(
                FunctionWithException<? super T, Boolean, ? extends Exception> condition,
                ThrowingConsumer<? super T, ? extends Exception> actionIfTrue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <U, V> StateFuture<Tuple2<Boolean, Object>> thenConditionallyCompose(
                FunctionWithException<? super T, Boolean, ? extends Exception> condition,
                FunctionWithException<? super T, ? extends StateFuture<U>, ? extends Exception>
                        actionIfTrue,
                FunctionWithException<? super T, ? extends StateFuture<V>, ? extends Exception>
                        actionIfFalse) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <U> StateFuture<Tuple2<Boolean, U>> thenConditionallyCompose(
                FunctionWithException<? super T, Boolean, ? extends Exception> condition,
                FunctionWithException<? super T, ? extends StateFuture<U>, ? extends Exception>
                        actionIfTrue) {
            throw new UnsupportedOperationException();
        }
    }
}
