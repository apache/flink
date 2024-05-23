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

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
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
import org.apache.flink.runtime.state.v2.ValueStateDescriptor;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Base class for {@link ForStDBOperation} tests. */
public class ForStDBOperationTestBase {
    @TempDir private Path tmpDbDir;
    protected RocksDB db;

    @BeforeEach
    public void setUp() throws Exception {
        db = RocksDB.open(tmpDbDir.toAbsolutePath().toString());
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
                new ColumnFamilyDescriptor(nameBytes, new ColumnFamilyOptions());
        return db.createColumnFamily(columnFamilyDescriptor);
    }

    StateRequestHandler buildMockStateRequestHandler() {

        return new StateRequestHandler() {
            @Override
            public <IN, OUT> InternalStateFuture<OUT> handleRequest(
                    @Nullable State state, StateRequestType type, @Nullable IN payload) {
                throw new UnsupportedOperationException();
            }
        };
    }

    protected ContextKey<Integer> buildContextKey(int i) {
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(i, 128);
        RecordContext<Integer> recordContext =
                new RecordContext<>(i, i, t -> {}, keyGroup, new Epoch(0));
        return new ContextKey<>(recordContext);
    }

    protected ForStValueState<Integer, String> buildForStValueState(String stateName)
            throws Exception {
        ColumnFamilyHandle cf = createColumnFamilyHandle(stateName);
        ValueStateDescriptor<String> valueStateDescriptor =
                new ValueStateDescriptor<>(stateName, BasicTypeInfo.STRING_TYPE_INFO);
        Supplier<SerializedCompositeKeyBuilder<Integer>> serializedKeyBuilder =
                () -> new SerializedCompositeKeyBuilder<>(IntSerializer.INSTANCE, 2, 32);
        Supplier<DataOutputSerializer> valueSerializerView = () -> new DataOutputSerializer(32);
        Supplier<DataInputDeserializer> valueDeserializerView =
                () -> new DataInputDeserializer(new byte[128]);
        return new ForStValueState<>(
                buildMockStateRequestHandler(),
                cf,
                valueStateDescriptor,
                serializedKeyBuilder,
                valueSerializerView,
                valueDeserializerView);
    }

    static class TestStateFuture<T> implements InternalStateFuture<T> {

        public CompletableFuture<T> future = new CompletableFuture<>();

        @Override
        public void complete(T result) {
            future.complete(result);
        }

        @Override
        public void completeExceptionally(String message, Throwable ex) {
            throw new UnsupportedOperationException();
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
    }
}
