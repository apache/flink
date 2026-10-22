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
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.v2.internal.InternalPartitionedState;

import org.forstdb.RocksDB;
import org.forstdb.RocksIterator;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.state.forst.ForStIterateOperation.CACHE_SIZE_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;

/** Test for {@link ForStIterateOperation}. */
class ForStDBIterateOperationTest extends ForStDBOperationTestBase {

    @Test
    void testIterateValues() throws Exception {
        ForStMapState<Integer, VoidNamespace, String, String> mapState =
                buildForStMapState("map-iter");
        prepareData(10, mapState, db);
        TestAsyncFuture<StateIterator<String>> future = new TestAsyncFuture<>();
        List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchIterRequest = new ArrayList<>();
        ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(1);
        ForStDBIterRequest<Integer, VoidNamespace, String, String, String> request1 =
                new ForStDBMapValueIterRequest<>(contextKey, mapState, null, null, future);
        batchIterRequest.add(request1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStIterateOperation iterOperation =
                new ForStIterateOperation(db, batchIterRequest, executor);
        iterOperation.process().get();

        StateIterator<String> iterator = future.getCompletedResult();
        AtomicInteger count = new AtomicInteger(0);
        iterator.onNext(
                val -> {
                    assertThat(val).isEqualTo("val-" + count.getAndIncrement());
                });
        assertThat(count.get()).isEqualTo(10);
        assertThat(iterator.isEmpty()).isFalse();
    }

    @Test
    void testIterateKeys() throws Exception {
        ForStMapState<Integer, VoidNamespace, String, String> mapState =
                buildForStMapState("map-iter");
        prepareData(13, mapState, db);
        TestAsyncFuture<StateIterator<String>> future = new TestAsyncFuture<>();
        List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchIterRequest = new ArrayList<>();
        ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(1);
        ForStDBIterRequest<Integer, VoidNamespace, String, String, String> request1 =
                new ForStDBMapKeyIterRequest<>(contextKey, mapState, null, null, future);
        batchIterRequest.add(request1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStIterateOperation iterOperation =
                new ForStIterateOperation(db, batchIterRequest, executor);
        iterOperation.process().get();

        StateIterator<String> iterator = future.getCompletedResult();
        AtomicInteger count = new AtomicInteger(0);
        iterator.onNext(
                val -> {
                    assertThat(val).isEqualTo("uk-" + count.getAndIncrement());
                });
        assertThat(count.get()).isEqualTo(13);
        assertThat(iterator.isEmpty()).isFalse();
    }

    @Test
    void testIterateEntries() throws Exception {
        ForStMapState<Integer, VoidNamespace, String, String> mapState =
                buildForStMapState("map-iter");
        prepareData(3, mapState, db);
        TestAsyncFuture<StateIterator<Map.Entry<String, String>>> future = new TestAsyncFuture<>();
        List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchIterRequest = new ArrayList<>();
        ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(1);
        ForStDBIterRequest<Integer, VoidNamespace, String, String, Map.Entry<String, String>>
                request1 =
                        new ForStDBMapEntryIterRequest<>(contextKey, mapState, null, null, future);
        batchIterRequest.add(request1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStIterateOperation iterOperation =
                new ForStIterateOperation(db, batchIterRequest, executor);
        iterOperation.process().get();

        StateIterator<Map.Entry<String, String>> iterator = future.getCompletedResult();
        AtomicInteger count = new AtomicInteger(0);
        iterator.onNext(
                entry -> {
                    int cnt = count.getAndIncrement();
                    assertThat(entry.getKey()).isEqualTo("uk-" + cnt);
                    assertThat(entry.getValue()).isEqualTo("val-" + cnt);
                });
        assertThat(count.get()).isEqualTo(3);
        assertThat(iterator.isEmpty()).isFalse();
    }

    @Test
    void testIteratorLoading() throws Exception {
        ForStMapState<Integer, VoidNamespace, String, String> mapState =
                buildForStMapState("map-iter");
        prepareData(200, mapState, db);
        TestAsyncFuture<StateIterator<Map.Entry<String, String>>> future = new TestAsyncFuture<>();
        List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchIterRequest = new ArrayList<>();
        ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(1);
        MockStateRequestHandler stateRequestHandler = new MockStateRequestHandler();
        ForStDBIterRequest<Integer, VoidNamespace, String, String, Map.Entry<String, String>>
                request1 =
                        new ForStDBMapEntryIterRequest<>(
                                contextKey, mapState, stateRequestHandler, null, future);
        batchIterRequest.add(request1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStIterateOperation iterOperation =
                new ForStIterateOperation(db, batchIterRequest, executor);
        iterOperation.process().get();

        StateIterator<Map.Entry<String, String>> iterator = future.getCompletedResult();
        AtomicInteger count = new AtomicInteger(0);
        try {
            iterator.onNext(
                    entry -> {
                        int cnt = count.getAndIncrement();
                        assertThat(entry.getKey()).isEqualTo("uk-" + cnt);
                        assertThat(entry.getValue()).isEqualTo("val-" + cnt);
                    });
            fail("should throw NPE");
        } catch (NullPointerException npe) {
            assertThat(stateRequestHandler.payload).isNotNull();
            assertThat(count.get()).isEqualTo(CACHE_SIZE_LIMIT);
            Tuple2<StateRequestType, RocksIterator> tuple =
                    (Tuple2<StateRequestType, RocksIterator>) stateRequestHandler.payload;
            assertThat(tuple.f0).isEqualTo(StateRequestType.MAP_ITER);
            TestAsyncFuture<StateIterator<Map.Entry<String, String>>> future2 =
                    new TestAsyncFuture<>();
            ForStDBIterRequest<Integer, VoidNamespace, String, String, Map.Entry<String, String>>
                    request2 =
                            new ForStDBMapEntryIterRequest(
                                    contextKey, mapState, null, tuple.f1, future2);
            batchIterRequest.clear();
            batchIterRequest.add(request2);
            ForStIterateOperation iterOperation2 =
                    new ForStIterateOperation(db, batchIterRequest, executor);
            iterOperation2.process().get();
            StateIterator<Map.Entry<String, String>> iterator2 = future2.getCompletedResult();
            iterator2.onNext(
                    entry -> {
                        int cnt = count.getAndIncrement();
                        assertThat(entry.getKey()).isEqualTo("uk-" + cnt);
                        assertThat(entry.getValue()).isEqualTo("val-" + cnt);
                    });
            assertThat(count.get()).isEqualTo(200);
            assertThat(iterator2.isEmpty()).isFalse();
        }
    }

    /**
     * A failing iterate request must close its native iterator. {@link org.forstdb.RocksIterator}
     * has no Cleaner and no finalizer, so an iterator that is not explicitly closed leaks its
     * native allocation for the lifetime of the JVM.
     *
     * <p>This drives the catch path: {@code process()} creates and seeks the iterator, fills a full
     * cache batch (so the iterator is deliberately retained for a follow-up request) and then
     * throws while deserializing.
     */
    @Test
    void testIteratorClosedWhenProcessFails() throws Exception {
        ForStMapState<Integer, VoidNamespace, String, String> mapState =
                buildForStMapState("map-iter");
        // More than CACHE_SIZE_LIMIT entries, so process() retains a live iterator when it fails.
        prepareData(CACHE_SIZE_LIMIT + 10, mapState, db);

        TestIterRequest request = new TestIterRequest(buildContextKey(1), mapState, null, true);
        List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchRequest = new ArrayList<>();
        batchRequest.add(request);

        ForStIterateOperation iterOperation =
                new ForStIterateOperation(db, batchRequest, Runnable::run);
        assertThatThrownBy(() -> iterOperation.process().get())
                .hasRootCauseInstanceOf(FailingDeserializationException.class);

        assertThat(request.observedIterator)
                .as("process() should have created an iterator before failing")
                .isNotNull();
        assertThat(request.closeCount).hasValue(1);
        assertThat(request.observedIterator.isOwningHandle())
                .as("native iterator handle should have been released")
                .isFalse();
        assertThat(request.rocksIterator).isNull();
    }

    /**
     * A request that is failed because an earlier request in the same batch failed must also close
     * its iterator. An {@code ITERATOR_LOADING} continuation carries a live iterator from a
     * previous partial iteration, so this path leaks too even though the request is never
     * processed.
     */
    @Test
    void testIteratorClosedWhenBatchAlreadyFailed() throws Exception {
        ForStMapState<Integer, VoidNamespace, String, String> mapState =
                buildForStMapState("map-iter");
        prepareData(CACHE_SIZE_LIMIT + 10, mapState, db);

        // A same-thread executor makes ordering deterministic: the first request fails and sets
        // the shared error, so the continuation takes the "already failed" branch.
        TestIterRequest failing = new TestIterRequest(buildContextKey(1), mapState, null, true);
        RocksIterator carriedIterator = db.newIterator(mapState.getColumnFamilyHandle());
        TestIterRequest continuation =
                new TestIterRequest(buildContextKey(1), mapState, carriedIterator, false);

        List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchRequest = new ArrayList<>();
        batchRequest.add(failing);
        batchRequest.add(continuation);

        ForStIterateOperation iterOperation =
                new ForStIterateOperation(db, batchRequest, Runnable::run);
        assertThatThrownBy(() -> iterOperation.process().get())
                .hasRootCauseInstanceOf(FailingDeserializationException.class);

        assertThat(continuation.completedExceptionally)
                .as("the continuation should have been failed without being processed")
                .doesNotHaveValue(null);
        assertThat(continuation.closeCount).hasValue(1);
        assertThat(carriedIterator.isOwningHandle())
                .as("carried-over native iterator handle should have been released")
                .isFalse();
        assertThat(continuation.rocksIterator).isNull();
    }

    private void prepareData(
            int num, ForStMapState<Integer, VoidNamespace, String, String> mapState, RocksDB db)
            throws Exception {
        for (int i = 0; i < num; i++) {
            ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(1);
            contextKey.setUserKey("uk-" + i);
            String value = "val-" + i;
            byte[] keyBytes = mapState.serializeKey(contextKey);
            byte[] valueBytes = mapState.serializeValue(value);
            db.put(mapState.getColumnFamilyHandle(), keyBytes, valueBytes);
        }
    }

    /** Thrown by {@link TestIterRequest} to drive the iterate failure paths. */
    private static class FailingDeserializationException extends RuntimeException {
        FailingDeserializationException() {
            super("Injected deserialization failure.");
        }
    }

    /**
     * An iterate request that records {@link #close()} calls and captures the native iterator that
     * {@link ForStDBIterRequest#process} created, so a test can assert the handle was released.
     */
    private static class TestIterRequest
            extends ForStDBIterRequest<Integer, VoidNamespace, String, String, String> {

        private final boolean failOnDeserialize;

        final AtomicInteger closeCount = new AtomicInteger();
        final AtomicReference<Throwable> completedExceptionally = new AtomicReference<>();
        volatile RocksIterator observedIterator;

        TestIterRequest(
                ContextKey<Integer, VoidNamespace> contextKey,
                ForStMapState<Integer, VoidNamespace, String, String> table,
                @Nullable RocksIterator rocksIterator,
                boolean failOnDeserialize) {
            super(contextKey, table, null, rocksIterator);
            this.failOnDeserialize = failOnDeserialize;
            this.observedIterator = rocksIterator;
        }

        @Override
        public void completeStateFutureExceptionally(String message, Throwable ex) {
            completedExceptionally.set(ex);
        }

        @Override
        public Collection<String> deserializeElement(List<RawEntry> entries, int userKeyOffset) {
            // Capture the iterator while process() still holds it, before any close() nulls it.
            observedIterator = rocksIterator;
            if (failOnDeserialize) {
                throw new FailingDeserializationException();
            }
            return Collections.emptyList();
        }

        @Override
        public void buildIteratorAndCompleteFuture(
                Collection<String> partialResult, boolean encounterEnd) {}

        @Override
        public void close() throws IOException {
            closeCount.incrementAndGet();
            super.close();
        }
    }

    private static class MockStateRequestHandler implements StateRequestHandler {
        Object payload = null;

        @Override
        public <IN, OUT> InternalAsyncFuture<OUT> handleRequest(
                @Nullable State state, StateRequestType type, @Nullable IN payload) {
            assertThat(type).isEqualTo(StateRequestType.ITERATOR_LOADING);
            this.payload = payload;
            return null;
        }

        @Override
        public <IN, OUT> OUT handleRequestSync(
                State state, StateRequestType type, @Nullable IN payload) {
            return null;
        }

        @Override
        public <N> void setCurrentNamespaceForState(
                @Nonnull InternalPartitionedState<N> state, N namespace) {
            state.setCurrentNamespace(namespace);
        }
    }
}
