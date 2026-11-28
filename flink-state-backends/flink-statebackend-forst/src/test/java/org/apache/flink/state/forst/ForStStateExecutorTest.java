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

import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.asyncprocessing.AsyncRequestContainer;
import org.apache.flink.runtime.asyncprocessing.EpochManager.Epoch;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.v2.AbstractKeyedState;

import org.forstdb.ColumnFamilyHandle;
import org.forstdb.RocksDB;
import org.forstdb.RocksDBException;
import org.forstdb.RocksIterator;
import org.forstdb.WriteOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for {@link ForStStateExecutor}. */
class ForStStateExecutorTest extends ForStDBOperationTestBase {

    private static final int KEY = 1;

    private static final int NO_ERROR = Integer.MAX_VALUE;

    @Test
    @SuppressWarnings("unchecked")
    void testExecuteValueStateRequest() throws Exception {
        ForStStateExecutor forStStateExecutor =
                new ForStStateExecutor(false, false, 3, 1, db, new WriteOptions());
        ForStValueState<Integer, VoidNamespace, String> state1 =
                buildForStValueState("value-state-1");
        ForStValueState<Integer, VoidNamespace, String> state2 =
                buildForStValueState("value-state-2");

        AsyncRequestContainer asyncRequestContainer = forStStateExecutor.createRequestContainer();
        assertTrue(asyncRequestContainer.isEmpty());

        // 1. Update value state: keyRange [0, keyNum)
        int keyNum = 1000;
        for (int i = 0; i < keyNum; i++) {
            ForStValueState<Integer, VoidNamespace, String> state = (i % 2 == 0 ? state1 : state2);
            asyncRequestContainer.offer(
                    buildStateRequest(state, StateRequestType.VALUE_UPDATE, i, "test-" + i, i * 2));
        }

        forStStateExecutor.executeBatchRequests(asyncRequestContainer).get();

        List<StateRequest<?, ?, ?, ?>> checkList = new ArrayList<>();
        asyncRequestContainer = forStStateExecutor.createRequestContainer();
        // 2. Get value state: keyRange [0, keyNum)
        //    Update value state: keyRange [keyNum, keyNum + 100]
        for (int i = 0; i < keyNum; i++) {
            ForStValueState<Integer, VoidNamespace, String> state = (i % 2 == 0 ? state1 : state2);
            StateRequest<?, ?, ?, ?> getRequest =
                    buildStateRequest(state, StateRequestType.VALUE_GET, i, null, i * 2);
            asyncRequestContainer.offer(getRequest);
            checkList.add(getRequest);
        }
        for (int i = keyNum; i < keyNum + 100; i++) {
            ForStValueState<Integer, VoidNamespace, String> state = (i % 2 == 0 ? state1 : state2);
            asyncRequestContainer.offer(
                    buildStateRequest(state, StateRequestType.VALUE_UPDATE, i, "test-" + i, i * 2));
        }
        forStStateExecutor.executeBatchRequests(asyncRequestContainer).get();

        // 3. Check value state Get result : [0, keyNum)
        for (StateRequest<?, ?, ?, ?> getRequest : checkList) {
            assertThat(getRequest.getRequestType()).isEqualTo(StateRequestType.VALUE_GET);
            int key = (Integer) getRequest.getRecordContext().getKey();
            assertThat(getRequest.getRecordContext().getRecord()).isEqualTo(key * 2);
            assertThat(((TestAsyncFuture<String>) getRequest.getFuture()).getCompletedResult())
                    .isEqualTo("test-" + key);
        }

        // 4. Clear value state:  keyRange [keyNum - 100, keyNum)
        //    Update state with null-value : keyRange [keyNum, keyNum + 100]
        asyncRequestContainer = forStStateExecutor.createRequestContainer();
        for (int i = keyNum - 100; i < keyNum; i++) {
            ForStValueState<Integer, VoidNamespace, String> state = (i % 2 == 0 ? state1 : state2);
            asyncRequestContainer.offer(
                    buildStateRequest(state, StateRequestType.CLEAR, i, null, i * 2));
        }
        for (int i = keyNum; i < keyNum + 100; i++) {
            ForStValueState<Integer, VoidNamespace, String> state = (i % 2 == 0 ? state1 : state2);
            asyncRequestContainer.offer(
                    buildStateRequest(state, StateRequestType.VALUE_UPDATE, i, null, i * 2));
        }
        forStStateExecutor.executeBatchRequests(asyncRequestContainer).get();

        // 5. Check that the deleted value is null :  keyRange [keyNum - 100, keyNum + 100)
        asyncRequestContainer = forStStateExecutor.createRequestContainer();
        checkList.clear();
        for (int i = keyNum - 100; i < keyNum + 100; i++) {
            ForStValueState<Integer, VoidNamespace, String> state = (i % 2 == 0 ? state1 : state2);
            StateRequest<?, ?, ?, ?> getRequest =
                    buildStateRequest(state, StateRequestType.VALUE_GET, i, null, i * 2);
            asyncRequestContainer.offer(getRequest);
            checkList.add(getRequest);
        }
        forStStateExecutor.executeBatchRequests(asyncRequestContainer).get();
        for (StateRequest<?, ?, ?, ?> getRequest : checkList) {
            assertThat(getRequest.getRequestType()).isEqualTo(StateRequestType.VALUE_GET);
            assertThat(((TestAsyncFuture<String>) getRequest.getFuture()).getCompletedResult())
                    .isEqualTo(null);
        }
        forStStateExecutor.shutdown();
    }

    @Test
    void testExecuteMapStateRequest() throws Exception {
        ForStStateExecutor forStStateExecutor =
                new ForStStateExecutor(true, false, 3, 1, db, new WriteOptions());
        ForStMapState<Integer, VoidNamespace, String, String> state =
                buildForStMapState("map-state");
        AsyncRequestContainer asyncRequestContainer = forStStateExecutor.createRequestContainer();
        assertTrue(asyncRequestContainer.isEmpty());

        // 1. prepare put data: keyRange [0, 100)
        for (int i = 0; i < 100; i++) {
            asyncRequestContainer.offer(
                    buildMapRequest(
                            state,
                            StateRequestType.MAP_PUT,
                            i,
                            Tuple2.of("uk-" + i, "uv-" + i),
                            i * 2));
        }
        for (int i = 50; i < 100; i++) {
            HashMap<String, String> map = new HashMap<>(100);
            for (int j = 0; j < 50; j++) {
                map.put("ukk-" + j, "uvv-" + j);
            }
            asyncRequestContainer.offer(
                    buildMapRequest(state, StateRequestType.MAP_PUT_ALL, i, map, i * 2));
        }
        forStStateExecutor.executeBatchRequests(asyncRequestContainer).get();

        asyncRequestContainer = forStStateExecutor.createRequestContainer();
        List<StateRequest<?, ?, ?, ?>> checkList = new ArrayList<>();

        // 2. check the number of user key under primary key is correct
        for (int i = 0; i < 100; i++) {
            StateRequest<?, ?, ?, ?> iterRequest =
                    buildStateRequest(state, StateRequestType.MAP_ITER_KEY, i, null, i * 2);
            asyncRequestContainer.offer(iterRequest);
            checkList.add(iterRequest);
        }

        forStStateExecutor.executeBatchRequests(asyncRequestContainer).get();

        for (int i = 0; i < 50; i++) { // 1 user key per primary key
            StateIterator<String> iter =
                    (StateIterator<String>)
                            ((TestAsyncFuture) checkList.get(i).getFuture()).getCompletedResult();
            AtomicInteger count = new AtomicInteger(0);
            iter.onNext(
                    k -> {
                        count.incrementAndGet();
                    });
            assertThat(count.get()).isEqualTo(1);
            assertThat(iter.isEmpty()).isFalse();
        }

        for (int i = 50; i < 100; i++) { // 51 user keys per primary key
            StateIterator<String> iter =
                    (StateIterator<String>)
                            ((TestAsyncFuture) checkList.get(i).getFuture()).getCompletedResult();
            AtomicInteger count = new AtomicInteger(0);
            iter.onNext(
                    k -> {
                        count.incrementAndGet();
                    });
            assertThat(count.get()).isEqualTo(51);
            assertThat(iter.isEmpty()).isFalse();
        }

        asyncRequestContainer = forStStateExecutor.createRequestContainer();

        // 3. delete primary key [75,100)
        for (int i = 75; i < 100; i++) {
            asyncRequestContainer.offer(
                    buildMapRequest(state, StateRequestType.CLEAR, i, null, i * 2));
        }

        forStStateExecutor.executeBatchRequests(asyncRequestContainer).get();
        asyncRequestContainer = forStStateExecutor.createRequestContainer();
        checkList.clear();
        // 4. check primary key [75,100) is deleted
        for (int i = 0; i < 100; i++) {
            StateRequest<?, ?, ?, ?> iterRequest =
                    buildStateRequest(state, StateRequestType.MAP_IS_EMPTY, i, null, i * 2);
            asyncRequestContainer.offer(iterRequest);
            checkList.add(iterRequest);
        }
        forStStateExecutor.executeBatchRequests(asyncRequestContainer).get();
        for (int i = 0; i < 75; i++) { // not empty
            boolean empty =
                    (Boolean) ((TestAsyncFuture) checkList.get(i).getFuture()).getCompletedResult();
            assertThat(empty).isFalse();
        }
        for (int i = 75; i < 100; i++) { // empty
            boolean empty =
                    (Boolean) ((TestAsyncFuture) checkList.get(i).getFuture()).getCompletedResult();
            assertThat(empty).isTrue();
        }

        forStStateExecutor.shutdown();
    }

    @ParameterizedTest
    @CsvSource({
        "MAP_ITER, 0",
        "MAP_ITER, 1",
        "MAP_ITER, " + ForStIterateOperation.CACHE_SIZE_LIMIT,
        "MAP_IS_EMPTY, 0",
        "CLEAR, 1"
    })
    void testIteratorErrorFailsRequest(StateRequestType requestType, int entriesBeforeError)
            throws Exception {
        ForStMapState<Integer, VoidNamespace, String, String> state =
                buildForStMapState("map-state");
        putEntries(state, 200, null);
        IteratorFault fault = new IteratorFault(state.getColumnFamilyHandle(), entriesBeforeError);
        try (WriteOptions writeOptions = new WriteOptions()) {
            ForStStateExecutor forStStateExecutor =
                    new ForStStateExecutor(
                            false, false, 2, 1, new FaultyIteratorDb(db, fault), writeOptions);
            try {
                AsyncRequestContainer<StateRequest<?, ?, ?, ?>> requests =
                        forStStateExecutor.createRequestContainer();
                StateRequest<?, ?, ?, ?> request =
                        buildMapRequest(state, requestType, KEY, null, 0);
                requests.offer(request);
                CompletableFuture<Void> batch = forStStateExecutor.executeBatchRequests(requests);

                assertThat(batch).failsWithin(30, TimeUnit.SECONDS);
                assertThat(((TestAsyncFuture<?>) request.getFuture()).future)
                        .failsWithin(30, TimeUnit.SECONDS)
                        .withThrowableThat()
                        .havingRootCause()
                        .isSameAs(fault.error);
                // Closing may be asynchronous.
                assertThat(fault.closed.await(30, TimeUnit.SECONDS)).isTrue();
                assertThat(fault.closes).hasValue(1);
            } finally {
                forStStateExecutor.shutdown();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {3, ForStIterateOperation.CACHE_SIZE_LIMIT + 10})
    void testBatchFailureDoesNotCloseIteratorsInUse(int heldEntryCount) throws Exception {
        ForStMapState<Integer, VoidNamespace, String, String> heldState =
                buildForStMapState("held-map-state");
        ForStMapState<Integer, VoidNamespace, String, String> failingState =
                buildForStMapState("failing-map-state");
        putEntries(heldState, heldEntryCount, null);
        // Cannot be deserialized.
        putEntries(failingState, 1, new byte[0]);
        IteratorFault held = new IteratorFault(heldState.getColumnFamilyHandle(), NO_ERROR);
        held.releaseNext = new CountDownLatch(1);
        IteratorFault failing = new IteratorFault(failingState.getColumnFamilyHandle(), NO_ERROR);
        // The failing request only starts once the held one is iterating.
        failing.seekGate = held.inNext;
        try (WriteOptions writeOptions = new WriteOptions()) {
            ForStStateExecutor forStStateExecutor =
                    new ForStStateExecutor(
                            false,
                            false,
                            2,
                            1,
                            new FaultyIteratorDb(db, held, failing),
                            writeOptions);
            try {
                AsyncRequestContainer<StateRequest<?, ?, ?, ?>> requests =
                        forStStateExecutor.createRequestContainer();
                StateRequest<?, ?, ?, ?> heldRequest =
                        buildMapRequest(heldState, StateRequestType.MAP_ITER, KEY, null, 0);
                requests.offer(heldRequest);
                requests.offer(
                        buildMapRequest(failingState, StateRequestType.MAP_ITER, KEY, null, 0));
                CompletableFuture<Void> batch = forStStateExecutor.executeBatchRequests(requests);

                assertThat(held.inNext.await(30, TimeUnit.SECONDS)).isTrue();
                assertThat(batch).failsWithin(30, TimeUnit.SECONDS);
                assertThat(held.closes).hasValue(0);

                held.releaseNext.countDown();
                assertThat(((TestAsyncFuture<?>) heldRequest.getFuture()).future)
                        .succeedsWithin(30, TimeUnit.SECONDS);
                if (heldEntryCount <= ForStIterateOperation.CACHE_SIZE_LIMIT) {
                    assertThat(held.closes).hasValue(1);
                } else {
                    // A partial page keeps its iterator open until shutdown reclaims it after
                    // workers stop.
                    assertThat(held.closes).hasValue(0);
                }
            } finally {
                held.releaseNext.countDown();
                forStStateExecutor.shutdown();
            }
            assertThat(held.closes).hasValue(1);
        }
    }

    @Test
    void testInterruptedShutdownWaitsForActiveReaderBeforeClosingIterators() throws Exception {
        ForStMapState<Integer, VoidNamespace, String, String> heldState =
                buildForStMapState("held-map-state");
        ForStMapState<Integer, VoidNamespace, String, String> failingState =
                buildForStMapState("failing-map-state");
        putEntries(heldState, ForStIterateOperation.CACHE_SIZE_LIMIT + 10, null);
        putEntries(failingState, 1, new byte[0]);
        IteratorFault held = new IteratorFault(heldState.getColumnFamilyHandle(), NO_ERROR);
        held.releaseNext = new CountDownLatch(1);
        IteratorFault failing = new IteratorFault(failingState.getColumnFamilyHandle(), NO_ERROR);
        failing.seekGate = held.inNext;
        try (WriteOptions writeOptions = new WriteOptions()) {
            ForStStateExecutor forStStateExecutor =
                    new ForStStateExecutor(
                            true,
                            false,
                            2,
                            1,
                            new FaultyIteratorDb(db, held, failing),
                            writeOptions);
            Thread shutdownThread = null;
            AtomicInteger shutdownInterrupted = new AtomicInteger();
            StateRequest<?, ?, ?, ?> heldRequest;
            try {
                AsyncRequestContainer<StateRequest<?, ?, ?, ?>> requests =
                        forStStateExecutor.createRequestContainer();
                heldRequest = buildMapRequest(heldState, StateRequestType.MAP_ITER, KEY, null, 0);
                requests.offer(heldRequest);
                requests.offer(
                        buildMapRequest(failingState, StateRequestType.MAP_ITER, KEY, null, 0));
                CompletableFuture<Void> batch = forStStateExecutor.executeBatchRequests(requests);

                assertThat(held.inNext.await(30, TimeUnit.SECONDS)).isTrue();
                assertThat(batch).failsWithin(30, TimeUnit.SECONDS);

                CountDownLatch shutdownEntered = new CountDownLatch(1);
                shutdownThread =
                        new Thread(
                                () -> {
                                    Thread.currentThread().interrupt();
                                    shutdownEntered.countDown();
                                    forStStateExecutor.shutdown();
                                    if (Thread.currentThread().isInterrupted()) {
                                        shutdownInterrupted.incrementAndGet();
                                    }
                                });
                shutdownThread.start();
                assertThat(shutdownEntered.await(30, TimeUnit.SECONDS)).isTrue();
                assertThat(held.releaseNextAwaitedAfterInterrupt.await(30, TimeUnit.SECONDS))
                        .isTrue();
                assertThat(held.closes).hasValue(0);
            } finally {
                held.releaseNext.countDown();
                if (shutdownThread != null) {
                    shutdownThread.join(30_000L);
                } else {
                    forStStateExecutor.shutdown();
                }
            }
            assertThat(shutdownThread.isAlive()).isFalse();
            assertThat(((TestAsyncFuture<?>) heldRequest.getFuture()).future)
                    .succeedsWithin(30, TimeUnit.SECONDS);
            assertThat(held.closes).hasValue(1);
            assertThat(shutdownInterrupted).hasValue(1);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testExecuteAggregatingStateRequest() throws Exception {
        ForStStateExecutor forStStateExecutor =
                new ForStStateExecutor(false, false, 4, 1, db, new WriteOptions());
        ForStAggregatingState<String, ?, Integer, Integer, Integer> state =
                buildForStSumAggregateState("agg-state-1");

        AsyncRequestContainer asyncRequestContainer = forStStateExecutor.createRequestContainer();
        assertTrue(asyncRequestContainer.isEmpty());

        // 1. init aggregateValue for every 1000 key
        int keyNum = 1000;
        for (int i = 0; i < keyNum; i++) {
            asyncRequestContainer.offer(
                    buildStateRequest(
                            state, StateRequestType.AGGREGATING_ADD, "" + i, i, "record" + i));
        }

        forStStateExecutor.executeBatchRequests(asyncRequestContainer).get();

        // 2. get all value and verify
        List<StateRequest<String, ?, Integer, Integer>> requests = new ArrayList<>();
        asyncRequestContainer = forStStateExecutor.createRequestContainer();
        for (int i = 0; i < keyNum; i++) {
            StateRequest<String, ?, Integer, Integer> r =
                    (StateRequest<String, ?, Integer, Integer>)
                            buildStateRequest(
                                    state,
                                    StateRequestType.AGGREGATING_GET,
                                    "" + i,
                                    null,
                                    "record" + i);
            requests.add(r);
            asyncRequestContainer.offer(r);
        }

        forStStateExecutor.executeBatchRequests(asyncRequestContainer).get();

        for (StateRequest<String, ?, Integer, Integer> request : requests) {
            assertThat(request.getRequestType()).isEqualTo(StateRequestType.AGGREGATING_GET);
            String key = request.getRecordContext().getKey();
            assertThat(request.getRecordContext().getRecord()).isEqualTo("record" + key);
            assertThat(((TestAsyncFuture<Integer>) request.getFuture()).getCompletedResult())
                    .isEqualTo(Integer.parseInt(key));
        }

        // 3. add more value for the aggregate state
        asyncRequestContainer = forStStateExecutor.createRequestContainer();
        int addCnt = 10;
        for (int i = 0; i < keyNum; i++) {
            for (int j = 0; j < addCnt; j++) {
                StateRequest<String, ?, Integer, Integer> r =
                        (StateRequest<String, ?, Integer, Integer>)
                                buildStateRequest(
                                        state, StateRequestType.AGGREGATING_ADD, "" + i, 1, i * 2);
                requests.add(r);
                asyncRequestContainer.offer(r);
            }
        }

        // clear first 100 state
        for (int i = 0; i < 100; i++) {
            StateRequest<String, ?, Integer, Integer> r =
                    (StateRequest<String, ?, Integer, Integer>)
                            buildStateRequest(
                                    state, StateRequestType.CLEAR, "" + i, null, "record" + i);
            requests.add(r);
            asyncRequestContainer.offer(r);
        }

        forStStateExecutor.executeBatchRequests(asyncRequestContainer).get();

        // 4. read and verify the updated aggregate state
        requests = new ArrayList<>();
        asyncRequestContainer = forStStateExecutor.createRequestContainer();
        for (int i = 0; i < keyNum; i++) {
            StateRequest<String, ?, Integer, Integer> r =
                    (StateRequest<String, ?, Integer, Integer>)
                            buildStateRequest(
                                    state,
                                    StateRequestType.AGGREGATING_GET,
                                    "" + i,
                                    null,
                                    "record" + i);
            requests.add(r);
            asyncRequestContainer.offer(r);
        }

        forStStateExecutor.executeBatchRequests(asyncRequestContainer).get();

        for (int i = 0; i < 100; i++) {
            StateRequest<String, ?, Integer, Integer> request = requests.get(i);
            assertThat(request.getRequestType()).isEqualTo(StateRequestType.AGGREGATING_GET);
            assertThat(((TestAsyncFuture<Integer>) request.getFuture()).getCompletedResult())
                    .isEqualTo(null);
        }
        for (int i = 100; i < keyNum; i++) {
            StateRequest<String, ?, Integer, Integer> request = requests.get(i);
            assertThat(request.getRequestType()).isEqualTo(StateRequestType.AGGREGATING_GET);
            String key = request.getRecordContext().getKey();
            assertThat(request.getRecordContext().getRecord()).isEqualTo("record" + key);
            assertThat(((TestAsyncFuture<Integer>) request.getFuture()).getCompletedResult())
                    .isEqualTo(1);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <K, N, V, R> StateRequest<?, ?, ?, ?> buildStateRequest(
            AbstractKeyedState<K, N, V> innerTable,
            StateRequestType requestType,
            K key,
            V value,
            R record) {
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 128);
        RecordContext<K> recordContext =
                new RecordContext<>(record, key, t -> {}, keyGroup, new Epoch(0), 0);
        TestAsyncFuture stateFuture = new TestAsyncFuture<>();
        return new StateRequest<>(
                innerTable, requestType, false, value, stateFuture, recordContext);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <K, N, UK, UV, R> StateRequest<?, ?, ?, ?> buildMapRequest(
            ForStMapState<K, N, UK, UV> innerTable,
            StateRequestType requestType,
            K key,
            Object value,
            R record) {
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 128);
        RecordContext<K> recordContext =
                new RecordContext<>(record, key, t -> {}, keyGroup, new Epoch(0), 0);
        TestAsyncFuture stateFuture = new TestAsyncFuture<>();
        return new StateRequest<>(
                innerTable, requestType, false, value, stateFuture, recordContext);
    }

    /** Stores {@code count} entries under {@link #KEY}, with {@code rawValue} if not null. */
    private void putEntries(
            ForStMapState<Integer, VoidNamespace, String, String> state,
            int count,
            @Nullable byte[] rawValue)
            throws Exception {
        for (int i = 0; i < count; i++) {
            ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(KEY);
            contextKey.setUserKey("uk-" + i);
            db.put(
                    state.getColumnFamilyHandle(),
                    state.serializeKey(contextKey),
                    rawValue != null ? rawValue : state.serializeValue("uv-" + i));
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for the test.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void awaitUninterruptibly(
            CountDownLatch latch, @Nullable CountDownLatch interruptedSignal) {
        boolean interrupted = false;
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        try {
            while (true) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    throw new IllegalStateException("Timed out waiting for the test.");
                }
                try {
                    if (latch.await(remainingNanos, TimeUnit.NANOSECONDS)) {
                        return;
                    }
                    throw new IllegalStateException("Timed out waiting for the test.");
                } catch (InterruptedException e) {
                    interrupted = true;
                    if (interruptedSignal != null) {
                        interruptedSignal.countDown();
                    }
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** How the iterators of one column family misbehave, and how often they were closed. */
    private static final class IteratorFault {
        final ColumnFamilyHandle columnFamily;
        final int entriesBeforeError;
        final RocksDBException error = new RocksDBException("Injected iterator failure");
        final AtomicInteger closes = new AtomicInteger();
        final CountDownLatch closed = new CountDownLatch(1);

        /** Awaited before seeking. */
        CountDownLatch seekGate = new CountDownLatch(0);

        /** Counted down by the first next(), which then waits for {@link #releaseNext}. */
        final CountDownLatch inNext = new CountDownLatch(1);

        /** Counted down when {@link #releaseNext} receives a worker interrupt (shutdownNow()). */
        final CountDownLatch releaseNextAwaitedAfterInterrupt = new CountDownLatch(1);

        CountDownLatch releaseNext = new CountDownLatch(0);

        IteratorFault(ColumnFamilyHandle columnFamily, int entriesBeforeError) {
            this.columnFamily = columnFamily;
            this.entriesBeforeError = entriesBeforeError;
        }
    }

    /** Hands out a {@link FaultyIterator} for the column families of the given faults. */
    private static final class FaultyIteratorDb extends RocksDB {
        /** Creates the iterators: one created by this disowned DB is never freed on close. */
        private final RocksDB owningDb;

        private final List<IteratorFault> faults;

        FaultyIteratorDb(RocksDB db, IteratorFault... faults) {
            super(db.getNativeHandle());
            disOwnNativeHandle();
            this.owningDb = db;
            this.faults = List.of(faults);
        }

        @Override
        public RocksIterator newIterator(ColumnFamilyHandle columnFamily) {
            RocksIterator iterator = owningDb.newIterator(columnFamily);
            for (IteratorFault fault : faults) {
                if (fault.columnFamily == columnFamily) {
                    return new FaultyIterator(this, iterator, fault);
                }
            }
            return iterator;
        }
    }

    /**
     * A real iterator that becomes invalid with {@link IteratorFault#error} after {@link
     * IteratorFault#entriesBeforeError} entries. Use after close fails in Java instead of touching
     * freed native memory.
     */
    private static final class FaultyIterator extends RocksIterator {
        private final RocksIterator delegate;
        private final IteratorFault fault;
        private int entries;
        private volatile boolean closed;

        FaultyIterator(RocksDB db, RocksIterator delegate, IteratorFault fault) {
            super(db, delegate.getNativeHandle());
            disOwnNativeHandle();
            this.delegate = delegate;
            this.fault = fault;
        }

        @Override
        public void seek(byte[] target) {
            await(fault.seekGate);
            checkOpen();
            delegate.seek(target);
        }

        @Override
        public boolean isValid() {
            checkOpen();
            return entries < fault.entriesBeforeError && delegate.isValid();
        }

        @Override
        public void status() throws RocksDBException {
            checkOpen();
            if (entries >= fault.entriesBeforeError) {
                throw fault.error;
            }
            delegate.status();
        }

        @Override
        public byte[] key() {
            checkOpen();
            return delegate.key();
        }

        @Override
        public byte[] value() {
            checkOpen();
            return delegate.value();
        }

        @Override
        public void next() {
            if (entries == 0) {
                fault.inNext.countDown();
                awaitUninterruptibly(fault.releaseNext, fault.releaseNextAwaitedAfterInterrupt);
            }
            checkOpen();
            delegate.next();
            entries++;
        }

        @Override
        public void close() {
            closed = true;
            fault.closes.incrementAndGet();
            delegate.close();
            fault.closed.countDown();
        }

        private void checkOpen() {
            if (closed) {
                throw new IllegalStateException("Iterator used after close.");
            }
        }
    }
}
