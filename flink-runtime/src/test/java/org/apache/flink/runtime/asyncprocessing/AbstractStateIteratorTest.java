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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The tests for {@link AbstractStateIterator} which facilitate the basic partial loading of state
 * asynchronous iterators.
 */
public class AbstractStateIteratorTest {

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testPartialLoading() {
        TestIteratorStateExecutor stateExecutor = new TestIteratorStateExecutor(100, 3);
        AsyncExecutionController aec =
                new AsyncExecutionController(
                        new SyncMailboxExecutor(), (a, b) -> {}, stateExecutor, 1, 100, 1000, 1);
        stateExecutor.bindAec(aec);
        RecordContext<String> recordContext = aec.buildContext("1", "key1");
        aec.setCurrentContext(recordContext);

        AtomicInteger processed = new AtomicInteger();

        aec.handleRequest(null, StateRequestType.MAP_ITER, null)
                .thenAccept(
                        (iter) -> {
                            assertThat(iter).isInstanceOf(StateIterator.class);
                            ((StateIterator<Integer>) iter)
                                    .onNext(
                                            (item) -> {
                                                assertThat(item)
                                                        .isEqualTo(processed.getAndIncrement());
                                            })
                                    .thenAccept(
                                            (v) -> {
                                                assertThat(processed.get()).isEqualTo(100);
                                            });
                        });
        aec.drainInflightRecords(0);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testPartialLoadingWithReturnValue() {
        TestIteratorStateExecutor stateExecutor = new TestIteratorStateExecutor(100, 3);
        AsyncExecutionController aec =
                new AsyncExecutionController(
                        new SyncMailboxExecutor(), (a, b) -> {}, stateExecutor, 1, 100, 1000, 1);
        stateExecutor.bindAec(aec);
        RecordContext<String> recordContext = aec.buildContext("1", "key1");
        aec.setCurrentContext(recordContext);

        AtomicInteger processed = new AtomicInteger();

        aec.handleRequest(null, StateRequestType.MAP_ITER, null)
                .thenAccept(
                        (iter) -> {
                            assertThat(iter).isInstanceOf(StateIterator.class);
                            ((StateIterator<Integer>) iter)
                                    .onNext(
                                            (item) -> {
                                                assertThat(item)
                                                        .isEqualTo(processed.getAndIncrement());
                                                return StateFutureUtils.completedFuture(
                                                        String.valueOf(item));
                                            })
                                    .thenAccept(
                                            (strings) -> {
                                                assertThat(processed.get()).isEqualTo(100);
                                                int validate = 0;
                                                for (String item : strings) {
                                                    assertThat(item)
                                                            .isEqualTo(String.valueOf(validate++));
                                                }
                                            });
                        });
        aec.drainInflightRecords(0);
    }

    /**
     * A brief implementation of {@link StateExecutor}, to illustrate the interaction between AEC
     * and StateExecutor.
     */
    @SuppressWarnings({"rawtypes"})
    static class TestIteratorStateExecutor implements StateExecutor {

        final int limit;

        final int step;

        AsyncExecutionController aec;

        int current = 0;

        AtomicInteger processedCount = new AtomicInteger(0);

        public TestIteratorStateExecutor(int limit, int step) {
            this.limit = limit;
            this.step = step;
        }

        public void bindAec(AsyncExecutionController aec) {
            this.aec = aec;
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public CompletableFuture<Void> executeBatchRequests(
                StateRequestContainer stateRequestContainer) {
            Preconditions.checkArgument(stateRequestContainer instanceof MockStateRequestContainer);
            CompletableFuture<Void> future = new CompletableFuture<>();
            for (StateRequest request :
                    ((MockStateRequestContainer) stateRequestContainer).getStateRequestList()) {
                if (request.getRequestType() == StateRequestType.MAP_ITER) {
                    ArrayList<Integer> results = new ArrayList<>(step);
                    for (int i = 0; current < limit && i < step; i++) {
                        results.add(current++);
                    }
                    request.getFuture()
                            .complete(
                                    new TestIterator(
                                            request.getState(),
                                            request.getRequestType(),
                                            aec,
                                            results,
                                            current,
                                            limit));
                } else if (request.getRequestType() == StateRequestType.ITERATOR_LOADING) {
                    assertThat(request.getPayload()).isInstanceOf(TestIterator.class);
                    assertThat(((TestIterator) request.getPayload()).current).isEqualTo(current);
                    ArrayList<Integer> results = new ArrayList<>(step);
                    for (int i = 0; current < limit && i < step; i++) {
                        results.add(current++);
                    }
                    request.getFuture()
                            .complete(
                                    new TestIterator(
                                            request.getState(),
                                            ((TestIterator) request.getPayload()).getRequestType(),
                                            aec,
                                            results,
                                            current,
                                            limit));
                } else {
                    fail("Unsupported request type " + request.getRequestType());
                }
                processedCount.incrementAndGet();
            }
            future.complete(null);
            return future;
        }

        @Override
        public StateRequestContainer createStateRequestContainer() {
            return new MockStateRequestContainer();
        }

        @Override
        public void shutdown() {}

        static class TestIterator extends AbstractStateIterator<Integer> {

            final int current;

            final int limit;

            public TestIterator(
                    State originalState,
                    StateRequestType requestType,
                    AsyncExecutionController aec,
                    Collection<Integer> partialResult,
                    int current,
                    int limit) {
                super(originalState, requestType, aec, partialResult);
                this.current = current;
                this.limit = limit;
            }

            @Override
            protected boolean hasNext() {
                return current < limit;
            }

            @Override
            protected Object nextPayloadForContinuousLoading() {
                return this;
            }
        }
    }
}
