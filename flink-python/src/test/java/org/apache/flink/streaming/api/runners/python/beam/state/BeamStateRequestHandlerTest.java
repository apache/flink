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

package org.apache.flink.streaming.api.runners.python.beam.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.streaming.api.utils.ByteArrayWrapper;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BeamStateRequestHandlerTest {

    @Test
    void testRejectsRequestsAfterClose() {
        final AtomicBoolean stateAccessed = new AtomicBoolean();
        final BeamStateStore keyedStateStore =
                new BeamStateStore() {
                    @Override
                    public ListState<byte[]> getListState(BeamFnApi.StateRequest request) {
                        stateAccessed.set(true);
                        return null;
                    }

                    @Override
                    public MapState<ByteArrayWrapper, byte[]> getMapState(
                            BeamFnApi.StateRequest request) {
                        throw new UnsupportedOperationException();
                    }
                };
        final BeamStateRequestHandler handler = createHandler(keyedStateStore);

        handler.close();

        assertThatThrownBy(() -> handler.handle(createBagUserStateRequest()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Beam state request handler is closed.");
        assertThat(stateAccessed).isFalse();
    }

    @Test
    void testCloseWaitsForInFlightRequest() throws Exception {
        final CountDownLatch stateAccessStarted = new CountDownLatch(1);
        final CountDownLatch releaseStateAccess = new CountDownLatch(1);
        final BeamStateStore keyedStateStore =
                new BeamStateStore() {
                    @Override
                    public ListState<byte[]> getListState(BeamFnApi.StateRequest request)
                            throws InterruptedException {
                        stateAccessStarted.countDown();
                        releaseStateAccess.await();
                        return null;
                    }

                    @Override
                    public MapState<ByteArrayWrapper, byte[]> getMapState(
                            BeamFnApi.StateRequest request) {
                        throw new UnsupportedOperationException();
                    }
                };
        final BeamStateRequestHandler handler = createHandler(keyedStateStore);
        final ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            final Future<?> requestFuture =
                    executor.submit(() -> handler.handle(createBagUserStateRequest()));
            assertThat(stateAccessStarted.await(10, TimeUnit.SECONDS)).isTrue();

            final CountDownLatch closeStarted = new CountDownLatch(1);
            final Future<?> closeFuture =
                    executor.submit(
                            () -> {
                                closeStarted.countDown();
                                handler.close();
                            });
            assertThat(closeStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> closeFuture.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);

            releaseStateAccess.countDown();
            requestFuture.get(10, TimeUnit.SECONDS);
            closeFuture.get(10, TimeUnit.SECONDS);
        } finally {
            releaseStateAccess.countDown();
            executor.shutdownNow();
        }
    }

    private static BeamStateRequestHandler createHandler(BeamStateStore keyedStateStore) {
        return new BeamStateRequestHandler(
                keyedStateStore,
                BeamStateStore.unsupported(),
                new NoOpBeamStateHandler<>(),
                new NoOpBeamStateHandler<>());
    }

    private static BeamFnApi.StateRequest createBagUserStateRequest() {
        return BeamFnApi.StateRequest.newBuilder()
                .setStateKey(
                        BeamFnApi.StateKey.newBuilder()
                                .setBagUserState(
                                        BeamFnApi.StateKey.BagUserState.getDefaultInstance()))
                .setGet(BeamFnApi.StateGetRequest.getDefaultInstance())
                .build();
    }

    private static class NoOpBeamStateHandler<S> implements BeamStateHandler<S> {

        @Override
        public BeamFnApi.StateResponse.Builder handle(BeamFnApi.StateRequest request, S state) {
            return BeamFnApi.StateResponse.newBuilder();
        }

        @Override
        public BeamFnApi.StateResponse.Builder handleGet(BeamFnApi.StateRequest request, S state) {
            return BeamFnApi.StateResponse.newBuilder();
        }

        @Override
        public BeamFnApi.StateResponse.Builder handleAppend(
                BeamFnApi.StateRequest request, S state) {
            return BeamFnApi.StateResponse.newBuilder();
        }

        @Override
        public BeamFnApi.StateResponse.Builder handleClear(
                BeamFnApi.StateRequest request, S state) {
            return BeamFnApi.StateResponse.newBuilder();
        }
    }
}
