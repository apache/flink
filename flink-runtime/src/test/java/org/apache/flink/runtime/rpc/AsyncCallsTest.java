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

package org.apache.flink.runtime.rpc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncCallsTest {

    // ------------------------------------------------------------------------
    //  shared test members
    // ------------------------------------------------------------------------

    private static final Duration timeout = Duration.ofSeconds(10L);

    private static RpcService rpcService;

    @BeforeAll
    static void setup() throws Exception {
        rpcService = RpcSystem.load().localServiceBuilder(new Configuration()).createAndStart();
    }

    @AfterAll
    static void shutdown() throws InterruptedException, ExecutionException, TimeoutException {
        rpcService.closeAsync().get();
    }

    // ------------------------------------------------------------------------
    //  tests
    // ------------------------------------------------------------------------

    @Test
    void testScheduleWithNoDelay() throws Exception {
        runScheduleWithNoDelayTest(TestEndpoint::new);
    }

    @Test
    void testFencedScheduleWithNoDelay() throws Exception {
        runScheduleWithNoDelayTest(FencedTestEndpoint::new);
    }

    private void runScheduleWithNoDelayTest(RpcEndpointFactory factory) throws Exception {
        // to collect all the thread references
        final ReentrantLock lock = new ReentrantLock();
        final AtomicBoolean concurrentAccess = new AtomicBoolean(false);

        RpcEndpoint rpcEndpoint = factory.create(rpcService, lock, concurrentAccess);
        rpcEndpoint.start();

        try {
            TestGateway gateway = rpcEndpoint.getSelfGateway(TestGateway.class);

            // a bunch of gateway calls
            gateway.someCall();
            gateway.anotherCall();
            gateway.someCall();

            // run something asynchronously
            for (int i = 0; i < 10000; i++) {
                rpcEndpoint.runAsync(
                        () -> {
                            boolean holdsLock = lock.tryLock();
                            if (holdsLock) {
                                lock.unlock();
                            } else {
                                concurrentAccess.set(true);
                            }
                        });
            }

            CompletableFuture<String> result =
                    rpcEndpoint.callAsync(
                            () -> {
                                boolean holdsLock = lock.tryLock();
                                if (holdsLock) {
                                    lock.unlock();
                                } else {
                                    concurrentAccess.set(true);
                                }
                                return "test";
                            },
                            Duration.ofSeconds(30L));

            String str = result.get(30, TimeUnit.SECONDS);
            assertThat(str).isEqualTo("test");

            // validate that no concurrent access happened
            assertThat(concurrentAccess)
                    .withFailMessage("Rpc Endpoint had concurrent access")
                    .isFalse();
        } finally {
            RpcUtils.terminateRpcEndpoint(rpcEndpoint);
        }
    }

    @Test
    void testScheduleWithDelay() throws Exception {
        runScheduleWithDelayTest(TestEndpoint::new);
    }

    @Test
    void testFencedScheduleWithDelay() throws Exception {
        runScheduleWithDelayTest(FencedTestEndpoint::new);
    }

    private void runScheduleWithDelayTest(RpcEndpointFactory factory) throws Exception {
        // to collect all the thread references
        final ReentrantLock lock = new ReentrantLock();
        final AtomicBoolean concurrentAccess = new AtomicBoolean(false);
        final OneShotLatch latch = new OneShotLatch();

        final long delay = 10L;

        RpcEndpoint rpcEndpoint = factory.create(rpcService, lock, concurrentAccess);
        rpcEndpoint.start();

        try {
            // run something asynchronously
            rpcEndpoint.runAsync(
                    () -> {
                        boolean holdsLock = lock.tryLock();
                        if (holdsLock) {
                            lock.unlock();
                        } else {
                            concurrentAccess.set(true);
                        }
                    });

            final long start = System.nanoTime();

            rpcEndpoint.scheduleRunAsync(
                    () -> {
                        boolean holdsLock = lock.tryLock();
                        if (holdsLock) {
                            lock.unlock();
                        } else {
                            concurrentAccess.set(true);
                        }
                        latch.trigger();
                    },
                    delay,
                    TimeUnit.MILLISECONDS);

            latch.await();
            final long stop = System.nanoTime();

            // validate that no concurrent access happened
            assertThat(concurrentAccess)
                    .withFailMessage("Rpc Endpoint had concurrent access")
                    .isFalse();

            assertThat(delay)
                    .withFailMessage("call was not properly delayed")
                    .isLessThanOrEqualTo((stop - start) / 1_000_000);
        } finally {
            RpcUtils.terminateRpcEndpoint(rpcEndpoint);
        }
    }

    @FunctionalInterface
    private interface RpcEndpointFactory {
        RpcEndpoint create(
                RpcService rpcService, ReentrantLock lock, AtomicBoolean concurrentAccess);
    }

    // ------------------------------------------------------------------------
    //  test RPC endpoint
    // ------------------------------------------------------------------------

    public interface TestGateway extends RpcGateway {

        void someCall();

        void anotherCall();
    }

    private static class TestEndpoint extends RpcEndpoint implements TestGateway {

        private final ReentrantLock lock;

        private final AtomicBoolean concurrentAccess;

        TestEndpoint(RpcService rpcService, ReentrantLock lock, AtomicBoolean concurrentAccess) {
            super(rpcService);
            this.lock = lock;
            this.concurrentAccess = concurrentAccess;
        }

        @Override
        public void someCall() {
            boolean holdsLock = lock.tryLock();
            if (holdsLock) {
                lock.unlock();
            } else {
                concurrentAccess.set(true);
            }
        }

        @Override
        public void anotherCall() {
            boolean holdsLock = lock.tryLock();
            if (holdsLock) {
                lock.unlock();
            } else {
                concurrentAccess.set(true);
            }
        }
    }

    public static class FencedTestEndpoint extends FencedRpcEndpoint<UUID> implements TestGateway {

        private final ReentrantLock lock;
        private final AtomicBoolean concurrentAccess;

        private final OneShotLatch enteringSetNewFencingToken;
        private final OneShotLatch triggerSetNewFencingToken;

        protected FencedTestEndpoint(
                RpcService rpcService, ReentrantLock lock, AtomicBoolean concurrentAccess) {
            this(
                    rpcService,
                    lock,
                    concurrentAccess,
                    UUID.randomUUID(),
                    new OneShotLatch(),
                    new OneShotLatch());
        }

        protected FencedTestEndpoint(
                RpcService rpcService,
                UUID initialFencingToken,
                OneShotLatch enteringSetNewFencingToken,
                OneShotLatch triggerSetNewFencingToken) {
            this(
                    rpcService,
                    new ReentrantLock(),
                    new AtomicBoolean(false),
                    initialFencingToken,
                    enteringSetNewFencingToken,
                    triggerSetNewFencingToken);
        }

        private FencedTestEndpoint(
                RpcService rpcService,
                ReentrantLock lock,
                AtomicBoolean concurrentAccess,
                UUID initialFencingToken,
                OneShotLatch enteringSetNewFencingToken,
                OneShotLatch triggerSetNewFencingToken) {
            super(rpcService, initialFencingToken);

            this.lock = lock;
            this.concurrentAccess = concurrentAccess;

            this.enteringSetNewFencingToken = enteringSetNewFencingToken;
            this.triggerSetNewFencingToken = triggerSetNewFencingToken;
        }

        @Override
        public void someCall() {
            boolean holdsLock = lock.tryLock();
            if (holdsLock) {
                lock.unlock();
            } else {
                concurrentAccess.set(true);
            }
        }

        @Override
        public void anotherCall() {
            boolean holdsLock = lock.tryLock();
            if (holdsLock) {
                lock.unlock();
            } else {
                concurrentAccess.set(true);
            }
        }
    }
}
