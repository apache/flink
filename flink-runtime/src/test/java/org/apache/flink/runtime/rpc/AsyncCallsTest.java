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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.exceptions.FencingTokenException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AsyncCallsTest extends TestLogger {

    // ------------------------------------------------------------------------
    //  shared test members
    // ------------------------------------------------------------------------

    private static final Time timeout = Time.seconds(10L);

    private static RpcService rpcService;

    @BeforeClass
    public static void setup() throws Exception {
        rpcService = RpcSystem.load().localServiceBuilder(new Configuration()).createAndStart();
    }

    @AfterClass
    public static void shutdown()
            throws InterruptedException, ExecutionException, TimeoutException {
        rpcService.stopService().get();
    }

    // ------------------------------------------------------------------------
    //  tests
    // ------------------------------------------------------------------------

    @Test
    public void testScheduleWithNoDelay() throws Exception {
        runScheduleWithNoDelayTest(TestEndpoint::new);
    }

    @Test
    public void testFencedScheduleWithNoDelay() throws Exception {
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
                            Time.seconds(30L));

            String str = result.get(30, TimeUnit.SECONDS);
            assertEquals("test", str);

            // validate that no concurrent access happened
            assertFalse("Rpc Endpoint had concurrent access", concurrentAccess.get());
        } finally {
            RpcUtils.terminateRpcEndpoint(rpcEndpoint, timeout);
        }
    }

    @Test
    public void testScheduleWithDelay() throws Exception {
        runScheduleWithDelayTest(TestEndpoint::new);
    }

    @Test
    public void testFencedScheduleWithDelay() throws Exception {
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
            assertFalse("Rpc Endpoint had concurrent access", concurrentAccess.get());

            assertTrue("call was not properly delayed", ((stop - start) / 1_000_000) >= delay);
        } finally {
            RpcUtils.terminateRpcEndpoint(rpcEndpoint, timeout);
        }
    }

    @FunctionalInterface
    private interface RpcEndpointFactory {
        RpcEndpoint create(
                RpcService rpcService, ReentrantLock lock, AtomicBoolean concurrentAccess);
    }

    /** Tests that async code is not executed if the fencing token changes. */
    @Test
    public void testRunAsyncWithFencing() throws Exception {
        final Time shortTimeout = Time.milliseconds(100L);
        final UUID newFencingToken = UUID.randomUUID();
        final CompletableFuture<UUID> resultFuture = new CompletableFuture<>();

        testRunAsync(
                endpoint -> {
                    endpoint.runAsync(() -> resultFuture.complete(endpoint.getFencingToken()));

                    return resultFuture;
                },
                newFencingToken);

        try {
            resultFuture.get(shortTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

            fail(
                    "The async run operation should not complete since it is filtered out due to the changed fencing token.");
        } catch (TimeoutException ignored) {
        }
    }

    /** Tests that code can be executed in the main thread without respecting the fencing token. */
    @Test
    public void testRunAsyncWithoutFencing() throws Exception {
        final CompletableFuture<UUID> resultFuture = new CompletableFuture<>();
        final UUID newFencingToken = UUID.randomUUID();

        testRunAsync(
                endpoint -> {
                    endpoint.runAsyncWithoutFencing(
                            () -> resultFuture.complete(endpoint.getFencingToken()));
                    return resultFuture;
                },
                newFencingToken);

        assertEquals(
                newFencingToken, resultFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS));
    }

    /** Tests that async callables are not executed if the fencing token changes. */
    @Test
    public void testCallAsyncWithFencing() throws Exception {
        final UUID newFencingToken = UUID.randomUUID();

        CompletableFuture<Boolean> resultFuture =
                testRunAsync(endpoint -> endpoint.callAsync(() -> true, timeout), newFencingToken);

        try {
            resultFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

            fail("The async call operation should fail due to the changed fencing token.");
        } catch (ExecutionException e) {
            assertTrue(ExceptionUtils.stripExecutionException(e) instanceof FencingTokenException);
        }
    }

    /**
     * Tests that async callables can be executed in the main thread without checking the fencing
     * token.
     */
    @Test
    public void testCallAsyncWithoutFencing() throws Exception {
        final UUID newFencingToken = UUID.randomUUID();

        CompletableFuture<Boolean> resultFuture =
                testRunAsync(
                        endpoint -> endpoint.callAsyncWithoutFencing(() -> true, timeout),
                        newFencingToken);

        assertTrue(resultFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS));
    }

    @Test
    public void testUnfencedMainThreadExecutor() throws Exception {
        final UUID newFencingToken = UUID.randomUUID();

        final boolean value = true;
        final CompletableFuture<Boolean> resultFuture =
                testRunAsync(
                        endpoint ->
                                CompletableFuture.supplyAsync(
                                        () -> value, endpoint.getUnfencedMainThreadExecutor()),
                        newFencingToken);

        assertThat(resultFuture.get(), is(value));
    }

    private static <T> CompletableFuture<T> testRunAsync(
            Function<FencedTestEndpoint, CompletableFuture<T>> runAsyncCall, UUID newFencingToken)
            throws Exception {
        final UUID initialFencingToken = UUID.randomUUID();
        final OneShotLatch enterSetNewFencingToken = new OneShotLatch();
        final OneShotLatch triggerSetNewFencingToken = new OneShotLatch();
        final FencedTestEndpoint fencedTestEndpoint =
                new FencedTestEndpoint(
                        rpcService,
                        initialFencingToken,
                        enterSetNewFencingToken,
                        triggerSetNewFencingToken);
        final FencedTestGateway fencedTestGateway =
                fencedTestEndpoint.getSelfGateway(FencedTestGateway.class);

        try {
            fencedTestEndpoint.start();

            CompletableFuture<Acknowledge> newFencingTokenFuture =
                    fencedTestGateway.setNewFencingToken(newFencingToken, timeout);

            assertFalse(newFencingTokenFuture.isDone());

            assertEquals(initialFencingToken, fencedTestEndpoint.getFencingToken());

            CompletableFuture<T> result = runAsyncCall.apply(fencedTestEndpoint);

            enterSetNewFencingToken.await();

            triggerSetNewFencingToken.trigger();

            newFencingTokenFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

            return result;
        } finally {
            RpcUtils.terminateRpcEndpoint(fencedTestEndpoint, timeout);
        }
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

    public interface FencedTestGateway extends FencedRpcGateway<UUID>, TestGateway {
        CompletableFuture<Acknowledge> setNewFencingToken(
                UUID fencingToken, @RpcTimeout Time timeout);
    }

    public static class FencedTestEndpoint extends FencedRpcEndpoint<UUID>
            implements FencedTestGateway {

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
        public CompletableFuture<Acknowledge> setNewFencingToken(UUID fencingToken, Time timeout) {
            enteringSetNewFencingToken.trigger();
            try {
                triggerSetNewFencingToken.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(
                        "TriggerSetNewFencingToken OneShotLatch was interrupted.");
            }

            setFencingToken(fencingToken);

            return CompletableFuture.completedFuture(Acknowledge.get());
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
