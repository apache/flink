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
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.exceptions.FencingTokenException;
import org.apache.flink.runtime.rpc.exceptions.RpcRuntimeException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FencedRpcEndpointTest extends TestLogger {

    private static final Time timeout = Time.seconds(10L);
    private static RpcService rpcService;

    @BeforeClass
    public static void setup() {
        rpcService = new TestingRpcService();
    }

    @AfterClass
    public static void teardown()
            throws ExecutionException, InterruptedException, TimeoutException {
        if (rpcService != null) {
            RpcUtils.terminateRpcService(rpcService, timeout);
        }
    }

    /**
     * Tests that the fencing token can be retrieved from the FencedRpcEndpoint and self
     * FencedRpcGateway. Moreover it tests that you can only set the fencing token from the main
     * thread.
     */
    @Test
    public void testFencingTokenSetting() throws Exception {
        final String value = "foobar";
        FencedTestingEndpoint fencedTestingEndpoint = new FencedTestingEndpoint(rpcService, value);
        FencedTestingGateway fencedGateway =
                fencedTestingEndpoint.getSelfGateway(FencedTestingGateway.class);

        try {
            fencedTestingEndpoint.start();

            assertNull(fencedGateway.getFencingToken());
            assertNull(fencedTestingEndpoint.getFencingToken());

            final UUID newFencingToken = UUID.randomUUID();

            boolean failed = false;
            try {
                fencedTestingEndpoint.setFencingToken(newFencingToken);
                failed = true;
            } catch (AssertionError ignored) {
                // expected to fail
            }

            assertFalse(
                    "Setting fencing token from outside the main thread did not fail as expected.",
                    failed);
            assertNull(fencedTestingEndpoint.getFencingToken());

            CompletableFuture<Acknowledge> setFencingFuture =
                    fencedTestingEndpoint.setFencingTokenInMainThread(newFencingToken, timeout);

            // wait for the completion of the set fencing token operation
            setFencingFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

            // self gateway should adapt its fencing token
            assertEquals(newFencingToken, fencedGateway.getFencingToken());
            assertEquals(newFencingToken, fencedTestingEndpoint.getFencingToken());
        } finally {
            RpcUtils.terminateRpcEndpoint(fencedTestingEndpoint, timeout);
        }
    }

    /** Tests that messages with the wrong fencing token are filtered out. */
    @Test
    public void testFencing() throws Exception {
        final UUID fencingToken = UUID.randomUUID();
        final UUID wrongFencingToken = UUID.randomUUID();
        final String value = "barfoo";
        FencedTestingEndpoint fencedTestingEndpoint =
                new FencedTestingEndpoint(rpcService, value, fencingToken);

        try {
            fencedTestingEndpoint.start();

            final FencedTestingGateway properFencedGateway =
                    rpcService
                            .connect(
                                    fencedTestingEndpoint.getAddress(),
                                    fencingToken,
                                    FencedTestingGateway.class)
                            .get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
            final FencedTestingGateway wronglyFencedGateway =
                    rpcService
                            .connect(
                                    fencedTestingEndpoint.getAddress(),
                                    wrongFencingToken,
                                    FencedTestingGateway.class)
                            .get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

            assertEquals(
                    value,
                    properFencedGateway
                            .foobar(timeout)
                            .get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS));

            try {
                wronglyFencedGateway
                        .foobar(timeout)
                        .get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
                fail("This should fail since we have the wrong fencing token.");
            } catch (ExecutionException e) {
                assertTrue(
                        ExceptionUtils.stripExecutionException(e) instanceof FencingTokenException);
            }

            final UUID newFencingToken = UUID.randomUUID();

            CompletableFuture<Acknowledge> newFencingTokenFuture =
                    fencedTestingEndpoint.setFencingTokenInMainThread(newFencingToken, timeout);

            // wait for the new fencing token to be set
            newFencingTokenFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

            // this should no longer work because of the new fencing token
            try {
                properFencedGateway
                        .foobar(timeout)
                        .get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

                fail("This should fail since we have the wrong fencing token by now.");
            } catch (ExecutionException e) {
                assertTrue(
                        ExceptionUtils.stripExecutionException(e) instanceof FencingTokenException);
            }

        } finally {
            RpcUtils.terminateRpcEndpoint(fencedTestingEndpoint, timeout);
        }
    }

    /**
     * Tests that the self gateway always uses the current fencing token whereas the remote gateway
     * has a fixed fencing token.
     */
    @Test
    public void testRemoteAndSelfGateways() throws Exception {
        final UUID initialFencingToken = UUID.randomUUID();
        final UUID newFencingToken = UUID.randomUUID();
        final String value = "foobar";

        final FencedTestingEndpoint fencedTestingEndpoint =
                new FencedTestingEndpoint(rpcService, value, initialFencingToken);

        try {
            fencedTestingEndpoint.start();

            FencedTestingGateway selfGateway =
                    fencedTestingEndpoint.getSelfGateway(FencedTestingGateway.class);
            FencedTestingGateway remoteGateway =
                    rpcService
                            .connect(
                                    fencedTestingEndpoint.getAddress(),
                                    initialFencingToken,
                                    FencedTestingGateway.class)
                            .get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

            assertEquals(initialFencingToken, selfGateway.getFencingToken());
            assertEquals(initialFencingToken, remoteGateway.getFencingToken());

            assertEquals(
                    value,
                    selfGateway
                            .foobar(timeout)
                            .get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS));
            assertEquals(
                    value,
                    remoteGateway
                            .foobar(timeout)
                            .get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS));

            CompletableFuture<Acknowledge> newFencingTokenFuture =
                    fencedTestingEndpoint.setFencingTokenInMainThread(newFencingToken, timeout);

            // wait for the new fencing token to be set
            newFencingTokenFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

            assertEquals(newFencingToken, selfGateway.getFencingToken());
            assertNotEquals(newFencingToken, remoteGateway.getFencingToken());

            assertEquals(
                    value,
                    selfGateway
                            .foobar(timeout)
                            .get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS));

            try {
                remoteGateway.foobar(timeout).get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
                fail("This should have failed because we don't have the right fencing token.");
            } catch (ExecutionException e) {
                assertTrue(
                        ExceptionUtils.stripExecutionException(e) instanceof FencingTokenException);
            }
        } finally {
            RpcUtils.terminateRpcEndpoint(fencedTestingEndpoint, timeout);
        }
    }

    /** Tests that call via the MainThreadExecutor fail after the fencing token changes. */
    @Test
    public void testMainThreadExecutorUnderChangingFencingToken() throws Exception {
        final Time shortTimeout = Time.milliseconds(100L);
        final UUID initialFencingToken = UUID.randomUUID();
        final String value = "foobar";
        final FencedTestingEndpoint fencedTestingEndpoint =
                new FencedTestingEndpoint(rpcService, value, initialFencingToken);

        try {
            fencedTestingEndpoint.start();

            FencedTestingGateway selfGateway =
                    fencedTestingEndpoint.getSelfGateway(FencedTestingGateway.class);

            CompletableFuture<Acknowledge> mainThreadExecutorComputation =
                    selfGateway.triggerMainThreadExecutorComputation(timeout);

            // we know that subsequent calls on the same gateway are executed sequentially
            // therefore, we know that the change fencing token call is executed after the trigger
            // MainThreadExecutor
            // computation
            final UUID newFencingToken = UUID.randomUUID();
            CompletableFuture<Acknowledge> newFencingTokenFuture =
                    fencedTestingEndpoint.setFencingTokenInMainThread(newFencingToken, timeout);

            newFencingTokenFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

            // trigger the computation
            CompletableFuture<Acknowledge> triggerFuture =
                    selfGateway.triggerComputationLatch(timeout);

            triggerFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

            // wait for the main thread executor computation to fail
            try {
                mainThreadExecutorComputation.get(
                        shortTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
                fail(
                        "The MainThreadExecutor computation should be able to complete because it was filtered out leading to a timeout exception.");
            } catch (TimeoutException ignored) {
                // as predicted
            }

        } finally {
            RpcUtils.terminateRpcEndpoint(fencedTestingEndpoint, timeout);
        }
    }

    /**
     * Tests that all calls from an unfenced remote gateway are ignored and that one cannot obtain
     * the fencing token from such a gateway.
     */
    @Test
    public void testUnfencedRemoteGateway() throws Exception {
        final UUID initialFencingToken = UUID.randomUUID();
        final String value = "foobar";

        final FencedTestingEndpoint fencedTestingEndpoint =
                new FencedTestingEndpoint(rpcService, value, initialFencingToken);

        try {
            fencedTestingEndpoint.start();

            FencedTestingGateway unfencedGateway =
                    rpcService
                            .connect(fencedTestingEndpoint.getAddress(), FencedTestingGateway.class)
                            .get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

            try {
                unfencedGateway
                        .foobar(timeout)
                        .get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
                fail("This should have failed because we have an unfenced gateway.");
            } catch (ExecutionException e) {
                assertTrue(
                        ExceptionUtils.stripExecutionException(e) instanceof RpcRuntimeException);
            }

            try {
                unfencedGateway.getFencingToken();
                fail("We should not be able to call getFencingToken on an unfenced gateway.");
            } catch (UnsupportedOperationException ignored) {
                // we should not be able to call getFencingToken on an unfenced gateway
            }
        } finally {
            RpcUtils.terminateRpcEndpoint(fencedTestingEndpoint, timeout);
        }
    }

    public interface FencedTestingGateway extends FencedRpcGateway<UUID> {
        CompletableFuture<String> foobar(@RpcTimeout Time timeout);

        CompletableFuture<Acknowledge> triggerMainThreadExecutorComputation(
                @RpcTimeout Time timeout);

        CompletableFuture<Acknowledge> triggerComputationLatch(@RpcTimeout Time timeout);
    }

    private static class FencedTestingEndpoint extends FencedRpcEndpoint<UUID>
            implements FencedTestingGateway {

        private final OneShotLatch computationLatch;

        private final String value;

        protected FencedTestingEndpoint(RpcService rpcService, String value) {
            this(rpcService, value, null);
        }

        protected FencedTestingEndpoint(
                RpcService rpcService, String value, UUID initialFencingToken) {
            super(rpcService, initialFencingToken);

            computationLatch = new OneShotLatch();

            this.value = value;
        }

        @Override
        public CompletableFuture<String> foobar(Time timeout) {
            return CompletableFuture.completedFuture(value);
        }

        @Override
        public CompletableFuture<Acknowledge> triggerMainThreadExecutorComputation(Time timeout) {
            return CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    computationLatch.await();
                                } catch (InterruptedException e) {
                                    throw new CompletionException(
                                            new FlinkException("Waiting on latch failed.", e));
                                }

                                return value;
                            },
                            getRpcService().getScheduledExecutor())
                    .thenApplyAsync((String v) -> Acknowledge.get(), getMainThreadExecutor());
        }

        @Override
        public CompletableFuture<Acknowledge> triggerComputationLatch(Time timeout) {
            computationLatch.trigger();

            return CompletableFuture.completedFuture(Acknowledge.get());
        }

        public CompletableFuture<Acknowledge> setFencingTokenInMainThread(
                UUID fencingToken, Time timeout) {
            return callAsyncWithoutFencing(
                    () -> {
                        setFencingToken(fencingToken);

                        return Acknowledge.get();
                    },
                    timeout);
        }
    }
}
