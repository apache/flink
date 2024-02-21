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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;

/** Tests for the FencedRpcEndpoint. */
class FencedRpcEndpointTest {

    private static final Time timeout = Time.seconds(10L);
    private static RpcService rpcService;

    @BeforeAll
    static void setup() {
        rpcService = new TestingRpcService();
    }

    @AfterAll
    static void teardown() throws ExecutionException, InterruptedException, TimeoutException {
        if (rpcService != null) {
            RpcUtils.terminateRpcService(rpcService);
        }
    }

    /** Tests that messages with the wrong fencing token are filtered out. */
    @Test
    void testFencing() throws Exception {
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

            assertThat(
                            properFencedGateway
                                    .foobar(timeout)
                                    .get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS))
                    .isEqualTo(value);

            try {
                wronglyFencedGateway
                        .foobar(timeout)
                        .get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
                fail("This should fail since we have the wrong fencing token.");
            } catch (ExecutionException e) {
                assertThat(ExceptionUtils.stripExecutionException(e))
                        .isInstanceOf(FencingTokenException.class);
            }

        } finally {
            RpcUtils.terminateRpcEndpoint(fencedTestingEndpoint);
            fencedTestingEndpoint.validateResourceClosed();
        }
    }

    /**
     * Tests that all calls from an unfenced remote gateway are ignored and that one cannot obtain
     * the fencing token from such a gateway.
     */
    @Test
    void testUnfencedRemoteGateway() throws Exception {
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
                assertThat(ExceptionUtils.stripExecutionException(e))
                        .isInstanceOf(RpcRuntimeException.class);
            }

            // we should not be able to call getFencingToken on an unfenced gateway
            assertThatThrownBy(unfencedGateway::getFencingToken)
                    .withFailMessage(
                            "We should not be able to call getFencingToken on an unfenced gateway.")
                    .isInstanceOf(UnsupportedOperationException.class);
        } finally {
            RpcUtils.terminateRpcEndpoint(fencedTestingEndpoint);
            fencedTestingEndpoint.validateResourceClosed();
        }
    }

    public interface FencedTestingGateway extends FencedRpcGateway<UUID> {
        CompletableFuture<String> foobar(@RpcTimeout Time timeout);

        CompletableFuture<Acknowledge> triggerComputationLatch(@RpcTimeout Time timeout);
    }

    private static class FencedTestingEndpoint extends FencedRpcEndpoint<UUID>
            implements FencedTestingGateway {

        private final OneShotLatch computationLatch;

        private final String value;

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
        public CompletableFuture<Acknowledge> triggerComputationLatch(Time timeout) {
            computationLatch.trigger();

            return CompletableFuture.completedFuture(Acknowledge.get());
        }
    }
}
