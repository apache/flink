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

package org.apache.flink.runtime.webmonitor.retriever.impl;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.dispatcher.cleanup.TestingRetryStrategies;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link RpcGatewayRetriever}. */
class RpcGatewayRetrieverTest {

    private static final Time TIMEOUT = Time.seconds(10L);
    private static TestingRpcService rpcService;

    @BeforeAll
    static void setup() {
        rpcService = new TestingRpcService();
    }

    @AfterAll
    static void teardown() throws InterruptedException, ExecutionException {
        if (rpcService != null) {
            RpcUtils.terminateRpcService(rpcService);
            rpcService = null;
        }
    }

    /**
     * Tests that the RpcGatewayRetriever can retrieve the specified gateway type from a leader
     * retrieval service.
     */
    @Test
    void testRpcGatewayRetrieval() throws Exception {
        final String expectedValue = "foobar";
        final String expectedValue2 = "barfoo";
        final UUID leaderSessionId = UUID.randomUUID();

        RpcGatewayRetriever<UUID, DummyGateway> gatewayRetriever =
                new RpcGatewayRetriever<>(
                        rpcService,
                        DummyGateway.class,
                        Function.identity(),
                        TestingRetryStrategies.NO_RETRY_STRATEGY);
        SettableLeaderRetrievalService settableLeaderRetrievalService =
                new SettableLeaderRetrievalService();
        DummyRpcEndpoint dummyRpcEndpoint =
                new DummyRpcEndpoint(rpcService, "dummyRpcEndpoint1", expectedValue);
        DummyRpcEndpoint dummyRpcEndpoint2 =
                new DummyRpcEndpoint(rpcService, "dummyRpcEndpoint2", expectedValue2);
        rpcService.registerGateway(
                dummyRpcEndpoint.getAddress(), dummyRpcEndpoint.getSelfGateway(DummyGateway.class));
        rpcService.registerGateway(
                dummyRpcEndpoint2.getAddress(),
                dummyRpcEndpoint2.getSelfGateway(DummyGateway.class));

        try {
            dummyRpcEndpoint.start();
            dummyRpcEndpoint2.start();

            settableLeaderRetrievalService.start(gatewayRetriever);

            final CompletableFuture<DummyGateway> gatewayFuture = gatewayRetriever.getFuture();

            assertThat(gatewayFuture).isNotDone();

            settableLeaderRetrievalService.notifyListener(
                    dummyRpcEndpoint.getAddress(), leaderSessionId);

            final DummyGateway dummyGateway =
                    gatewayFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

            assertThat(dummyGateway.getAddress()).isEqualTo(dummyRpcEndpoint.getAddress());
            FlinkAssertions.assertThatFuture(dummyGateway.foobar(TIMEOUT))
                    .eventuallySucceeds()
                    .isEqualTo(expectedValue);

            // elect a new leader
            settableLeaderRetrievalService.notifyListener(
                    dummyRpcEndpoint2.getAddress(), leaderSessionId);

            final CompletableFuture<DummyGateway> gatewayFuture2 = gatewayRetriever.getFuture();
            final DummyGateway dummyGateway2 =
                    gatewayFuture2.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

            assertThat(dummyGateway2.getAddress()).isEqualTo(dummyRpcEndpoint2.getAddress());
            FlinkAssertions.assertThatFuture(dummyGateway2.foobar(TIMEOUT))
                    .eventuallySucceeds()
                    .isEqualTo(expectedValue2);
        } finally {
            RpcUtils.terminateRpcEndpoint(dummyRpcEndpoint, dummyRpcEndpoint2);
        }
    }

    /** Testing RpcGateway. */
    public interface DummyGateway extends FencedRpcGateway<UUID> {
        CompletableFuture<String> foobar(@RpcTimeout Time timeout);
    }

    static class DummyRpcEndpoint extends RpcEndpoint implements DummyGateway {

        private final String value;

        protected DummyRpcEndpoint(RpcService rpcService, String endpointId, String value) {
            super(rpcService, endpointId);
            this.value = value;
        }

        @Override
        public CompletableFuture<String> foobar(Time timeout) {
            return CompletableFuture.completedFuture(value);
        }

        @Override
        public UUID getFencingToken() {
            return HighAvailabilityServices.DEFAULT_LEADER_ID;
        }
    }
}
