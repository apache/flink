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

import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RpcAddressBasedGatewayRetriever}. */
class RpcAddressBasedGatewayRetrieverTest {
    private static TestingRpcService rpcService;

    @BeforeAll
    static void setup() {
        rpcService = new TestingRpcService();
    }

    @AfterAll
    static void teardown() throws InterruptedException, ExecutionException, TimeoutException {
        if (rpcService != null) {
            RpcUtils.terminateRpcService(rpcService);
            rpcService = null;
        }
    }

    @Test
    void testRetrieverGateway() throws Exception {
        final String expectedValue = "gateway";

        RpcAddressBasedGatewayRetriever<DummyRpcGateway> gatewayRetriever =
                new RpcAddressBasedGatewayRetriever<>(rpcService, DummyRpcGateway.class);

        DummyRpcEndpoint dummyRpcEndpoint = new DummyRpcEndpoint(rpcService, expectedValue);

        rpcService.registerGateway(
                dummyRpcEndpoint.getAddress(),
                dummyRpcEndpoint.getSelfGateway(DummyRpcGateway.class));

        try {
            dummyRpcEndpoint.start();

            final CompletableFuture<DummyRpcGateway> gatewayFuture =
                    gatewayRetriever.getFutureFromAddress(dummyRpcEndpoint.getAddress());

            assertThat(gatewayFuture).isDone();

            DummyRpcGateway retrievedGateway = gatewayFuture.get();

            assertThat(retrievedGateway.getAddress()).isEqualTo(dummyRpcEndpoint.getAddress());
            assertThat(expectedValue).isEqualTo(retrievedGateway.getValue().get());
        } finally {
            RpcUtils.terminateRpcEndpoint(dummyRpcEndpoint);
        }
    }

    public interface DummyRpcGateway extends RpcGateway {
        CompletableFuture<String> getValue();
    }

    static class DummyRpcEndpoint extends RpcEndpoint implements DummyRpcGateway {

        private final String value;

        protected DummyRpcEndpoint(RpcService rpcService, String value) {
            super(rpcService);
            this.value = value;
        }

        @Override
        public CompletableFuture<String> getValue() {
            return CompletableFuture.completedFuture(value);
        }
    }
}
