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

package org.apache.flink.runtime.webmonitor.retriever;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.TestingRpcGateway;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test cases for the {@link LeaderGatewayRetriever}. */
class LeaderGatewayRetrieverTest {

    /** Tests that the gateway retrieval is retried in case of a failure. */
    @Test
    void testGatewayRetrievalFailures() throws Exception {
        final String address = "localhost";
        final UUID leaderId = UUID.randomUUID();

        RpcGateway rpcGateway = TestingRpcGateway.newBuilder().build();

        TestingLeaderGatewayRetriever leaderGatewayRetriever =
                new TestingLeaderGatewayRetriever(rpcGateway);
        SettableLeaderRetrievalService settableLeaderRetrievalService =
                new SettableLeaderRetrievalService();

        settableLeaderRetrievalService.start(leaderGatewayRetriever);

        CompletableFuture<RpcGateway> gatewayFuture = leaderGatewayRetriever.getFuture();

        // this triggers the first gateway retrieval attempt
        settableLeaderRetrievalService.notifyListener(address, leaderId);

        FlinkAssertions.assertThatFuture(gatewayFuture)
                .as("The first future should have been failed.")
                .eventuallyFailsWith(ExecutionException.class);

        // the second attempt should fail as well
        assertThat((leaderGatewayRetriever.getNow())).isNotPresent();

        // the third attempt should succeed
        assertThat(leaderGatewayRetriever.getNow()).hasValue(rpcGateway);
    }

    private static class TestingLeaderGatewayRetriever extends LeaderGatewayRetriever<RpcGateway> {

        private final RpcGateway rpcGateway;
        private int retrievalAttempt = 0;

        private TestingLeaderGatewayRetriever(RpcGateway rpcGateway) {
            this.rpcGateway = rpcGateway;
        }

        @Override
        protected CompletableFuture<RpcGateway> createGateway(
                CompletableFuture<Tuple2<String, UUID>> leaderFuture) {
            CompletableFuture<RpcGateway> result;

            if (retrievalAttempt < 2) {
                result =
                        FutureUtils.completedExceptionally(
                                new FlinkException("Could not resolve the leader gateway."));
            } else {
                result = CompletableFuture.completedFuture(rpcGateway);
            }

            retrievalAttempt++;

            return result;
        }
    }
}
