/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.util.SerializedValue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for remote AkkaRpcActors. */
class RemoteAkkaRpcActorTest {

    private static AkkaRpcService rpcService;
    private static AkkaRpcService otherRpcService;

    private static final Configuration configuration = new Configuration();

    @BeforeAll
    static void setupClass() throws Exception {
        rpcService =
                AkkaRpcServiceUtils.createRemoteRpcService(
                        configuration, "localhost", "0", null, Optional.empty());

        otherRpcService =
                AkkaRpcServiceUtils.createRemoteRpcService(
                        configuration, "localhost", "0", null, Optional.empty());
    }

    @AfterAll
    static void teardownClass() throws InterruptedException, ExecutionException, TimeoutException {
        RpcUtils.terminateRpcService(rpcService, otherRpcService);
    }

    @Test
    void canRespondWithNullValueRemotely() throws Exception {
        try (final AkkaRpcActorTest.NullRespondingEndpoint nullRespondingEndpoint =
                new AkkaRpcActorTest.NullRespondingEndpoint(rpcService)) {
            nullRespondingEndpoint.start();

            final AkkaRpcActorTest.NullRespondingGateway rpcGateway =
                    otherRpcService
                            .connect(
                                    nullRespondingEndpoint.getAddress(),
                                    AkkaRpcActorTest.NullRespondingGateway.class)
                            .join();

            final CompletableFuture<Integer> nullValuedResponseFuture = rpcGateway.foobar();

            assertThat(nullValuedResponseFuture.join()).isNull();
        }
    }

    @Test
    void canRespondWithSynchronousNullValueRemotely() throws Exception {
        try (final AkkaRpcActorTest.NullRespondingEndpoint nullRespondingEndpoint =
                new AkkaRpcActorTest.NullRespondingEndpoint(rpcService)) {
            nullRespondingEndpoint.start();

            final AkkaRpcActorTest.NullRespondingGateway rpcGateway =
                    otherRpcService
                            .connect(
                                    nullRespondingEndpoint.getAddress(),
                                    AkkaRpcActorTest.NullRespondingGateway.class)
                            .join();

            final Integer value = rpcGateway.synchronousFoobar();

            assertThat(value).isNull();
        }
    }

    @Test
    void canRespondWithSerializedValueRemotely() throws Exception {
        try (final AkkaRpcActorTest.SerializedValueRespondingEndpoint endpoint =
                new AkkaRpcActorTest.SerializedValueRespondingEndpoint(rpcService)) {
            endpoint.start();

            final AkkaRpcActorTest.SerializedValueRespondingGateway remoteGateway =
                    otherRpcService
                            .connect(
                                    endpoint.getAddress(),
                                    AkkaRpcActorTest.SerializedValueRespondingGateway.class)
                            .join();

            assertThat(remoteGateway.getSerializedValueSynchronously())
                    .isEqualTo(AkkaRpcActorTest.SerializedValueRespondingEndpoint.SERIALIZED_VALUE);

            final CompletableFuture<SerializedValue<String>> responseFuture =
                    remoteGateway.getSerializedValue();

            assertThat(responseFuture.get())
                    .isEqualTo(AkkaRpcActorTest.SerializedValueRespondingEndpoint.SERIALIZED_VALUE);
        }
    }

    @Test
    void failsRpcResultImmediatelyIfEndpointIsStopped() throws Exception {
        try (final AkkaRpcActorTest.SerializedValueRespondingEndpoint endpoint =
                new AkkaRpcActorTest.SerializedValueRespondingEndpoint(rpcService)) {
            endpoint.start();

            final AkkaRpcActorTest.SerializedValueRespondingGateway gateway =
                    otherRpcService
                            .connect(
                                    endpoint.getAddress(),
                                    AkkaRpcActorTest.SerializedValueRespondingGateway.class)
                            .join();

            endpoint.close();

            // the rpc result should not fail because of a TimeoutException
            assertThatThrownBy(() -> gateway.getSerializedValue().join())
                    .satisfies(
                            FlinkAssertions.anyCauseMatches(RecipientUnreachableException.class));
        }
    }

    @Test
    void failsRpcResultImmediatelyIfRemoteRpcServiceIsNotAvailable() throws Exception {
        final AkkaRpcService toBeClosedRpcService =
                AkkaRpcServiceUtils.createRemoteRpcService(
                        configuration, "localhost", "0", null, Optional.empty());
        try (final AkkaRpcActorTest.SerializedValueRespondingEndpoint endpoint =
                new AkkaRpcActorTest.SerializedValueRespondingEndpoint(toBeClosedRpcService)) {
            endpoint.start();

            final AkkaRpcActorTest.SerializedValueRespondingGateway gateway =
                    otherRpcService
                            .connect(
                                    endpoint.getAddress(),
                                    AkkaRpcActorTest.SerializedValueRespondingGateway.class)
                            .join();

            toBeClosedRpcService.stopService().join();

            // the rpc result should not fail because of a TimeoutException
            assertThatThrownBy(() -> gateway.getSerializedValue().join())
                    .satisfies(
                            FlinkAssertions.anyCauseMatches(RecipientUnreachableException.class));
        } finally {
            RpcUtils.terminateRpcService(toBeClosedRpcService);
        }
    }
}
