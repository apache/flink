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

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.exceptions.HandshakeException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.pekko.actor.ActorSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the handshake between rpc endpoints. */
class PekkoRpcActorHandshakeTest {

    private static PekkoRpcService rpcService1;
    private static PekkoRpcService rpcService2;
    private static WrongVersionPekkoRpcService wrongVersionRpcService;

    @BeforeAll
    static void setupClass() {
        final ActorSystem actorSystem1 = PekkoUtils.createDefaultActorSystem();
        final ActorSystem actorSystem2 = PekkoUtils.createDefaultActorSystem();
        final ActorSystem wrongVersionActorSystem = PekkoUtils.createDefaultActorSystem();

        PekkoRpcServiceConfiguration rpcServiceConfig =
                PekkoRpcServiceConfiguration.defaultConfiguration();
        rpcService1 = new PekkoRpcService(actorSystem1, rpcServiceConfig);
        rpcService2 = new PekkoRpcService(actorSystem2, rpcServiceConfig);
        wrongVersionRpcService =
                new WrongVersionPekkoRpcService(
                        wrongVersionActorSystem,
                        PekkoRpcServiceConfiguration.defaultConfiguration());
    }

    @AfterAll
    static void teardownClass() throws Exception {
        final Collection<CompletableFuture<?>> terminationFutures = new ArrayList<>(3);

        terminationFutures.add(rpcService1.closeAsync());
        terminationFutures.add(rpcService2.closeAsync());
        terminationFutures.add(wrongVersionRpcService.closeAsync());

        FutureUtils.waitForAll(terminationFutures).get();
    }

    @Test
    void testVersionMatchBetweenRpcComponents() throws Exception {
        PekkoRpcActorTest.DummyRpcEndpoint rpcEndpoint =
                new PekkoRpcActorTest.DummyRpcEndpoint(rpcService1);
        final int value = 42;
        rpcEndpoint.setFoobar(value);

        rpcEndpoint.start();

        try {
            final PekkoRpcActorTest.DummyRpcGateway dummyRpcGateway =
                    rpcService2
                            .connect(
                                    rpcEndpoint.getAddress(),
                                    PekkoRpcActorTest.DummyRpcGateway.class)
                            .get();

            assertThat(dummyRpcGateway.foobar().get()).isEqualTo(value);
        } finally {
            RpcUtils.terminateRpcEndpoint(rpcEndpoint);
        }
    }

    @Test
    void testVersionMismatchBetweenRpcComponents() throws Exception {
        PekkoRpcActorTest.DummyRpcEndpoint rpcEndpoint =
                new PekkoRpcActorTest.DummyRpcEndpoint(rpcService1);

        rpcEndpoint.start();

        try {
            assertThatThrownBy(
                            () ->
                                    wrongVersionRpcService
                                            .connect(
                                                    rpcEndpoint.getAddress(),
                                                    PekkoRpcActorTest.DummyRpcGateway.class)
                                            .get())
                    .extracting(ExceptionUtils::stripExecutionException)
                    .isInstanceOf(HandshakeException.class);
        } finally {
            RpcUtils.terminateRpcEndpoint(rpcEndpoint);
        }
    }

    /**
     * Tests that we receive a HandshakeException when connecting to a rpc endpoint which does not
     * support the requested rpc gateway.
     */
    @Test
    void testWrongGatewayEndpointConnection() throws Exception {
        PekkoRpcActorTest.DummyRpcEndpoint rpcEndpoint =
                new PekkoRpcActorTest.DummyRpcEndpoint(rpcService1);

        rpcEndpoint.start();

        CompletableFuture<WrongRpcGateway> futureGateway =
                rpcService2.connect(rpcEndpoint.getAddress(), WrongRpcGateway.class);

        try {
            assertThatThrownBy(() -> futureGateway.get())
                    .extracting(ExceptionUtils::stripExecutionException)
                    .isInstanceOf(HandshakeException.class);

        } finally {
            RpcUtils.terminateRpcEndpoint(rpcEndpoint);
        }
    }

    private static class WrongVersionPekkoRpcService extends PekkoRpcService {

        WrongVersionPekkoRpcService(
                ActorSystem actorSystem, PekkoRpcServiceConfiguration configuration) {
            super(actorSystem, configuration);
        }

        @Override
        protected int getVersion() {
            return -1;
        }
    }

    private interface WrongRpcGateway extends RpcGateway {
        CompletableFuture<Boolean> barfoo();

        void tell(String message);
    }
}
