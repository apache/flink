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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.exceptions.HandshakeException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import akka.actor.ActorSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the handshake between rpc endpoints. */
class AkkaRpcActorHandshakeTest {

    private static AkkaRpcService akkaRpcService1;
    private static AkkaRpcService akkaRpcService2;
    private static WrongVersionAkkaRpcService wrongVersionAkkaRpcService;

    @BeforeAll
    static void setupClass() {
        final ActorSystem actorSystem1 = AkkaUtils.createDefaultActorSystem();
        final ActorSystem actorSystem2 = AkkaUtils.createDefaultActorSystem();
        final ActorSystem wrongVersionActorSystem = AkkaUtils.createDefaultActorSystem();

        AkkaRpcServiceConfiguration akkaRpcServiceConfig =
                AkkaRpcServiceConfiguration.defaultConfiguration();
        akkaRpcService1 = new AkkaRpcService(actorSystem1, akkaRpcServiceConfig);
        akkaRpcService2 = new AkkaRpcService(actorSystem2, akkaRpcServiceConfig);
        wrongVersionAkkaRpcService =
                new WrongVersionAkkaRpcService(
                        wrongVersionActorSystem,
                        AkkaRpcServiceConfiguration.defaultConfiguration());
    }

    @AfterAll
    static void teardownClass() throws Exception {
        final Collection<CompletableFuture<?>> terminationFutures = new ArrayList<>(3);

        terminationFutures.add(akkaRpcService1.stopService());
        terminationFutures.add(akkaRpcService2.stopService());
        terminationFutures.add(wrongVersionAkkaRpcService.stopService());

        FutureUtils.waitForAll(terminationFutures).get();
    }

    @Test
    void testVersionMatchBetweenRpcComponents() throws Exception {
        AkkaRpcActorTest.DummyRpcEndpoint rpcEndpoint =
                new AkkaRpcActorTest.DummyRpcEndpoint(akkaRpcService1);
        final int value = 42;
        rpcEndpoint.setFoobar(value);

        rpcEndpoint.start();

        try {
            final AkkaRpcActorTest.DummyRpcGateway dummyRpcGateway =
                    akkaRpcService2
                            .connect(
                                    rpcEndpoint.getAddress(),
                                    AkkaRpcActorTest.DummyRpcGateway.class)
                            .get();

            assertThat(dummyRpcGateway.foobar().get()).isEqualTo(value);
        } finally {
            RpcUtils.terminateRpcEndpoint(rpcEndpoint);
        }
    }

    @Test
    void testVersionMismatchBetweenRpcComponents() throws Exception {
        AkkaRpcActorTest.DummyRpcEndpoint rpcEndpoint =
                new AkkaRpcActorTest.DummyRpcEndpoint(akkaRpcService1);

        rpcEndpoint.start();

        try {
            assertThatThrownBy(
                            () ->
                                    wrongVersionAkkaRpcService
                                            .connect(
                                                    rpcEndpoint.getAddress(),
                                                    AkkaRpcActorTest.DummyRpcGateway.class)
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
        AkkaRpcActorTest.DummyRpcEndpoint rpcEndpoint =
                new AkkaRpcActorTest.DummyRpcEndpoint(akkaRpcService1);

        rpcEndpoint.start();

        CompletableFuture<WrongRpcGateway> futureGateway =
                akkaRpcService2.connect(rpcEndpoint.getAddress(), WrongRpcGateway.class);

        try {
            assertThatThrownBy(() -> futureGateway.get())
                    .extracting(ExceptionUtils::stripExecutionException)
                    .isInstanceOf(HandshakeException.class);

        } finally {
            RpcUtils.terminateRpcEndpoint(rpcEndpoint);
        }
    }

    private static class WrongVersionAkkaRpcService extends AkkaRpcService {

        WrongVersionAkkaRpcService(
                ActorSystem actorSystem, AkkaRpcServiceConfiguration configuration) {
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
