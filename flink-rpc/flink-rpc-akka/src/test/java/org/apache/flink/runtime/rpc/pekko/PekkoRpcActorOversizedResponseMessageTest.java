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

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.exceptions.RpcException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.FunctionWithException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the over sized response message handling of the {@link PekkoRpcActor}. */
class PekkoRpcActorOversizedResponseMessageTest {

    private static final int FRAMESIZE = 32000;

    private static final String OVERSIZED_PAYLOAD = new String(new byte[FRAMESIZE]);

    private static final String PAYLOAD = "Hello";

    private static RpcService rpcService1;

    private static RpcService rpcService2;

    @BeforeAll
    static void setupClass() throws Exception {
        final Configuration configuration = new Configuration();
        // some tests explicitly test local communication where no serialization should occur
        configuration.set(AkkaOptions.FORCE_RPC_INVOCATION_SERIALIZATION, false);
        configuration.setString(AkkaOptions.FRAMESIZE, FRAMESIZE + " b");

        rpcService1 =
                PekkoRpcServiceUtils.remoteServiceBuilder(configuration, "localhost", 0)
                        .createAndStart();
        rpcService2 =
                PekkoRpcServiceUtils.remoteServiceBuilder(configuration, "localhost", 0)
                        .createAndStart();
    }

    @AfterAll
    static void teardownClass() throws Exception {
        RpcUtils.terminateRpcService(rpcService1, rpcService2);
    }

    @Test
    void testOverSizedResponseMsgAsync() throws Exception {
        assertThatThrownBy(
                        () ->
                                runRemoteMessageResponseTest(
                                        OVERSIZED_PAYLOAD, this::requestMessageAsync))
                .hasCauseInstanceOf(RpcException.class)
                .extracting(ExceptionUtils::stripExecutionException)
                .isInstanceOf(RpcException.class)
                .extracting(Throwable::getMessage)
                .satisfies(message -> assertThat(message).contains(String.valueOf(FRAMESIZE)));
    }

    @Test
    void testNormalSizedResponseMsgAsync() throws Exception {
        final String message = runRemoteMessageResponseTest(PAYLOAD, this::requestMessageAsync);
        assertThat(message).isEqualTo(PAYLOAD);
    }

    @Test
    void testNormalSizedResponseMsgSync() throws Exception {
        final String message =
                runRemoteMessageResponseTest(PAYLOAD, MessageRpcGateway::messageSync);
        assertThat(message).isEqualTo(PAYLOAD);
    }

    @Test
    void testOverSizedResponseMsgSync() throws Exception {
        assertThatThrownBy(
                        () ->
                                runRemoteMessageResponseTest(
                                        OVERSIZED_PAYLOAD, MessageRpcGateway::messageSync))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                RpcException.class, String.valueOf(FRAMESIZE)));
    }

    /**
     * Tests that we can send arbitrarily large objects when communicating locally with the rpc
     * endpoint.
     */
    @Test
    void testLocalOverSizedResponseMsgSync() throws Exception {
        final String message =
                runLocalMessageResponseTest(OVERSIZED_PAYLOAD, MessageRpcGateway::messageSync);
        assertThat(message).isEqualTo(OVERSIZED_PAYLOAD);
    }

    /**
     * Tests that we can send arbitrarily large objects when communicating locally with the rpc
     * endpoint.
     */
    @Test
    void testLocalOverSizedResponseMsgAsync() throws Exception {
        final String message =
                runLocalMessageResponseTest(OVERSIZED_PAYLOAD, this::requestMessageAsync);
        assertThat(message).isEqualTo(OVERSIZED_PAYLOAD);
    }

    private String requestMessageAsync(MessageRpcGateway messageRpcGateway) throws Exception {
        CompletableFuture<String> messageFuture = messageRpcGateway.messageAsync();
        return messageFuture.get();
    }

    private <T> T runRemoteMessageResponseTest(
            String payload, FunctionWithException<MessageRpcGateway, T, Exception> rpcCall)
            throws Exception {
        final MessageRpcEndpoint rpcEndpoint = new MessageRpcEndpoint(rpcService1, payload);

        try {
            rpcEndpoint.start();

            MessageRpcGateway rpcGateway =
                    rpcService2.connect(rpcEndpoint.getAddress(), MessageRpcGateway.class).get();

            return rpcCall.apply(rpcGateway);
        } finally {
            RpcUtils.terminateRpcEndpoint(rpcEndpoint);
        }
    }

    private <T> T runLocalMessageResponseTest(
            String payload, FunctionWithException<MessageRpcGateway, T, Exception> rpcCall)
            throws Exception {
        final MessageRpcEndpoint rpcEndpoint = new MessageRpcEndpoint(rpcService1, payload);

        try {
            rpcEndpoint.start();

            MessageRpcGateway rpcGateway =
                    rpcService1.connect(rpcEndpoint.getAddress(), MessageRpcGateway.class).get();

            return rpcCall.apply(rpcGateway);
        } finally {
            RpcUtils.terminateRpcEndpoint(rpcEndpoint);
        }
    }

    // -------------------------------------------------------------------------

    interface MessageRpcGateway extends RpcGateway {
        CompletableFuture<String> messageAsync();

        String messageSync() throws RpcException;
    }

    static class MessageRpcEndpoint extends RpcEndpoint implements MessageRpcGateway {

        @Nonnull private final String message;

        MessageRpcEndpoint(RpcService rpcService, @Nonnull String message) {
            super(rpcService);
            this.message = message;
        }

        @Override
        public CompletableFuture<String> messageAsync() {
            return CompletableFuture.completedFuture(message);
        }

        @Override
        public String messageSync() throws RpcException {
            return message;
        }
    }
}
