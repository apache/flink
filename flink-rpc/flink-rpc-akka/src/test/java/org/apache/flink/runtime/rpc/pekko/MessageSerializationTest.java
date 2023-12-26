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
import org.apache.flink.runtime.rpc.Local;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that pekko rpc invocation messages are properly serialized and errors reported. */
class MessageSerializationTest {
    private static RpcService rpcService1;
    private static RpcService rpcService2;

    private static final int maxFrameSize = 32000;

    @BeforeAll
    static void setup() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(AkkaOptions.FRAMESIZE, maxFrameSize + "b");

        rpcService1 =
                PekkoRpcServiceUtils.remoteServiceBuilder(configuration, "localhost", 0)
                        .createAndStart();
        rpcService2 =
                PekkoRpcServiceUtils.remoteServiceBuilder(configuration, "localhost", 0)
                        .createAndStart();
    }

    @AfterAll
    static void teardown() throws InterruptedException, ExecutionException, TimeoutException {
        final Collection<CompletableFuture<?>> terminationFutures = new ArrayList<>(2);

        terminationFutures.add(rpcService1.closeAsync());
        terminationFutures.add(rpcService2.closeAsync());

        FutureUtils.waitForAll(terminationFutures).get();
    }

    /** Tests that a local rpc call with a non serializable argument can be executed. */
    @Test
    void testNonSerializableLocalMessageTransfer() throws Exception {
        LinkedBlockingQueue<Object> linkedBlockingQueue = new LinkedBlockingQueue<>();
        TestEndpoint testEndpoint = new TestEndpoint(rpcService1, linkedBlockingQueue);
        testEndpoint.start();

        TestGateway testGateway = testEndpoint.getSelfGateway(TestGateway.class);

        NonSerializableObject expected = new NonSerializableObject(42);

        testGateway.foobar(expected);

        assertThat(linkedBlockingQueue.take()).isSameAs(expected);
    }

    /**
     * Tests that a remote rpc call with a non-serializable argument fails with an {@link
     * IOException} (or an {@link java.lang.reflect.UndeclaredThrowableException} if the method
     * declaration does not include the {@link IOException} as throwable).
     */
    @Test
    void testNonSerializableRemoteMessageTransfer() throws Exception {
        LinkedBlockingQueue<Object> linkedBlockingQueue = new LinkedBlockingQueue<>();

        TestEndpoint testEndpoint = new TestEndpoint(rpcService1, linkedBlockingQueue);
        testEndpoint.start();

        String address = testEndpoint.getAddress();

        TestGateway remoteGateway = rpcService2.connect(address, TestGateway.class).get();

        assertThatThrownBy(() -> remoteGateway.foobar(new Object()))
                .isInstanceOf(IOException.class);
    }

    /** Tests that a remote rpc call with a serializable argument can be successfully executed. */
    @Test
    void testSerializableRemoteMessageTransfer() throws Exception {
        LinkedBlockingQueue<Object> linkedBlockingQueue = new LinkedBlockingQueue<>();

        TestEndpoint testEndpoint = new TestEndpoint(rpcService1, linkedBlockingQueue);
        testEndpoint.start();

        String address = testEndpoint.getAddress();

        CompletableFuture<TestGateway> remoteGatewayFuture =
                rpcService2.connect(address, TestGateway.class);

        TestGateway remoteGateway = remoteGatewayFuture.get();

        int expected = 42;

        remoteGateway.foobar(expected);

        assertThat(linkedBlockingQueue.take()).isEqualTo(expected);
    }

    /** Tests that a message which exceeds the maximum frame size will cause timeout exception. */
    @Test
    void testMaximumFramesizeRemoteMessageTransfer() throws Throwable {
        LinkedBlockingQueue<Object> linkedBlockingQueue = new LinkedBlockingQueue<>();

        TestEndpoint testEndpoint = new TestEndpoint(rpcService1, linkedBlockingQueue);
        testEndpoint.start();

        String address = testEndpoint.getAddress();

        TestGateway remoteGateway = rpcService2.connect(address, TestGateway.class).get();

        int bufferSize = maxFrameSize + 1;
        byte[] buffer = new byte[bufferSize];

        CompletableFuture<Void> completableFuture = remoteGateway.foobar(buffer);

        assertThatThrownBy(completableFuture::get).hasCauseInstanceOf(TimeoutException.class);
    }

    private interface TestGateway extends RpcGateway {
        @Local
        CompletableFuture<Void> foobar(Object object) throws IOException, InterruptedException;
    }

    private static class TestEndpoint extends RpcEndpoint implements TestGateway {

        private final LinkedBlockingQueue<Object> queue;

        protected TestEndpoint(RpcService rpcService, LinkedBlockingQueue<Object> queue) {
            super(rpcService);
            this.queue = queue;
        }

        @Override
        public CompletableFuture<Void> foobar(Object object) throws InterruptedException {
            queue.put(object);
            return CompletableFuture.completedFuture(null);
        }
    }

    private static class NonSerializableObject {
        private final Object object = new Object();
        private final int value;

        NonSerializableObject(int value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof NonSerializableObject) {
                NonSerializableObject nonSerializableObject = (NonSerializableObject) obj;

                return value == nonSerializableObject.value;
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return value * 41;
        }
    }
}
