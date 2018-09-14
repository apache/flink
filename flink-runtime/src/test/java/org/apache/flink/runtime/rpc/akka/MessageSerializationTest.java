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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import org.hamcrest.core.Is;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests that akka rpc invocation messages are properly serialized and errors reported
 */
public class MessageSerializationTest extends TestLogger {
	private static ActorSystem actorSystem1;
	private static ActorSystem actorSystem2;
	private static AkkaRpcService akkaRpcService1;
	private static AkkaRpcService akkaRpcService2;

	private static final Time timeout = Time.seconds(10L);
	private static final int maxFrameSize = 32000;

	@BeforeClass
	public static void setup() {
		Config akkaConfig = AkkaUtils.getDefaultAkkaConfig();
		Config modifiedAkkaConfig = akkaConfig.withValue(AkkaRpcService.MAXIMUM_FRAME_SIZE_PATH, ConfigValueFactory.fromAnyRef(maxFrameSize + "b"));

		actorSystem1 = AkkaUtils.createActorSystem(modifiedAkkaConfig);
		actorSystem2 = AkkaUtils.createActorSystem(modifiedAkkaConfig);

		akkaRpcService1 = new AkkaRpcService(actorSystem1, timeout);
		akkaRpcService2 = new AkkaRpcService(actorSystem2, timeout);
	}

	@AfterClass
	public static void teardown() throws InterruptedException, ExecutionException, TimeoutException {
		final Collection<CompletableFuture<?>> terminationFutures = new ArrayList<>(4);

		terminationFutures.add(akkaRpcService1.stopService());
		terminationFutures.add(FutureUtils.toJava(actorSystem1.terminate()));
		terminationFutures.add(akkaRpcService2.stopService());
		terminationFutures.add(FutureUtils.toJava(actorSystem2.terminate()));

		FutureUtils
			.waitForAll(terminationFutures)
			.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
	}

	/**
	 * Tests that a local rpc call with a non serializable argument can be executed.
	 */
	@Test
	public void testNonSerializableLocalMessageTransfer() throws Exception {
		LinkedBlockingQueue<Object> linkedBlockingQueue = new LinkedBlockingQueue<>();
		TestEndpoint testEndpoint = new TestEndpoint(akkaRpcService1, linkedBlockingQueue);
		testEndpoint.start();

		TestGateway testGateway = testEndpoint.getSelfGateway(TestGateway.class);

		NonSerializableObject expected = new NonSerializableObject(42);

		testGateway.foobar(expected);

		assertThat(linkedBlockingQueue.take(), Is.<Object>is(expected));
	}

	/**
	 * Tests that a remote rpc call with a non-serializable argument fails with an
	 * {@link IOException} (or an {@link java.lang.reflect.UndeclaredThrowableException} if the
	 * the method declaration does not include the {@link IOException} as throwable).
	 */
	@Test(expected = IOException.class)
	public void testNonSerializableRemoteMessageTransfer() throws Exception {
		LinkedBlockingQueue<Object> linkedBlockingQueue = new LinkedBlockingQueue<>();

		TestEndpoint testEndpoint = new TestEndpoint(akkaRpcService1, linkedBlockingQueue);
		testEndpoint.start();

		String address = testEndpoint.getAddress();

		CompletableFuture<TestGateway> remoteGatewayFuture = akkaRpcService2.connect(address, TestGateway.class);

		TestGateway remoteGateway = remoteGatewayFuture.get(timeout.getSize(), timeout.getUnit());

		remoteGateway.foobar(new Object());

		fail("Should have failed because Object is not serializable.");
	}

	/**
	 * Tests that a remote rpc call with a serializable argument can be successfully executed.
	 */
	@Test
	public void testSerializableRemoteMessageTransfer() throws Exception {
		LinkedBlockingQueue<Object> linkedBlockingQueue = new LinkedBlockingQueue<>();

		TestEndpoint testEndpoint = new TestEndpoint(akkaRpcService1, linkedBlockingQueue);
		testEndpoint.start();

		String address = testEndpoint.getAddress();

		CompletableFuture<TestGateway> remoteGatewayFuture = akkaRpcService2.connect(address, TestGateway.class);

		TestGateway remoteGateway = remoteGatewayFuture.get(timeout.getSize(), timeout.getUnit());

		int expected = 42;

		remoteGateway.foobar(expected);

		assertThat(linkedBlockingQueue.take(), Is.<Object>is(expected));
	}

	/**
	 * Tests that a message which exceeds the maximum frame size is detected and a corresponding
	 * exception is thrown.
	 */
	@Test(expected = IOException.class)
	public void testMaximumFramesizeRemoteMessageTransfer() throws Exception {
		LinkedBlockingQueue<Object> linkedBlockingQueue = new LinkedBlockingQueue<>();

		TestEndpoint testEndpoint = new TestEndpoint(akkaRpcService1, linkedBlockingQueue);
		testEndpoint.start();

		String address = testEndpoint.getAddress();

		CompletableFuture<TestGateway> remoteGatewayFuture = akkaRpcService2.connect(address, TestGateway.class);

		TestGateway remoteGateway = remoteGatewayFuture.get(timeout.getSize(), timeout.getUnit());

		int bufferSize = maxFrameSize + 1;
		byte[] buffer = new byte[bufferSize];

		remoteGateway.foobar(buffer);

		fail("Should have failed due to exceeding the maximum framesize.");
	}

	private interface TestGateway extends RpcGateway {
		void foobar(Object object) throws IOException, InterruptedException;
	}

	private static class TestEndpoint extends RpcEndpoint implements TestGateway {

		private final LinkedBlockingQueue<Object> queue;

		protected TestEndpoint(RpcService rpcService, LinkedBlockingQueue<Object> queue) {
			super(rpcService);
			this.queue = queue;
		}

		@Override
		public void foobar(Object object) throws InterruptedException {
			queue.put(object);
		}

		@Override
		public CompletableFuture<Void> postStop() {
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
