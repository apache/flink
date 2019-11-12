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
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.exceptions.HandshakeException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the handshake between rpc endpoints.
 */
public class AkkaRpcActorHandshakeTest extends TestLogger {

	private static final Time timeout = Time.seconds(10L);

	private static AkkaRpcService akkaRpcService1;
	private static AkkaRpcService akkaRpcService2;
	private static WrongVersionAkkaRpcService wrongVersionAkkaRpcService;

	@BeforeClass
	public static void setupClass() {
		final ActorSystem actorSystem1 = AkkaUtils.createDefaultActorSystem();
		final ActorSystem actorSystem2 = AkkaUtils.createDefaultActorSystem();
		final ActorSystem wrongVersionActorSystem = AkkaUtils.createDefaultActorSystem();

		AkkaRpcServiceConfiguration akkaRpcServiceConfig = AkkaRpcServiceConfiguration.defaultConfiguration();
		akkaRpcService1 = new AkkaRpcService(actorSystem1, akkaRpcServiceConfig);
		akkaRpcService2 = new AkkaRpcService(actorSystem2, akkaRpcServiceConfig);
		wrongVersionAkkaRpcService = new WrongVersionAkkaRpcService(
			wrongVersionActorSystem, AkkaRpcServiceConfiguration.defaultConfiguration());
	}

	@AfterClass
	public static void teardownClass() throws Exception {
		final Collection<CompletableFuture<?>> terminationFutures = new ArrayList<>(3);

		terminationFutures.add(akkaRpcService1.stopService());
		terminationFutures.add(akkaRpcService2.stopService());
		terminationFutures.add(wrongVersionAkkaRpcService.stopService());

		FutureUtils.waitForAll(terminationFutures).get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
	}

	@Test
	public void testVersionMatchBetweenRpcComponents() throws Exception {
		AkkaRpcActorTest.DummyRpcEndpoint rpcEndpoint = new AkkaRpcActorTest.DummyRpcEndpoint(akkaRpcService1);
		final int value = 42;
		rpcEndpoint.setFoobar(value);

		rpcEndpoint.start();

		try {
			final AkkaRpcActorTest.DummyRpcGateway dummyRpcGateway = akkaRpcService2.connect(rpcEndpoint.getAddress(), AkkaRpcActorTest.DummyRpcGateway.class).get();

			assertThat(dummyRpcGateway.foobar().get(), equalTo(value));
		} finally {
			RpcUtils.terminateRpcEndpoint(rpcEndpoint, timeout);
		}
	}

	@Test
	public void testVersionMismatchBetweenRpcComponents() throws Exception {
		AkkaRpcActorTest.DummyRpcEndpoint rpcEndpoint = new AkkaRpcActorTest.DummyRpcEndpoint(akkaRpcService1);

		rpcEndpoint.start();

		try {
			try {
				wrongVersionAkkaRpcService.connect(rpcEndpoint.getAddress(), AkkaRpcActorTest.DummyRpcGateway.class).get();
				fail("Expected HandshakeException.");
			} catch (ExecutionException ee) {
				assertThat(ExceptionUtils.stripExecutionException(ee), instanceOf(HandshakeException.class));
			}
		} finally {
			RpcUtils.terminateRpcEndpoint(rpcEndpoint, timeout);
		}
	}

	/**
	 * Tests that we receive a HandshakeException when connecting to a rpc endpoint which
	 * does not support the requested rpc gateway.
	 */
	@Test
	public void testWrongGatewayEndpointConnection() throws Exception {
		AkkaRpcActorTest.DummyRpcEndpoint rpcEndpoint = new AkkaRpcActorTest.DummyRpcEndpoint(akkaRpcService1);

		rpcEndpoint.start();

		CompletableFuture<WrongRpcGateway> futureGateway = akkaRpcService2.connect(rpcEndpoint.getAddress(), WrongRpcGateway.class);

		try {
			futureGateway.get(timeout.getSize(), timeout.getUnit());
			fail("We expected a HandshakeException.");
		} catch (ExecutionException executionException) {
			assertThat(ExceptionUtils.stripExecutionException(executionException), instanceOf(HandshakeException.class));
		} finally {
			RpcUtils.terminateRpcEndpoint(rpcEndpoint, timeout);
		}
	}

	private static class WrongVersionAkkaRpcService extends AkkaRpcService {

		WrongVersionAkkaRpcService(ActorSystem actorSystem, AkkaRpcServiceConfiguration configuration) {
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
