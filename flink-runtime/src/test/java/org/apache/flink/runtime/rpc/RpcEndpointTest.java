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

package org.apache.flink.runtime.rpc;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorSystem;
import akka.actor.Terminated;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for the RpcEndpoint and its self gateways.
 */
public class RpcEndpointTest extends TestLogger {

	private static final Time TIMEOUT = Time.seconds(10L);
	private static ActorSystem actorSystem = null;
	private static RpcService rpcService = null;

	@BeforeClass
	public static void setup() {
		actorSystem = AkkaUtils.createDefaultActorSystem();
		rpcService = new AkkaRpcService(actorSystem, TIMEOUT);
	}

	@AfterClass
	public static void teardown() throws Exception {

		final CompletableFuture<Void> rpcTerminationFuture = rpcService.stopService();
		final CompletableFuture<Terminated> actorSystemTerminationFuture = FutureUtils.toJava(actorSystem.terminate());

		FutureUtils
			.waitForAll(Arrays.asList(rpcTerminationFuture, actorSystemTerminationFuture))
			.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
	}

	/**
	 * Tests that we can obtain the self gateway from a RpcEndpoint and can interact with
	 * it via the self gateway.
	 */
	@Test
	public void testSelfGateway() throws Exception {
		int expectedValue = 1337;
		BaseEndpoint baseEndpoint = new BaseEndpoint(rpcService, expectedValue);

		try {
			baseEndpoint.start();

			BaseGateway baseGateway = baseEndpoint.getSelfGateway(BaseGateway.class);

			CompletableFuture<Integer> foobar = baseGateway.foobar();

			assertEquals(Integer.valueOf(expectedValue), foobar.get());
		} finally {
			baseEndpoint.shutDown();
		}
	}

	/**
	 * Tests that we cannot accidentally obtain a wrong self gateway type which is
	 * not implemented by the RpcEndpoint.
	 */
	@Test(expected = RuntimeException.class)
	public void testWrongSelfGateway() throws Exception {
		int expectedValue = 1337;
		BaseEndpoint baseEndpoint = new BaseEndpoint(rpcService, expectedValue);

		try {
			baseEndpoint.start();

			DifferentGateway differentGateway = baseEndpoint.getSelfGateway(DifferentGateway.class);

			fail("Expected to fail with a RuntimeException since we requested the wrong gateway type.");
		} finally {
			baseEndpoint.shutDown();
		}
	}

	/**
	 * Tests that we can extend existing RpcEndpoints and can communicate with them via the
	 * self gateways.
	 */
	@Test
	public void testEndpointInheritance() throws Exception {
		int foobar = 1;
		int barfoo = 2;
		String foo = "foobar";

		ExtendedEndpoint endpoint = new ExtendedEndpoint(rpcService, foobar, barfoo, foo);

		try {
			endpoint.start();

			BaseGateway baseGateway = endpoint.getSelfGateway(BaseGateway.class);
			ExtendedGateway extendedGateway = endpoint.getSelfGateway(ExtendedGateway.class);
			DifferentGateway differentGateway = endpoint.getSelfGateway(DifferentGateway.class);

			assertEquals(Integer.valueOf(foobar), baseGateway.foobar().get());
			assertEquals(Integer.valueOf(foobar), extendedGateway.foobar().get());

			assertEquals(Integer.valueOf(barfoo), extendedGateway.barfoo().get());
			assertEquals(foo, differentGateway.foo().get());
		} finally {
			endpoint.shutDown();
		}
	}

	public interface BaseGateway extends RpcGateway {
		CompletableFuture<Integer> foobar();
	}

	public interface ExtendedGateway extends BaseGateway {
		CompletableFuture<Integer> barfoo();
	}

	public interface DifferentGateway extends RpcGateway {
		CompletableFuture<String> foo();
	}

	public static class BaseEndpoint extends RpcEndpoint implements BaseGateway {

		private final int foobarValue;

		protected BaseEndpoint(RpcService rpcService, int foobarValue) {
			super(rpcService);

			this.foobarValue = foobarValue;
		}

		@Override
		public CompletableFuture<Integer> foobar() {
			return CompletableFuture.completedFuture(foobarValue);
		}

		@Override
		public CompletableFuture<Void> postStop() {
			return CompletableFuture.completedFuture(null);
		}
	}

	public static class ExtendedEndpoint extends BaseEndpoint implements ExtendedGateway, DifferentGateway {

		private final int barfooValue;

		private final String fooString;

		protected ExtendedEndpoint(RpcService rpcService, int foobarValue, int barfooValue, String fooString) {
			super(rpcService, foobarValue);

			this.barfooValue = barfooValue;
			this.fooString = fooString;
		}

		@Override
		public CompletableFuture<Integer> barfoo() {
			return CompletableFuture.completedFuture(barfooValue);
		}

		@Override
		public CompletableFuture<String> foo() {
			return CompletableFuture.completedFuture(fooString);
		}
	}
}
