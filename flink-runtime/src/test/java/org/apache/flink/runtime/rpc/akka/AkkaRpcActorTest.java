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

import akka.actor.ActorSystem;
import akka.util.Timeout;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.TestLogger;
import org.hamcrest.core.Is;
import org.junit.AfterClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class AkkaRpcActorTest extends TestLogger {

	// ------------------------------------------------------------------------
	//  shared test members
	// ------------------------------------------------------------------------

	private static ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();

	private static Timeout timeout = new Timeout(10000, TimeUnit.MILLISECONDS);

	private static AkkaRpcService akkaRpcService =
		new AkkaRpcService(actorSystem, timeout);

	@AfterClass
	public static void shutdown() {
		akkaRpcService.stopService();
		actorSystem.shutdown();
		actorSystem.awaitTermination();
	}

	/**
	 * Tests that the rpc endpoint and the associated rpc gateway have the same addresses.
	 * @throws Exception
	 */
	@Test
	public void testAddressResolution() throws Exception {
		DummyRpcEndpoint rpcEndpoint = new DummyRpcEndpoint(akkaRpcService);

		Future<DummyRpcGateway> futureRpcGateway = akkaRpcService.connect(rpcEndpoint.getAddress(), DummyRpcGateway.class);

		DummyRpcGateway rpcGateway = Await.result(futureRpcGateway, timeout.duration());

		assertEquals(rpcEndpoint.getAddress(), rpcGateway.getAddress());
	}

	/**
	 * Tests that the {@link AkkaRpcActor} stashes messages until the corresponding
	 * {@link RpcEndpoint} has been started.
	 */
	@Test
	public void testMessageStashing() throws Exception {
		int expectedValue = 1337;

		DummyRpcEndpoint rpcEndpoint = new DummyRpcEndpoint(akkaRpcService);

		DummyRpcGateway rpcGateway = rpcEndpoint.getSelf();

		// this message should not be processed until we've started the rpc endpoint
		Future<Integer> result = rpcGateway.foobar();

		// set a new value which we expect to be returned
		rpcEndpoint.setFoobar(expectedValue);

		// now process the rpc
		rpcEndpoint.start();

		Integer actualValue = Await.result(result, timeout.duration());

		assertThat("The new foobar value should have been returned.", actualValue, Is.is(expectedValue));

		rpcEndpoint.shutDown();
	}

	private interface DummyRpcGateway extends RpcGateway {
		Future<Integer> foobar();
	}

	private static class DummyRpcEndpoint extends RpcEndpoint<DummyRpcGateway> {

		private volatile int _foobar = 42;

		protected DummyRpcEndpoint(RpcService rpcService) {
			super(rpcService);
		}

		@RpcMethod
		public int foobar() {
			return _foobar;
		}

		public void setFoobar(int value) {
			_foobar = value;
		}
	}
}
