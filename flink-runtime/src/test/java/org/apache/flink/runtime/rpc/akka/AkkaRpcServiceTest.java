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

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.jobmaster.JobMaster;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManager;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Test;

import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AkkaRpcServiceTest extends TestLogger {

	// ------------------------------------------------------------------------
	//  shared test members
	// ------------------------------------------------------------------------

	private static ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();

	private static AkkaRpcService akkaRpcService =
			new AkkaRpcService(actorSystem, new Timeout(10000, TimeUnit.MILLISECONDS));

	@AfterClass
	public static void shutdown() {
		akkaRpcService.stopService();
		actorSystem.shutdown();
	}

	// ------------------------------------------------------------------------
	//  tests
	// ------------------------------------------------------------------------

	@Test
	public void testScheduleRunnable() throws Exception {
		final OneShotLatch latch = new OneShotLatch();
		final long delay = 100;
		final long start = System.nanoTime();

		akkaRpcService.scheduleRunnable(new Runnable() {
			@Override
			public void run() {
				latch.trigger();
			}
		}, delay, TimeUnit.MILLISECONDS);

		latch.await();
		final long stop = System.nanoTime();

		assertTrue("call was not properly delayed", ((stop - start) / 1000000) >= delay);
	}

	private interface TestRpcGateway extends RpcGateway {
		Future<Integer> inc(Integer input);
	}

	private static class TestRpcEndpoint extends RpcEndpoint<TestRpcGateway> {

		/**
		 * Initializes the RPC endpoint.
		 *
		 * @param rpcService The RPC server that dispatches calls to this RPC endpoint.
		 */
		protected TestRpcEndpoint(RpcService rpcService) {
			super(rpcService);
		}

		@RpcMethod
		public Integer inc(Integer input) {
			return input + 1;
		}
	}

	@Test
	public void testAkkaRpcGateway() throws Exception {
		ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();
		AkkaRpcService akkaRpcService = new AkkaRpcService(actorSystem, new Timeout(10, TimeUnit.SECONDS));
		TestRpcEndpoint endpoint = new TestRpcEndpoint(akkaRpcService);
		String address = endpoint.getAddress();
		endpoint.start();

		FiniteDuration akkaTimeout = new FiniteDuration(10, TimeUnit.SECONDS);
		TestRpcGateway client1 =
			Await.result(akkaRpcService.connect(address, TestRpcGateway.class), akkaTimeout);
		TestRpcGateway client2 =
			Await.result(akkaRpcService.connect(address, TestRpcGateway.class), akkaTimeout);
		assertEquals(akkaRpcService.getAddress(client1), akkaRpcService.getAddress(client2));
		assertEquals(new Integer(11), Await.result(client1.inc(10), akkaTimeout));
		assertEquals(new Integer(21), Await.result(client2.inc(20), akkaTimeout));
	}

	@Test
	public void testMultiActorSystem() throws Exception {
		ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();
		ActorSystem actorSystem2 = AkkaUtils.createDefaultActorSystem();
		ActorSystem actorSystem3 = AkkaUtils.createDefaultActorSystem();
		AkkaRpcService akkaRpcService = new AkkaRpcService(actorSystem, new Timeout(10, TimeUnit.SECONDS));
		AkkaRpcService akkaRpcService2 = new AkkaRpcService(actorSystem2, new Timeout(10, TimeUnit.SECONDS));
		AkkaRpcService akkaRpcService3 = new AkkaRpcService(actorSystem3, new Timeout(10, TimeUnit.SECONDS));
		TestRpcEndpoint endpoint = new TestRpcEndpoint(akkaRpcService);
		TestRpcEndpoint endpoint2 = new TestRpcEndpoint(akkaRpcService2);
		endpoint.start();
		endpoint2.start();

		FiniteDuration akkaTimeout = new FiniteDuration(10, TimeUnit.SECONDS);
		TestRpcGateway client1 =
			Await.result(akkaRpcService3.connect(endpoint.getAddress(), TestRpcGateway.class), akkaTimeout);
		TestRpcGateway client2 =
			Await.result(akkaRpcService3.connect(endpoint2.getAddress(), TestRpcGateway.class), akkaTimeout);
		assertEquals(endpoint.getAddress(), akkaRpcService3.getAddress(client1));
		assertEquals(endpoint2.getAddress(), akkaRpcService3.getAddress(client2));
		assertEquals(new Integer(11), Await.result(client1.inc(10), akkaTimeout));
		assertEquals(new Integer(21), Await.result(client2.inc(20), akkaTimeout));

		boolean caught = false;
		try {
			akkaRpcService.getAddress(client2);
		} catch (IllegalArgumentException e) {
			caught = true;
		}
		assertTrue(caught);

	}

	@Test
	public void testStopServer() throws Exception {
		ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();
		AkkaRpcService akkaRpcService = new AkkaRpcService(actorSystem, new Timeout(10, TimeUnit.SECONDS));
		TestRpcEndpoint endpoint = new TestRpcEndpoint(akkaRpcService);
		String address = endpoint.getAddress();
		endpoint.start();

		// ensure server is working
		FiniteDuration akkaTimeout = new FiniteDuration(10, TimeUnit.SECONDS);
		TestRpcGateway client1 =
			Await.result(akkaRpcService.connect(address, TestRpcGateway.class), akkaTimeout);
		assertEquals(new Integer(11), Await.result(client1.inc(10), akkaTimeout));

		// try to stop a client, server still work
		akkaRpcService.stopServer(client1);
		assertEquals(new Integer(11), Await.result(client1.inc(10), akkaTimeout));

		// stop the server, server stopped
		akkaRpcService.stopServer(endpoint.getSelf());

		boolean caught = false;
		try {
			Await.result(client1.inc(10), akkaTimeout);
		} catch (Exception e) {
			caught = true;
		}
		assertTrue(caught);

	}
}
