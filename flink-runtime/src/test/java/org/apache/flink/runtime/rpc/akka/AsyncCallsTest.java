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

import org.junit.AfterClass;
import org.junit.Test;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class AsyncCallsTest {

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
	public void testScheduleWithNoDelay() throws Exception {

		// to collect all the thread references
		final BlockingQueue<Thread> queue = new LinkedBlockingQueue<>();

		TestEndpoint testEndpoint = new TestEndpoint(akkaRpcService, queue);
		TestGateway gateway = testEndpoint.getSelf();

		// a bunch of gateway calls
		gateway.someCall();
		gateway.anotherCall();
		gateway.someCall();

		// run something asynchronously
		testEndpoint.runAsync(new Runnable() {
			@Override
			public void run() {
				queue.add(Thread.currentThread());
			}
		});
	
		Future<String> result = testEndpoint.callAsync(new Callable<String>() {
			@Override
			public String call() throws Exception {
				return "test";
			}
		}, new Timeout(30, TimeUnit.SECONDS));
		String str = Await.result(result, new FiniteDuration(30, TimeUnit.SECONDS));
		assertEquals("test", str);

		// validate that all calls were on the same thread
		final Thread ref = queue.take();
		Thread cmp;
		while ((cmp = queue.poll()) != null) {
			assertEquals("Calls were handled by different threads", ref, cmp);
		}

		akkaRpcService.stopServer(testEndpoint.getSelf());
	}

	@Test
	public void testScheduleWithDelay() throws Exception {

		// to collect all the thread references
		final BlockingQueue<Thread> queue = new LinkedBlockingQueue<>();
		final long delay = 200;

		TestEndpoint testEndpoint = new TestEndpoint(akkaRpcService, queue);

		// run something asynchronously
		testEndpoint.runAsync(new Runnable() {
			@Override
			public void run() {
				queue.add(Thread.currentThread());
			}
		});

		final long start = System.nanoTime();

		testEndpoint.scheduleRunAsync(new Runnable() {
			@Override
			public void run() {
				queue.add(Thread.currentThread());
			}
		}, delay);

		// validate that all calls were on the same thread
		final Thread ref = queue.take();
		final Thread cmp = queue.take();
		final long stop = System.nanoTime();

		assertEquals("Calls were handled by different threads", ref, cmp);
		assertTrue("call was not properly delayed", ((stop - start) / 1000000) >= delay);
	}

	// ------------------------------------------------------------------------
	//  test RPC endpoint
	// ------------------------------------------------------------------------
	
	interface TestGateway extends RpcGateway {

		void someCall();

		void anotherCall();
	}

	@SuppressWarnings("unused")
	public static class TestEndpoint extends RpcEndpoint<TestGateway> {

		private final BlockingQueue<Thread> queue;

		public TestEndpoint(RpcService rpcService, BlockingQueue<Thread> queue) {
			super(rpcService);
			this.queue = queue;
		}

		@Override
		public Class<TestGateway> getSelfGatewayType() {
			return TestGateway.class;
		}

		@RpcMethod
		public void someCall() {
			queue.add(Thread.currentThread());
		}

		@RpcMethod
		public void anotherCall() {
			queue.add(Thread.currentThread());
		}
	}
}
