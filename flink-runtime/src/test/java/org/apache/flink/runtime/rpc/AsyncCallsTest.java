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

import akka.actor.ActorSystem;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.*;

public class AsyncCallsTest extends TestLogger {

	// ------------------------------------------------------------------------
	//  shared test members
	// ------------------------------------------------------------------------

	private static final ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();

	private static final AkkaRpcService akkaRpcService =
			new AkkaRpcService(actorSystem, Time.milliseconds(10000L));

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
		final ReentrantLock lock = new ReentrantLock();
		final AtomicBoolean concurrentAccess = new AtomicBoolean(false);

		TestEndpoint testEndpoint = new TestEndpoint(akkaRpcService, lock);
		testEndpoint.start();
		TestGateway gateway = testEndpoint.getSelfGateway(TestGateway.class);

		// a bunch of gateway calls
		gateway.someCall();
		gateway.anotherCall();
		gateway.someCall();

		// run something asynchronously
		for (int i = 0; i < 10000; i++) {
			testEndpoint.runAsync(new Runnable() {
				@Override
				public void run() {
					boolean holdsLock = lock.tryLock();
					if (holdsLock) {
						lock.unlock();
					} else {
						concurrentAccess.set(true);
					}
				}
			});
		}
	
		CompletableFuture<String> result = testEndpoint.callAsync(
			() -> {
				boolean holdsLock = lock.tryLock();
				if (holdsLock) {
					lock.unlock();
				} else {
					concurrentAccess.set(true);
				}
				return "test";
			},
			Time.seconds(30L));

		String str = result.get(30, TimeUnit.SECONDS);
		assertEquals("test", str);

		// validate that no concurrent access happened
		assertFalse("Rpc Endpoint had concurrent access", testEndpoint.hasConcurrentAccess());
		assertFalse("Rpc Endpoint had concurrent access", concurrentAccess.get());

		testEndpoint.shutDown();
	}

	@Test
	public void testScheduleWithDelay() throws Exception {

		// to collect all the thread references
		final ReentrantLock lock = new ReentrantLock();
		final AtomicBoolean concurrentAccess = new AtomicBoolean(false);
		final OneShotLatch latch = new OneShotLatch();

		final long delay = 100;

		TestEndpoint testEndpoint = new TestEndpoint(akkaRpcService, lock);
		testEndpoint.start();

		// run something asynchronously
		testEndpoint.runAsync(new Runnable() {
			@Override
			public void run() {
				boolean holdsLock = lock.tryLock();
				if (holdsLock) {
					lock.unlock();
				} else {
					concurrentAccess.set(true);
				}
			}
		});

		final long start = System.nanoTime();

		testEndpoint.scheduleRunAsync(new Runnable() {
			@Override
			public void run() {
				boolean holdsLock = lock.tryLock();
				if (holdsLock) {
					lock.unlock();
				} else {
					concurrentAccess.set(true);
				}
				latch.trigger();
			}
		}, delay, TimeUnit.MILLISECONDS);

		latch.await();
		final long stop = System.nanoTime();

		// validate that no concurrent access happened
		assertFalse("Rpc Endpoint had concurrent access", testEndpoint.hasConcurrentAccess());
		assertFalse("Rpc Endpoint had concurrent access", concurrentAccess.get());

		assertTrue("call was not properly delayed", ((stop - start) / 1_000_000) >= delay);
	}

	// ------------------------------------------------------------------------
	//  test RPC endpoint
	// ------------------------------------------------------------------------
	
	public interface TestGateway extends RpcGateway {

		void someCall();

		void anotherCall();
	}

	@SuppressWarnings("unused")
	public static class TestEndpoint extends RpcEndpoint implements TestGateway {

		private final ReentrantLock lock;

		private volatile boolean concurrentAccess;

		public TestEndpoint(RpcService rpcService, ReentrantLock lock) {
			super(rpcService);
			this.lock = lock;
		}

		@Override
		public void someCall() {
			boolean holdsLock = lock.tryLock();
			if (holdsLock) {
				lock.unlock();
			} else {
				concurrentAccess = true;
			}
		}

		@Override
		public void anotherCall() {
			boolean holdsLock = lock.tryLock();
			if (holdsLock) {
				lock.unlock();
			} else {
				concurrentAccess = true;
			}
		}

		public boolean hasConcurrentAccess() {
			return concurrentAccess;
		}
	}
}
