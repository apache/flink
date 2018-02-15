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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.exceptions.FencingTokenException;
import org.apache.flink.testutils.category.Flip6;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import akka.actor.Terminated;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static org.junit.Assert.*;

@Category(Flip6.class)
public class AsyncCallsTest extends TestLogger {

	// ------------------------------------------------------------------------
	//  shared test members
	// ------------------------------------------------------------------------

	private static final ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();

	private static final Time timeout = Time.seconds(10L);

	private static final AkkaRpcService akkaRpcService =
			new AkkaRpcService(actorSystem, Time.milliseconds(10000L));

	@AfterClass
	public static void shutdown() throws InterruptedException, ExecutionException, TimeoutException {
		final CompletableFuture<Void> rpcTerminationFuture = akkaRpcService.stopService();
		final CompletableFuture<Terminated> actorSystemTerminationFuture = FutureUtils.toJava(actorSystem.terminate());

		FutureUtils
			.waitForAll(Arrays.asList(rpcTerminationFuture, actorSystemTerminationFuture))
			.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
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

	/**
	 * Tests that async code is not executed if the fencing token changes.
	 */
	@Test
	public void testRunAsyncWithFencing() throws Exception {
		final Time shortTimeout = Time.milliseconds(100L);
		final UUID newFencingToken = UUID.randomUUID();
		final CompletableFuture<UUID> resultFuture = new CompletableFuture<>();

		testRunAsync(
			endpoint -> {
				endpoint.runAsync(
					() -> resultFuture.complete(endpoint.getFencingToken()));

				return resultFuture;
			},
			newFencingToken);

		try {
			resultFuture.get(shortTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			fail("The async run operation should not complete since it is filtered out due to the changed fencing token.");
		} catch (TimeoutException ignored) {}
	}

	/**
	 * Tests that code can be executed in the main thread without respecting the fencing token.
	 */
	@Test
	public void testRunAsyncWithoutFencing() throws Exception {
		final CompletableFuture<UUID> resultFuture = new CompletableFuture<>();
		final UUID newFencingToken = UUID.randomUUID();

		testRunAsync(
			endpoint -> {
				endpoint.runAsyncWithoutFencing(
					() -> resultFuture.complete(endpoint.getFencingToken()));
				return resultFuture;
			},
			newFencingToken);

		assertEquals(newFencingToken, resultFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS));
	}

	/**
	 * Tests that async callables are not executed if the fencing token changes.
	 */
	@Test
	public void testCallAsyncWithFencing() throws Exception {
		final UUID newFencingToken = UUID.randomUUID();

		CompletableFuture<Boolean> resultFuture = testRunAsync(
			endpoint -> endpoint.callAsync(() -> true, timeout),
			newFencingToken);

		try {
			resultFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			fail("The async call operation should fail due to the changed fencing token.");
		} catch (ExecutionException e) {
			assertTrue(ExceptionUtils.stripExecutionException(e) instanceof FencingTokenException);
		}
	}

	/**
	 * Tests that async callables can be executed in the main thread without checking the fencing token.
	 */
	@Test
	public void testCallAsyncWithoutFencing() throws Exception {
		final UUID newFencingToken = UUID.randomUUID();

		CompletableFuture<Boolean> resultFuture = testRunAsync(
			endpoint -> endpoint.callAsyncWithoutFencing(() -> true, timeout),
			newFencingToken);

		assertTrue(resultFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS));
	}

	private static <T> CompletableFuture<T> testRunAsync(Function<FencedTestEndpoint, CompletableFuture<T>> runAsyncCall, UUID newFencingToken) throws Exception {
		final UUID initialFencingToken = UUID.randomUUID();
		final OneShotLatch enterSetNewFencingToken = new OneShotLatch();
		final OneShotLatch triggerSetNewFencingToken = new OneShotLatch();
		final FencedTestEndpoint fencedTestEndpoint = new FencedTestEndpoint(
			akkaRpcService,
			initialFencingToken,
			enterSetNewFencingToken,
			triggerSetNewFencingToken);
		final FencedTestGateway fencedTestGateway = fencedTestEndpoint.getSelfGateway(FencedTestGateway.class);

		try {
			fencedTestEndpoint.start();

			CompletableFuture<Acknowledge> newFencingTokenFuture = fencedTestGateway.setNewFencingToken(newFencingToken, timeout);

			assertFalse(newFencingTokenFuture.isDone());

			assertEquals(initialFencingToken, fencedTestEndpoint.getFencingToken());

			CompletableFuture<T> result = runAsyncCall.apply(fencedTestEndpoint);

			enterSetNewFencingToken.await();

			triggerSetNewFencingToken.trigger();

			newFencingTokenFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			return result;
		} finally {
			fencedTestEndpoint.shutDown();
			fencedTestEndpoint.getTerminationFuture().get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}
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

		@Override
		public CompletableFuture<Void> postStop() {
			return CompletableFuture.completedFuture(null);
		}
	}

	public interface FencedTestGateway extends FencedRpcGateway<UUID> {
		CompletableFuture<Acknowledge> setNewFencingToken(UUID fencingToken, @RpcTimeout Time timeout);
	}

	public static class FencedTestEndpoint extends FencedRpcEndpoint<UUID> implements FencedTestGateway {

		private final OneShotLatch enteringSetNewFencingToken;
		private final OneShotLatch triggerSetNewFencingToken;

		protected FencedTestEndpoint(
				RpcService rpcService,
				UUID initialFencingToken,
				OneShotLatch enteringSetNewFencingToken,
				OneShotLatch triggerSetNewFencingToken) {
			super(rpcService);

			this.enteringSetNewFencingToken = enteringSetNewFencingToken;
			this.triggerSetNewFencingToken = triggerSetNewFencingToken;

			// make it look as if we are running in the main thread
			currentMainThread.set(Thread.currentThread());

			try {
				setFencingToken(initialFencingToken);
			} finally {
				currentMainThread.set(null);
			}
		}

		@Override
		public CompletableFuture<Acknowledge> setNewFencingToken(UUID fencingToken, Time timeout) {
			enteringSetNewFencingToken.trigger();
			try {
				triggerSetNewFencingToken.await();
			} catch (InterruptedException e) {
				throw new RuntimeException("TriggerSetNewFencingToken OneShotLatch was interrupted.");
			}

			setFencingToken(fencingToken);

			return CompletableFuture.completedFuture(Acknowledge.get());
		}

		@Override
		public CompletableFuture<Void> postStop() {
			return CompletableFuture.completedFuture(null);
		}
	}
}
