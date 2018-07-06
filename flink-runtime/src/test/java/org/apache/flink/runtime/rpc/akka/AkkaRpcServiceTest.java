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
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorSystem;
import akka.actor.Terminated;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AkkaRpcServiceTest extends TestLogger {

	// ------------------------------------------------------------------------
	//  shared test members
	// ------------------------------------------------------------------------

	private static ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();

	private static final Time timeout = Time.milliseconds(10000);

	private static AkkaRpcService akkaRpcService =
			new AkkaRpcService(actorSystem, timeout);

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
	public void testScheduleRunnable() throws Exception {
		final OneShotLatch latch = new OneShotLatch();
		final long delay = 100;
		final long start = System.nanoTime();

		ScheduledFuture<?> scheduledFuture = akkaRpcService.scheduleRunnable(new Runnable() {
			@Override
			public void run() {
				latch.trigger();
			}
		}, delay, TimeUnit.MILLISECONDS);

		scheduledFuture.get();

		assertTrue(latch.isTriggered());
		final long stop = System.nanoTime();

		assertTrue("call was not properly delayed", ((stop - start) / 1000000) >= delay);
	}

	/**
	 * Tests that the {@link AkkaRpcService} can execute runnables
	 */
	@Test
	public void testExecuteRunnable() throws Exception {
		final OneShotLatch latch = new OneShotLatch();

		akkaRpcService.execute(() -> latch.trigger());

		latch.await(30L, TimeUnit.SECONDS);
	}

	/**
	 * Tests that the {@link AkkaRpcService} can execute callables and returns their result as
	 * a {@link CompletableFuture}.
	 */
	@Test
	public void testExecuteCallable() throws InterruptedException, ExecutionException, TimeoutException {
		final OneShotLatch latch = new OneShotLatch();
		final int expected = 42;

		CompletableFuture<Integer> result = akkaRpcService.execute(new Callable<Integer>() {
			@Override
			public Integer call() throws Exception {
				latch.trigger();
				return expected;
			}
		});

		int actual = result.get(30L, TimeUnit.SECONDS);

		assertEquals(expected, actual);
		assertTrue(latch.isTriggered());
	}

	@Test
	public void testGetAddress() {
		assertEquals(AkkaUtils.getAddress(actorSystem).host().get(), akkaRpcService.getAddress());
	}

	@Test
	public void testGetPort() {
		assertEquals(AkkaUtils.getAddress(actorSystem).port().get(), akkaRpcService.getPort());
	}

	/**
	 * Tests that we can wait for the termination of the rpc service
	 */
	@Test(timeout = 60000)
	public void testTerminationFuture() throws Exception {
		final ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();
		final AkkaRpcService rpcService = new AkkaRpcService(actorSystem, Time.milliseconds(1000));

		CompletableFuture<Void> terminationFuture = rpcService.getTerminationFuture();

		assertFalse(terminationFuture.isDone());

		CompletableFuture.runAsync(rpcService::stopService, actorSystem.dispatcher());

		terminationFuture.get();
	}

	/**
	 * Tests a simple scheduled runnable being executed by the RPC services scheduled executor
	 * service.
	 */
	@Test(timeout = 60000)
	public void testScheduledExecutorServiceSimpleSchedule() throws ExecutionException, InterruptedException {
		ScheduledExecutor scheduledExecutor = akkaRpcService.getScheduledExecutor();

		final OneShotLatch latch = new OneShotLatch();

		ScheduledFuture<?> future = scheduledExecutor.schedule(
			new Runnable() {
				@Override
				public void run() {
					latch.trigger();
				}
			},
			10L,
			TimeUnit.MILLISECONDS);

		future.get();

		// once the future is completed, then the latch should have been triggered
		assertTrue(latch.isTriggered());
	}

	/**
	 * Tests that the RPC service's scheduled executor service can execute runnables at a fixed
	 * rate.
	 */
	@Test(timeout = 60000)
	public void testScheduledExecutorServicePeriodicSchedule() throws ExecutionException, InterruptedException {
		ScheduledExecutor scheduledExecutor = akkaRpcService.getScheduledExecutor();

		final int tries = 4;
		final long delay = 10L;
		final CountDownLatch countDownLatch = new CountDownLatch(tries);

		long currentTime = System.nanoTime();

		ScheduledFuture<?> future = scheduledExecutor.scheduleAtFixedRate(
			new Runnable() {
				@Override
				public void run() {
					countDownLatch.countDown();
				}
			},
			delay,
			delay,
			TimeUnit.MILLISECONDS);

		assertTrue(!future.isDone());

		countDownLatch.await();

		// the future should not complete since we have a periodic task
		assertTrue(!future.isDone());

		long finalTime = System.nanoTime() - currentTime;

		// the processing should have taken at least delay times the number of count downs.
		assertTrue(finalTime >= tries * delay);

		future.cancel(true);
	}

	/**
	 * Tests that the RPC service's scheduled executor service can execute runnable with a fixed
	 * delay.
	 */
	@Test(timeout = 60000)
	public void testScheduledExecutorServiceWithFixedDelaySchedule() throws ExecutionException, InterruptedException {
		ScheduledExecutor scheduledExecutor = akkaRpcService.getScheduledExecutor();

		final int tries = 4;
		final long delay = 10L;
		final CountDownLatch countDownLatch = new CountDownLatch(tries);

		long currentTime = System.nanoTime();

		ScheduledFuture<?> future = scheduledExecutor.scheduleWithFixedDelay(
			new Runnable() {
				@Override
				public void run() {
					countDownLatch.countDown();
				}
			},
			delay,
			delay,
			TimeUnit.MILLISECONDS);

		assertTrue(!future.isDone());

		countDownLatch.await();

		// the future should not complete since we have a periodic task
		assertTrue(!future.isDone());

		long finalTime = System.nanoTime() - currentTime;

		// the processing should have taken at least delay times the number of count downs.
		assertTrue(finalTime >= tries * delay);

		future.cancel(true);
	}

	/**
	 * Tests that canceling the returned future will stop the execution of the scheduled runnable.
	 */
	@Test
	public void testScheduledExecutorServiceCancelWithFixedDelay() throws InterruptedException {
		ScheduledExecutor scheduledExecutor = akkaRpcService.getScheduledExecutor();

		long delay = 10L;

		final OneShotLatch futureTask = new OneShotLatch();
		final OneShotLatch latch = new OneShotLatch();
		final OneShotLatch shouldNotBeTriggeredLatch = new OneShotLatch();

		ScheduledFuture<?> future = scheduledExecutor.scheduleWithFixedDelay(
			new Runnable() {
				@Override
				public void run() {
					try {
						if (!futureTask.isTriggered()) {
							// first run
							futureTask.trigger();
							latch.await();
						} else {
							shouldNotBeTriggeredLatch.trigger();
						}
					} catch (InterruptedException e) {
						// ignore
					}
				}
			},
			delay,
			delay,
			TimeUnit.MILLISECONDS);

		// wait until we're in the runnable
		futureTask.await();

		// cancel the scheduled future
		future.cancel(false);

		latch.trigger();

		try {
			shouldNotBeTriggeredLatch.await(5 * delay, TimeUnit.MILLISECONDS);
			fail("The shouldNotBeTriggeredLatch should never be triggered.");
		} catch (TimeoutException e) {
			// expected
		}
	}

	@Test
	public void testVersionIncompatibility() {
	}
}
