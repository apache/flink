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
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorSystem;
import akka.actor.Terminated;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link AkkaRpcService}.
 */
public class AkkaRpcServiceTest extends TestLogger {

	private static final Time TIMEOUT = Time.milliseconds(10000L);

	// ------------------------------------------------------------------------
	//  shared test members
	// ------------------------------------------------------------------------

	private static ActorSystem actorSystem;

	private static AkkaRpcService akkaRpcService;

	@BeforeClass
	public static void setup() {
		actorSystem = AkkaUtils.createDefaultActorSystem();
		akkaRpcService = new AkkaRpcService(actorSystem, AkkaRpcServiceConfiguration.defaultConfiguration());
	}

	@AfterClass
	public static void shutdown() throws InterruptedException, ExecutionException, TimeoutException {
		final CompletableFuture<Void> rpcTerminationFuture = akkaRpcService.stopService();
		final CompletableFuture<Terminated> actorSystemTerminationFuture = FutureUtils.toJava(actorSystem.terminate());

		FutureUtils
			.waitForAll(Arrays.asList(rpcTerminationFuture, actorSystemTerminationFuture))
			.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

		actorSystem = null;
		akkaRpcService = null;
	}

	// ------------------------------------------------------------------------
	//  tests
	// ------------------------------------------------------------------------

	@Test
	public void testScheduleRunnable() throws Exception {
		final OneShotLatch latch = new OneShotLatch();
		final long delay = 100L;
		final long start = System.nanoTime();

		ScheduledFuture<?> scheduledFuture = akkaRpcService.scheduleRunnable(latch::trigger, delay, TimeUnit.MILLISECONDS);

		scheduledFuture.get();

		assertTrue(latch.isTriggered());
		final long stop = System.nanoTime();

		assertTrue("call was not properly delayed", ((stop - start) / 1000000) >= delay);
	}

	/**
	 * Tests that the {@link AkkaRpcService} can execute runnables.
	 */
	@Test
	public void testExecuteRunnable() throws Exception {
		final OneShotLatch latch = new OneShotLatch();

		akkaRpcService.execute(latch::trigger);

		latch.await(30L, TimeUnit.SECONDS);
	}

	/**
	 * Tests that the {@link AkkaRpcService} can execute callables and returns their result as
	 * a {@link CompletableFuture}.
	 */
	@Test
	public void testExecuteCallable() throws Exception {
		final OneShotLatch latch = new OneShotLatch();
		final int expected = 42;

		CompletableFuture<Integer> result = akkaRpcService.execute(() -> {
			latch.trigger();
			return expected;
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
	 * Tests that we can wait for the termination of the rpc service.
	 */
	@Test(timeout = 60000)
	public void testTerminationFuture() throws Exception {
		final AkkaRpcService rpcService = startAkkaRpcService();

		CompletableFuture<Void> terminationFuture = rpcService.getTerminationFuture();

		assertFalse(terminationFuture.isDone());

		rpcService.stopService();

		terminationFuture.get();
	}

	/**
	 * Tests a simple scheduled runnable being executed by the RPC services scheduled executor
	 * service.
	 */
	@Test(timeout = 60000)
	public void testScheduledExecutorServiceSimpleSchedule() throws Exception {
		ScheduledExecutor scheduledExecutor = akkaRpcService.getScheduledExecutor();

		final OneShotLatch latch = new OneShotLatch();

		ScheduledFuture<?> future = scheduledExecutor.schedule(
			latch::trigger,
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
	public void testScheduledExecutorServicePeriodicSchedule() throws Exception {
		ScheduledExecutor scheduledExecutor = akkaRpcService.getScheduledExecutor();

		final int tries = 4;
		final long delay = 10L;
		final CountDownLatch countDownLatch = new CountDownLatch(tries);

		long currentTime = System.nanoTime();

		ScheduledFuture<?> future = scheduledExecutor.scheduleAtFixedRate(
			countDownLatch::countDown,
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
	public void testScheduledExecutorServiceWithFixedDelaySchedule() throws Exception {
		ScheduledExecutor scheduledExecutor = akkaRpcService.getScheduledExecutor();

		final int tries = 4;
		final long delay = 10L;
		final CountDownLatch countDownLatch = new CountDownLatch(tries);

		long currentTime = System.nanoTime();

		ScheduledFuture<?> future = scheduledExecutor.scheduleWithFixedDelay(
			countDownLatch::countDown,
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
			() -> {
				try {
					if (futureTask.isTriggered()) {
						shouldNotBeTriggeredLatch.trigger();
					} else {
						// first run
						futureTask.trigger();
						latch.await();
					}
				} catch (InterruptedException ignored) {
					// ignore
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

	/**
	 * Tests that the {@link AkkaRpcService} terminates all its RpcEndpoints when shutting down.
	 */
	@Test
	public void testAkkaRpcServiceShutDownWithRpcEndpoints() throws Exception {
		final AkkaRpcService akkaRpcService = startAkkaRpcService();

		try {
			final int numberActors = 5;

			CompletableFuture<Void> terminationFuture = akkaRpcService.getTerminationFuture();

			final Collection<CompletableFuture<Void>> onStopFutures = startStopNCountingAsynchronousOnStopEndpoints(akkaRpcService, numberActors);

			for (CompletableFuture<Void> onStopFuture : onStopFutures) {
				onStopFuture.complete(null);
			}

			terminationFuture.get();
			assertThat(akkaRpcService.getActorSystem().whenTerminated().isCompleted(), is(true));
		} finally {
			RpcUtils.terminateRpcService(akkaRpcService, TIMEOUT);
		}
	}

	/**
	 * Tests that {@link AkkaRpcService} terminates all its RpcEndpoints and also stops
	 * the underlying {@link ActorSystem} if one of the RpcEndpoints fails while stopping.
	 */
	@Test
	public void testAkkaRpcServiceShutDownWithFailingRpcEndpoints() throws Exception {
		final AkkaRpcService akkaRpcService = startAkkaRpcService();

		final int numberActors = 5;

		CompletableFuture<Void> terminationFuture = akkaRpcService.getTerminationFuture();

		final Collection<CompletableFuture<Void>> onStopFutures = startStopNCountingAsynchronousOnStopEndpoints(akkaRpcService, numberActors);

		Iterator<CompletableFuture<Void>> iterator = onStopFutures.iterator();

		for (int i = 0; i < numberActors - 1; i++) {
			iterator.next().complete(null);
		}

		iterator.next().completeExceptionally(new OnStopException("onStop exception occurred."));

		for (CompletableFuture<Void> onStopFuture : onStopFutures) {
			onStopFuture.complete(null);
		}

		try {
			terminationFuture.get();
			fail("Expected the termination future to complete exceptionally.");
		} catch (ExecutionException e) {
			assertThat(ExceptionUtils.findThrowable(e, OnStopException.class).isPresent(), is(true));
		}

		assertThat(akkaRpcService.getActorSystem().whenTerminated().isCompleted(), is(true));
	}

	private Collection<CompletableFuture<Void>> startStopNCountingAsynchronousOnStopEndpoints(AkkaRpcService akkaRpcService, int numberActors) throws Exception {
		final Collection<CompletableFuture<Void>> onStopFutures = new ArrayList<>(numberActors);

		final CountDownLatch countDownLatch = new CountDownLatch(numberActors);

		for (int i = 0; i < numberActors; i++) {
			CompletableFuture<Void> onStopFuture = new CompletableFuture<>();
			final CountingAsynchronousOnStopEndpoint endpoint = new CountingAsynchronousOnStopEndpoint(akkaRpcService, onStopFuture, countDownLatch);
			endpoint.start();
			onStopFutures.add(onStopFuture);
		}

		CompletableFuture<Void> terminationFuture = akkaRpcService.stopService();

		assertThat(terminationFuture.isDone(), is(false));
		assertThat(akkaRpcService.getActorSystem().whenTerminated().isCompleted(), is(false));

		countDownLatch.await();

		return onStopFutures;
	}

	@Nonnull
	private AkkaRpcService startAkkaRpcService() {
		final ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();
		return new AkkaRpcService(actorSystem, AkkaRpcServiceConfiguration.defaultConfiguration());
	}

	private static class CountingAsynchronousOnStopEndpoint extends AkkaRpcActorTest.AsynchronousOnStopEndpoint {

		private final CountDownLatch countDownLatch;

		protected CountingAsynchronousOnStopEndpoint(RpcService rpcService, CompletableFuture<Void> onStopFuture, CountDownLatch countDownLatch) {
			super(rpcService, onStopFuture);
			this.countDownLatch = countDownLatch;
		}

		@Override
		public CompletableFuture<Void> onStop() {
			countDownLatch.countDown();
			return super.onStop();
		}
	}

	private static class OnStopException extends FlinkException {
		private static final long serialVersionUID = 7136609202083168954L;

		public OnStopException(String message) {
			super(message);
		}
	}
}
