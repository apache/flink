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
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorTest;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceConfiguration;

import akka.actor.ActorSystem;
import akka.actor.Terminated;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link RpcEndpoint.MainThreadExecutor}.
 */
public class RpcEndpointMainThreadExecutorTest extends ScheduledExecutorTest {

	private static final Time TIMEOUT = Time.seconds(10L);
	private static ActorSystem actorSystem = null;
	private static RpcService rpcService = null;

	private RpcEndpoint rpcEndpoint;

	@BeforeClass
	public static void setupClass() {
		actorSystem = AkkaUtils.createDefaultActorSystem();
		rpcService = new AkkaRpcService(actorSystem, AkkaRpcServiceConfiguration.defaultConfiguration());
	}

	@AfterClass
	public static void teardownClass() throws Exception {
		final CompletableFuture<Void> rpcTerminationFuture = rpcService.stopService();
		final CompletableFuture<Terminated> actorSystemTerminationFuture = FutureUtils.toJava(actorSystem.terminate());

		FutureUtils
			.waitForAll(Arrays.asList(rpcTerminationFuture, actorSystemTerminationFuture))
			.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
	}

	@Override
	public ScheduledExecutor createScheduledExecutor() {
		rpcEndpoint = new RpcEndpointTest.BaseEndpoint(rpcService, 1024);
		rpcEndpoint.start();
		return rpcEndpoint.getMainThreadExecutor();
	}

	@Override
	public void destroyScheduleExecutor(ScheduledExecutor scheduledExecutor) throws Exception {
		if (rpcEndpoint != null) {
			RpcUtils.terminateRpcEndpoint(rpcEndpoint, TIMEOUT);
			rpcEndpoint = null;
		}
	}

	@Test
	public void testExecutorRunAsync() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		rpcEndpoint
			.getMainThreadExecutor()
			.runAsync(latch::countDown);
		latch.await();
	}

	@Test
	public void testExecute() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		rpcEndpoint
			.getMainThreadExecutor()
			.execute(latch::countDown);
		latch.await();
	}

	@Test
	public void testScheduleRunAsync() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		final long start = System.nanoTime();
		rpcEndpoint
			.getMainThreadExecutor()
			.scheduleRunAsync(latch::countDown, 10L);
		latch.await();
		final long duration = System.nanoTime() - start;
		assertTrue(duration >= TimeUnit.MILLISECONDS.toNanos(10L));
	}

	@Test
	public void testAssertRunningInMainThread() throws Exception {
		final AtomicBoolean runningInMainThread = new AtomicBoolean(false);
		rpcEndpoint
			.getMainThreadExecutor()
			.schedule(() -> {
				rpcEndpoint
					.getMainThreadExecutor()
					.assertRunningInMainThread();
				runningInMainThread.set(true);
			}, 0, TimeUnit.MILLISECONDS)
			.get();
		assertTrue(runningInMainThread.get());
		try {
			rpcEndpoint.getMainThreadExecutor().assertRunningInMainThread();
			fail("assertRunningInMainThread should fail");
		} catch (Throwable ignored) {
		}
	}

	@Test
	public void testScheduleAtFixedRateWithCancellationAfterScheduled() throws Exception {
		testSchedulePeriodicallyWithCancellationAfterScheduled(
			(runnable, delay, period, timeUnit) ->
				rpcEndpoint
					.getMainThreadExecutor()
					.scheduleAtFixedRate(runnable, delay, period, timeUnit));
	}

	@Test
	public void testScheduleWithFixedDelayWithCancellationAfterScheduled() throws Exception {
		testSchedulePeriodicallyWithCancellationAfterScheduled(
			(runnable, delay, period, timeUnit) ->
				rpcEndpoint
					.getMainThreadExecutor()
					.scheduleWithFixedDelay(runnable, delay, period, timeUnit));
	}

	/**
	 * This case is designed to check cancellation behavior when the
	 * periodic scheduling is already enqueued.
	 *
	 * <p>
	 *     1. Executes an one-shot task first.
	 *     2. Schedules a task periodically.
	 *     3. Blocks in the one-shot task. So the periodic scheduled task could not be executed.
	 *     4. Cancels the periodic scheduling.
	 *     5. Verifies that the periodic task is cancelled successfully. It's never executed.
	 * </p>
	 *
	 * @param function the periodically schedule function
	 * @throws Exception exception
	 */
	private void testSchedulePeriodicallyWithCancellationAfterScheduled(
		PeriodicallyScheduleFunction function) throws Exception {

		final AtomicBoolean periodicSchedulingExecuted = new AtomicBoolean(false);
		final CountDownLatch oneShotRunnableExecuted = new CountDownLatch(1);
		final CountDownLatch oneShotLatch = new CountDownLatch(1);
		rpcEndpoint
			.getMainThreadExecutor()
			.execute(() -> {
			oneShotRunnableExecuted.countDown();
			try {
				// blocks the main thread here
				oneShotLatch.await();
			} catch (InterruptedException ignored) {
			}
		});
		final ScheduledFuture future = function.periodicallySchedule(
			() -> periodicSchedulingExecuted.set(false), 0L, 1L, TimeUnit.MILLISECONDS);
		// the one-shot task is being executed
		oneShotRunnableExecuted.await();
		// wait for a while to give the periodic scheduling a chance to be enqueued
		Thread.sleep(10L);
		// now do the cancellation
		assertTrue(future.cancel(false));
		// resume the main thread
		oneShotLatch.countDown();
		// wait for a while to check if the periodic scheduling is executed
		Thread.sleep(10L);
		assertFalse(periodicSchedulingExecuted.get());
	}
}
