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

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matcher;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link HeartbeatManager}.
 */
public class HeartbeatManagerTest extends TestLogger {
	private static final Logger LOG = LoggerFactory.getLogger(HeartbeatManagerTest.class);

	/**
	 * Tests that regular heartbeat signal triggers the right callback functions in the
	 * {@link HeartbeatListener}.
	 */
	@Test
	public void testRegularHeartbeat() throws InterruptedException {
		final long heartbeatTimeout = 1000L;
		ResourceID ownResourceID = new ResourceID("foobar");
		ResourceID targetResourceID = new ResourceID("barfoo");
		final int outputPayload = 42;
		final ArrayBlockingQueue<String> reportedPayloads = new ArrayBlockingQueue<>(2);
		final TestingHeartbeatListener<String, Integer> heartbeatListener = new TestingHeartbeatListenerBuilder<String, Integer>()
			.setReportPayloadConsumer((ignored, payload) -> reportedPayloads.offer(payload))
			.setRetrievePayloadFunction((ignored) -> CompletableFuture.completedFuture(outputPayload))
			.createNewTestingHeartbeatListener();

		HeartbeatManagerImpl<String, Integer> heartbeatManager = new HeartbeatManagerImpl<>(
			heartbeatTimeout,
			ownResourceID,
			heartbeatListener,
			TestingUtils.defaultScheduledExecutor(),
			LOG);

		final ArrayBlockingQueue<Integer> reportedPayloadsHeartbeatTarget = new ArrayBlockingQueue<>(2);
		final TestingHeartbeatTarget<Integer> heartbeatTarget = new TestingHeartbeatTargetBuilder<Integer>()
			.setReceiveHeartbeatConsumer((ignoredA, payload) -> reportedPayloadsHeartbeatTarget.offer(payload))
			.createTestingHeartbeatTarget();

		heartbeatManager.monitorTarget(targetResourceID, heartbeatTarget);

		final String inputPayload1 = "foobar";
		heartbeatManager.requestHeartbeat(targetResourceID, inputPayload1);

		assertThat(reportedPayloads.take(), is(inputPayload1));
		assertThat(reportedPayloadsHeartbeatTarget.take(), is(outputPayload));

		final String inputPayload2 = "barfoo";
		heartbeatManager.receiveHeartbeat(targetResourceID, inputPayload2);
		assertThat(reportedPayloads.take(), is(inputPayload2));
	}

	/**
	 * Tests that the heartbeat monitors are updated when receiving a new heartbeat signal.
	 */
	@Test
	public void testHeartbeatMonitorUpdate() {
		long heartbeatTimeout = 1000L;
		ResourceID ownResourceID = new ResourceID("foobar");
		ResourceID targetResourceID = new ResourceID("barfoo");
		@SuppressWarnings("unchecked")
		HeartbeatListener<Object, Object> heartbeatListener = mock(HeartbeatListener.class);
		ScheduledExecutor scheduledExecutor = mock(ScheduledExecutor.class);
		ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);

		doReturn(scheduledFuture).when(scheduledExecutor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

		Object expectedObject = new Object();

		when(heartbeatListener.retrievePayload(any(ResourceID.class))).thenReturn(CompletableFuture.completedFuture(expectedObject));

		HeartbeatManagerImpl<Object, Object> heartbeatManager = new HeartbeatManagerImpl<>(
			heartbeatTimeout,
			ownResourceID,
			heartbeatListener,
			scheduledExecutor,
			LOG);

		@SuppressWarnings("unchecked")
		HeartbeatTarget<Object> heartbeatTarget = mock(HeartbeatTarget.class);

		heartbeatManager.monitorTarget(targetResourceID, heartbeatTarget);

		heartbeatManager.receiveHeartbeat(targetResourceID, expectedObject);

		verify(scheduledFuture, times(1)).cancel(true);
		verify(scheduledExecutor, times(2)).schedule(any(Runnable.class), eq(heartbeatTimeout), eq(TimeUnit.MILLISECONDS));
	}

	/**
	 * Tests that a heartbeat timeout is signaled if the heartbeat is not reported in time.
	 */
	@Test
	public void testHeartbeatTimeout() throws Exception {
		long heartbeatTimeout = 100L;
		int numHeartbeats = 6;
		long heartbeatInterval = 20L;
		final int payload = 42;

		ResourceID ownResourceID = new ResourceID("foobar");
		ResourceID targetResourceID = new ResourceID("barfoo");

		final CompletableFuture<ResourceID> timeoutFuture = new CompletableFuture<>();
		final TestingHeartbeatListener<Integer, Integer> heartbeatListener = new TestingHeartbeatListenerBuilder<Integer, Integer>()
			.setRetrievePayloadFunction(ignored -> CompletableFuture.completedFuture(payload))
			.setNotifyHeartbeatTimeoutConsumer(timeoutFuture::complete)
			.createNewTestingHeartbeatListener();

		HeartbeatManagerImpl<Integer, Integer> heartbeatManager = new HeartbeatManagerImpl<>(
			heartbeatTimeout,
			ownResourceID,
			heartbeatListener,
			TestingUtils.defaultScheduledExecutor(),
			LOG);

		final HeartbeatTarget<Integer> heartbeatTarget = new TestingHeartbeatTargetBuilder<Integer>()
			.createTestingHeartbeatTarget();

		heartbeatManager.monitorTarget(targetResourceID, heartbeatTarget);

		for (int i = 0; i < numHeartbeats; i++) {
			heartbeatManager.receiveHeartbeat(targetResourceID, payload);
			Thread.sleep(heartbeatInterval);
		}

		assertFalse(timeoutFuture.isDone());

		ResourceID timeoutResourceID = timeoutFuture.get(2 * heartbeatTimeout, TimeUnit.MILLISECONDS);

		assertEquals(targetResourceID, timeoutResourceID);
	}

	/**
	 * Tests the heartbeat interplay between the {@link HeartbeatManagerImpl} and the
	 * {@link HeartbeatManagerSenderImpl}. The sender should regularly trigger heartbeat requests
	 * which are fulfilled by the receiver. Upon stopping the receiver, the sender should notify
	 * the heartbeat listener about the heartbeat timeout.
	 *
	 * @throws Exception
	 */
	@Test
	public void testHeartbeatCluster() throws Exception {
		long heartbeatTimeout = 100L;
		long heartbeatPeriod = 20L;
		ResourceID resourceIdTarget = new ResourceID("foobar");
		ResourceID resourceIDSender = new ResourceID("barfoo");
		final int targetPayload = 42;
		final AtomicInteger numReportPayloadCallsTarget = new AtomicInteger(0);
		final TestingHeartbeatListener<String, Integer> heartbeatListenerTarget = new TestingHeartbeatListenerBuilder<String, Integer>()
			.setRetrievePayloadFunction(ignored -> CompletableFuture.completedFuture(targetPayload))
			.setReportPayloadConsumer((ignoredA, ignoredB) -> numReportPayloadCallsTarget.incrementAndGet())
			.createNewTestingHeartbeatListener();

		final String senderPayload = "1337";
		final CompletableFuture<ResourceID> targetHeartbeatTimeoutFuture = new CompletableFuture<>();
		final AtomicInteger numReportPayloadCallsSender = new AtomicInteger(0);
		final TestingHeartbeatListener<Integer, String> heartbeatListenerSender = new TestingHeartbeatListenerBuilder<Integer, String>()
			.setRetrievePayloadFunction(ignored -> CompletableFuture.completedFuture(senderPayload))
			.setNotifyHeartbeatTimeoutConsumer(targetHeartbeatTimeoutFuture::complete)
			.setReportPayloadConsumer((ignoredA, ignoredB) -> numReportPayloadCallsSender.incrementAndGet())
			.createNewTestingHeartbeatListener();

		HeartbeatManagerImpl<String, Integer> heartbeatManagerTarget = new HeartbeatManagerImpl<>(
			heartbeatTimeout,
			resourceIdTarget,
			heartbeatListenerTarget,
			TestingUtils.defaultScheduledExecutor(),
			LOG);

		HeartbeatManagerSenderImpl<Integer, String> heartbeatManagerSender = new HeartbeatManagerSenderImpl<>(
			heartbeatPeriod,
			heartbeatTimeout,
			resourceIDSender,
			heartbeatListenerSender,
			TestingUtils.defaultScheduledExecutor(),
			LOG);

		heartbeatManagerTarget.monitorTarget(resourceIDSender, heartbeatManagerSender);
		heartbeatManagerSender.monitorTarget(resourceIdTarget, heartbeatManagerTarget);

		Thread.sleep(2 * heartbeatTimeout);

		assertFalse(targetHeartbeatTimeoutFuture.isDone());

		heartbeatManagerTarget.stop();

		ResourceID timeoutResourceID = targetHeartbeatTimeoutFuture.get(2 * heartbeatTimeout, TimeUnit.MILLISECONDS);

		assertThat(timeoutResourceID, is(resourceIdTarget));

		int numberHeartbeats = (int) (2 * heartbeatTimeout / heartbeatPeriod);

		final Matcher<Integer> numberHeartbeatsMatcher = greaterThanOrEqualTo(numberHeartbeats / 2);
		assertThat(numReportPayloadCallsTarget.get(), is(numberHeartbeatsMatcher));
		assertThat(numReportPayloadCallsSender.get(), is(numberHeartbeatsMatcher));
	}

	/**
	 * Tests that after unmonitoring a target, there won't be a timeout triggered.
	 */
	@Test
	public void testTargetUnmonitoring() throws Exception {
		// this might be too aggressive for Travis, let's see...
		long heartbeatTimeout = 50L;
		ResourceID resourceID = new ResourceID("foobar");
		ResourceID targetID = new ResourceID("target");
		final int payload = 42;

		final CompletableFuture<ResourceID> timeoutFuture = new CompletableFuture<>();
		final TestingHeartbeatListener<Integer, Integer> heartbeatListener = new TestingHeartbeatListenerBuilder<Integer, Integer>()
			.setRetrievePayloadFunction(ignored -> CompletableFuture.completedFuture(payload))
			.setNotifyHeartbeatTimeoutConsumer(timeoutFuture::complete)
			.createNewTestingHeartbeatListener();

		HeartbeatManager<Integer, Integer> heartbeatManager = new HeartbeatManagerImpl<>(
			heartbeatTimeout,
			resourceID,
			heartbeatListener,
			TestingUtils.defaultScheduledExecutor(),
			LOG);

		final HeartbeatTarget<Integer> heartbeatTarget = new TestingHeartbeatTargetBuilder<Integer>()
			.createTestingHeartbeatTarget();
		heartbeatManager.monitorTarget(targetID, heartbeatTarget);

		heartbeatManager.unmonitorTarget(targetID);

		try {
			timeoutFuture.get(2 * heartbeatTimeout, TimeUnit.MILLISECONDS);
			fail("Timeout should time out.");
		} catch (TimeoutException ignored) {
			// the timeout should not be completed since we unmonitored the target
		}
	}

	/**
	 * Tests that the last heartbeat from an unregistered target equals -1.
	 */
	@Test
	public void testLastHeartbeatFromUnregisteredTarget() {
		final long heartbeatTimeout = 100L;
		final ResourceID resourceId = ResourceID.generate();
		@SuppressWarnings("unchecked")
		final HeartbeatListener<Object, Object> heartbeatListener = mock(HeartbeatListener.class);

		HeartbeatManager<?, ?> heartbeatManager = new HeartbeatManagerImpl<>(
			heartbeatTimeout,
			resourceId,
			heartbeatListener,
			mock(ScheduledExecutor.class),
			LOG);

		try {
			assertEquals(-1L, heartbeatManager.getLastHeartbeatFrom(ResourceID.generate()));
		} finally {
			heartbeatManager.stop();
		}
	}

	/**
	 * Tests that we can correctly retrieve the last heartbeat for registered targets.
	 */
	@Test
	public void testLastHeartbeatFrom() {
		final long heartbeatTimeout = 100L;
		final ResourceID resourceId = ResourceID.generate();
		@SuppressWarnings("unchecked")
		final HeartbeatListener<Object, Object> heartbeatListener = mock(HeartbeatListener.class);
		@SuppressWarnings("unchecked")
		final HeartbeatTarget<Object> heartbeatTarget = mock(HeartbeatTarget.class);
		final ResourceID target = ResourceID.generate();

		HeartbeatManager<Object, Object> heartbeatManager = new HeartbeatManagerImpl<>(
			heartbeatTimeout,
			resourceId,
			heartbeatListener,
			mock(ScheduledExecutor.class),
			LOG);

		try {
			heartbeatManager.monitorTarget(target, heartbeatTarget);

			assertEquals(0L, heartbeatManager.getLastHeartbeatFrom(target));

			final long currentTime = System.currentTimeMillis();

			heartbeatManager.receiveHeartbeat(target, null);

			assertTrue(heartbeatManager.getLastHeartbeatFrom(target) >= currentTime);
		} finally {
			heartbeatManager.stop();
		}
	}

	/**
	 * Tests that the heartbeat target {@link ResourceID} is properly passed to the {@link HeartbeatListener} by the
	 * {@link HeartbeatManagerImpl}.
	 */
	@Test
	public void testHeartbeatManagerTargetPayload() throws Exception {
		final long heartbeatTimeout = 100L;

		final ResourceID someTargetId = ResourceID.generate();
		final ResourceID specialTargetId = ResourceID.generate();

		final Map<ResourceID, Integer> payloads = new HashMap<>(2);
		payloads.put(someTargetId, 0);
		payloads.put(specialTargetId, 1);

		final CompletableFuture<Integer> someHeartbeatPayloadFuture = new CompletableFuture<>();
		final TestingHeartbeatTarget<Integer> someHeartbeatTarget = new TestingHeartbeatTargetBuilder<Integer>()
			.setReceiveHeartbeatConsumer((ignored, payload) -> someHeartbeatPayloadFuture.complete(payload))
			.createTestingHeartbeatTarget();

		final CompletableFuture<Integer> specialHeartbeatPayloadFuture = new CompletableFuture<>();
		final TestingHeartbeatTarget<Integer> specialHeartbeatTarget = new TestingHeartbeatTargetBuilder<Integer>()
			.setReceiveHeartbeatConsumer((ignored, payload) -> specialHeartbeatPayloadFuture.complete(payload))
			.createTestingHeartbeatTarget();

		final TestingHeartbeatListener<Void, Integer> testingHeartbeatListener = new TestingHeartbeatListenerBuilder<Void, Integer>()
			.setRetrievePayloadFunction(resourceId -> CompletableFuture.completedFuture(payloads.get(resourceId)))
			.createNewTestingHeartbeatListener();

		HeartbeatManager<?, Integer> heartbeatManager = new HeartbeatManagerImpl<>(
			heartbeatTimeout,
			ResourceID.generate(),
			testingHeartbeatListener,
			TestingUtils.defaultScheduledExecutor(),
			LOG);

		try {
			heartbeatManager.monitorTarget(someTargetId, someHeartbeatTarget);
			heartbeatManager.monitorTarget(specialTargetId, specialHeartbeatTarget);

			heartbeatManager.requestHeartbeat(someTargetId, null);
			assertThat(someHeartbeatPayloadFuture.get(), is(payloads.get(someTargetId)));

			heartbeatManager.requestHeartbeat(specialTargetId, null);
			assertThat(specialHeartbeatPayloadFuture.get(), is(payloads.get(specialTargetId)));
		} finally {
			heartbeatManager.stop();
		}
	}

	/**
	 * Tests that the heartbeat target {@link ResourceID} is properly passed to the {@link HeartbeatListener} by the
	 * {@link HeartbeatManagerSenderImpl}.
	 */
	@Test
	public void testHeartbeatManagerSenderTargetPayload() throws Exception {
		final long heartbeatTimeout = 100L;
		final long heartbeatPeriod = 2000L;

		final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);

		final ResourceID someTargetId = ResourceID.generate();
		final ResourceID specialTargetId = ResourceID.generate();

		final OneShotLatch someTargetReceivedLatch = new OneShotLatch();
		final OneShotLatch specialTargetReceivedLatch = new OneShotLatch();

		final TargetDependentHeartbeatReceiver someHeartbeatTarget = new TargetDependentHeartbeatReceiver(someTargetReceivedLatch);
		final TargetDependentHeartbeatReceiver specialHeartbeatTarget = new TargetDependentHeartbeatReceiver(specialTargetReceivedLatch);

		final int defaultResponse = 0;
		final int specialResponse = 1;

		HeartbeatManager<?, Integer> heartbeatManager = new HeartbeatManagerSenderImpl<>(
			heartbeatPeriod,
			heartbeatTimeout,
			ResourceID.generate(),
			new TargetDependentHeartbeatSender(specialTargetId, specialResponse, defaultResponse),
			new ScheduledExecutorServiceAdapter(scheduledThreadPoolExecutor),
			LOG);

		try {
			heartbeatManager.monitorTarget(someTargetId, someHeartbeatTarget);
			heartbeatManager.monitorTarget(specialTargetId, specialHeartbeatTarget);

			someTargetReceivedLatch.await(5, TimeUnit.SECONDS);
			specialTargetReceivedLatch.await(5, TimeUnit.SECONDS);

			assertEquals(defaultResponse, someHeartbeatTarget.getLastRequestedHeartbeatPayload());
			assertEquals(specialResponse, specialHeartbeatTarget.getLastRequestedHeartbeatPayload());
		} finally {
			heartbeatManager.stop();
			scheduledThreadPoolExecutor.shutdown();
		}
	}

	/**
	 * Test {@link HeartbeatTarget} that exposes the last received payload.
	 */
	private static class TargetDependentHeartbeatReceiver implements HeartbeatTarget<Integer> {

		private volatile int lastReceivedHeartbeatPayload = -1;
		private volatile int lastRequestedHeartbeatPayload = -1;

		private final OneShotLatch latch;

		public TargetDependentHeartbeatReceiver() {
			this(new OneShotLatch());
		}

		public TargetDependentHeartbeatReceiver(OneShotLatch latch) {
			this.latch = latch;
		}

		@Override
		public void receiveHeartbeat(ResourceID heartbeatOrigin, Integer heartbeatPayload) {
			this.lastReceivedHeartbeatPayload = heartbeatPayload;
			latch.trigger();
		}

		@Override
		public void requestHeartbeat(ResourceID requestOrigin, Integer heartbeatPayload) {
			this.lastRequestedHeartbeatPayload = heartbeatPayload;
			latch.trigger();
		}

		public int getLastReceivedHeartbeatPayload() {
			return lastReceivedHeartbeatPayload;
		}

		public int getLastRequestedHeartbeatPayload() {
			return lastRequestedHeartbeatPayload;
		}
	}

	/**
	 * Test {@link HeartbeatListener} that returns different payloads based on the target {@link ResourceID}.
	 */
	private static class TargetDependentHeartbeatSender implements HeartbeatListener<Object, Integer>  {
		private final ResourceID specialId;
		private final int specialResponse;
		private final int defaultResponse;

		TargetDependentHeartbeatSender(ResourceID specialId, int specialResponse, int defaultResponse) {
			this.specialId = specialId;
			this.specialResponse = specialResponse;
			this.defaultResponse = defaultResponse;
		}

		@Override
		public void notifyHeartbeatTimeout(ResourceID resourceID) {
		}

		@Override
		public void reportPayload(ResourceID resourceID, Object payload) {
		}

		@Override
		public CompletableFuture<Integer> retrievePayload(ResourceID resourceID) {
			if (resourceID.equals(specialId)) {
				return CompletableFuture.completedFuture(specialResponse);
			} else {
				return CompletableFuture.completedFuture(defaultResponse);
			}
		}
	}
}
