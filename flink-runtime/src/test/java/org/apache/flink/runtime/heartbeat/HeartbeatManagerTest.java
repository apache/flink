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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.CompletableFuture;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.util.DirectExecutorService;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class HeartbeatManagerTest extends TestLogger {
	private static final Logger LOG = LoggerFactory.getLogger(HeartbeatManagerTest.class);

	/**
	 * Tests that regular heartbeat signal triggers the right callback functions in the
	 * {@link HeartbeatListener}.
	 */
	@Test
	public void testRegularHeartbeat() {
		long heartbeatTimeout = 1000L;
		ResourceID ownResourceID = new ResourceID("foobar");
		ResourceID targetResourceID = new ResourceID("barfoo");
		HeartbeatListener<Object, Object> heartbeatListener = mock(HeartbeatListener.class);
		ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);

		Object expectedObject = new Object();

		when(heartbeatListener.retrievePayload()).thenReturn(FlinkCompletableFuture.completed(expectedObject));

		HeartbeatManagerImpl<Object, Object> heartbeatManager = new HeartbeatManagerImpl<>(
			heartbeatTimeout,
			ownResourceID,
			new DirectExecutorService(),
			scheduledExecutorService,
			LOG);

		heartbeatManager.start(heartbeatListener);

		HeartbeatTarget<Object> heartbeatTarget = mock(HeartbeatTarget.class);

		heartbeatManager.monitorTarget(targetResourceID, heartbeatTarget);

		heartbeatManager.requestHeartbeat(targetResourceID, expectedObject);

		verify(heartbeatListener, times(1)).reportPayload(targetResourceID, expectedObject);
		verify(heartbeatListener, times(1)).retrievePayload();
		verify(heartbeatTarget, times(1)).sendHeartbeat(ownResourceID, expectedObject);

		heartbeatManager.sendHeartbeat(targetResourceID, expectedObject);

		verify(heartbeatListener, times(2)).reportPayload(targetResourceID, expectedObject);
	}

	/**
	 * Tests that the heartbeat monitors are updated when receiving a new heartbeat signal.
	 */
	@Test
	public void testHeartbeatMonitorUpdate() {
		long heartbeatTimeout = 1000L;
		ResourceID ownResourceID = new ResourceID("foobar");
		ResourceID targetResourceID = new ResourceID("barfoo");
		HeartbeatListener<Object, Object> heartbeatListener = mock(HeartbeatListener.class);
		ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
		ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);

		doReturn(scheduledFuture).when(scheduledExecutorService).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

		Object expectedObject = new Object();

		when(heartbeatListener.retrievePayload()).thenReturn(FlinkCompletableFuture.completed(expectedObject));

		HeartbeatManagerImpl<Object, Object> heartbeatManager = new HeartbeatManagerImpl<>(
			heartbeatTimeout,
			ownResourceID,
			new DirectExecutorService(),
			scheduledExecutorService,
			LOG);

		heartbeatManager.start(heartbeatListener);

		HeartbeatTarget<Object> heartbeatTarget = mock(HeartbeatTarget.class);

		heartbeatManager.monitorTarget(targetResourceID, heartbeatTarget);

		heartbeatManager.sendHeartbeat(targetResourceID, expectedObject);

		verify(scheduledFuture, times(1)).cancel(true);
		verify(scheduledExecutorService, times(2)).schedule(any(Runnable.class), eq(heartbeatTimeout), eq(TimeUnit.MILLISECONDS));
	}

	/**
	 * Tests that a heartbeat timeout is signaled if the heartbeat is not reported in time.
	 *
	 * @throws Exception
	 */
	@Test
	public void testHeartbeatTimeout() throws Exception {
		long heartbeatTimeout = 100L;
		int numHeartbeats = 10;
		long heartbeatInterval = 20L;
		Object payload = new Object();

		ResourceID ownResourceID = new ResourceID("foobar");
		ResourceID targetResourceID = new ResourceID("barfoo");
		TestingHeartbeatListener heartbeatListener = new TestingHeartbeatListener(payload);
		ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
		ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);

		doReturn(scheduledFuture).when(scheduledExecutorService).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

		Object expectedObject = new Object();

		HeartbeatManagerImpl<Object, Object> heartbeatManager = new HeartbeatManagerImpl<>(
			heartbeatTimeout,
			ownResourceID,
			new DirectExecutorService(),
			new ScheduledThreadPoolExecutor(1),
			LOG);

		heartbeatManager.start(heartbeatListener);

		HeartbeatTarget<Object> heartbeatTarget = mock(HeartbeatTarget.class);

		Future<ResourceID> timeoutFuture = heartbeatListener.getTimeoutFuture();

		heartbeatManager.monitorTarget(targetResourceID, heartbeatTarget);

		for (int i = 0; i < numHeartbeats; i++) {
			heartbeatManager.sendHeartbeat(targetResourceID, expectedObject);
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
		Object object = new Object();
		Object object2 = new Object();
		ResourceID resourceID = new ResourceID("foobar");
		ResourceID resourceID2 = new ResourceID("barfoo");
		HeartbeatListener<Object, Object> heartbeatListener = mock(HeartbeatListener.class);

		when(heartbeatListener.retrievePayload()).thenReturn(FlinkCompletableFuture.completed(object));

		TestingHeartbeatListener heartbeatListener2 = new TestingHeartbeatListener(object2);

		Future<ResourceID> futureTimeout = heartbeatListener2.getTimeoutFuture();

		HeartbeatManagerImpl<Object, Object> heartbeatManager = new HeartbeatManagerImpl<>(
			heartbeatTimeout,
			resourceID,
			new DirectExecutorService(),
			new ScheduledThreadPoolExecutor(1),
			LOG);

		HeartbeatManagerSenderImpl<Object, Object> heartbeatManager2 = new HeartbeatManagerSenderImpl<>(
			heartbeatPeriod,
			heartbeatTimeout,
			resourceID2,
			new DirectExecutorService(),
			new ScheduledThreadPoolExecutor(1),
			LOG);;

		heartbeatManager.start(heartbeatListener);
		heartbeatManager2.start(heartbeatListener2);

		heartbeatManager.monitorTarget(resourceID2, heartbeatManager2);
		heartbeatManager2.monitorTarget(resourceID, heartbeatManager);

		Thread.sleep(2 * heartbeatTimeout);

		assertFalse(futureTimeout.isDone());

		heartbeatManager.stop();

		ResourceID timeoutResourceID = futureTimeout.get(2 * heartbeatTimeout, TimeUnit.MILLISECONDS);

		assertEquals(resourceID, timeoutResourceID);

		int numberHeartbeats = (int) (2 * heartbeatTimeout / heartbeatPeriod);

		verify(heartbeatListener, atLeast(numberHeartbeats / 2)).reportPayload(resourceID2, object2);
		assertTrue(heartbeatListener2.getNumberHeartbeatReports() >= numberHeartbeats / 2);
	}

	/**
	 * Tests that after unmonitoring a target, there won't be a timeout triggered
	 */
	@Test
	public void testTargetUnmonitoring() throws InterruptedException, ExecutionException {
		// this might be too aggresive for Travis, let's see...
		long heartbeatTimeout = 100L;
		ResourceID resourceID = new ResourceID("foobar");
		ResourceID targetID = new ResourceID("target");
		Object object = new Object();

		HeartbeatManager<Object, Object> heartbeatManager = new HeartbeatManagerImpl<>(
			heartbeatTimeout,
			resourceID,
			new DirectExecutorService(),
			new ScheduledThreadPoolExecutor(1),
			LOG);

		TestingHeartbeatListener heartbeatListener = new TestingHeartbeatListener(object);

		heartbeatManager.start(heartbeatListener);

		heartbeatManager.monitorTarget(targetID, mock(HeartbeatTarget.class));

		heartbeatManager.unmonitorTarget(targetID);

		Future<ResourceID> timeout = heartbeatListener.getTimeoutFuture();


		try {
			timeout.get(2 * heartbeatTimeout, TimeUnit.MILLISECONDS);
			fail("Timeout should time out.");
		} catch (TimeoutException e) {
			// the timeout should not be completed since we unmonitored the target
		}
	}

	static class TestingHeartbeatListener implements HeartbeatListener<Object, Object> {

		private final CompletableFuture<ResourceID> future = new FlinkCompletableFuture<>();

		private final Object payload;

		private int numberHeartbeatReports;

		TestingHeartbeatListener(Object payload) {
			this.payload = payload;
		}

		public Future<ResourceID> getTimeoutFuture() {
			return future;
		}

		public int getNumberHeartbeatReports() {
			return numberHeartbeatReports;
		}

		@Override
		public void notifyHeartbeatTimeout(ResourceID resourceID) {
			future.complete(resourceID);
		}

		@Override
		public void reportPayload(ResourceID resourceID, Object payload) {
			numberHeartbeatReports++;
		}

		@Override
		public Future<Object> retrievePayload() {
			return FlinkCompletableFuture.completed(payload);
		}
	}
}
