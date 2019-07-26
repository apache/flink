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
import org.apache.flink.runtime.concurrent.ScheduledExecutor;

import org.slf4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link HeartbeatServices} implementation for testing purposes.
 */
public class TestingHeartbeatServices extends HeartbeatServices {

	private final long heartbeatInterval;

	private final long heartbeatTimeout;

	private final ConcurrentHashMap<ResourceID, TestHeartbeatManager> heartbeatManagers = new ConcurrentHashMap<>();

	private final ConcurrentHashMap<ResourceID, TestHeartbeatManager> heartbeatManagerSenders = new ConcurrentHashMap<>();

	public TestingHeartbeatServices() {
		this(Long.MAX_VALUE, Long.MAX_VALUE);
	}

	public TestingHeartbeatServices(long heartbeatInterval, long heartbeatTimeout) {
		super(heartbeatInterval, heartbeatTimeout);

		this.heartbeatInterval = heartbeatInterval;
		this.heartbeatTimeout = heartbeatTimeout;
	}

	@Override
	public <I, O> HeartbeatManager<I, O> createHeartbeatManager(
		ResourceID resourceId,
		HeartbeatListener<I, O> heartbeatListener,
		ScheduledExecutor mainThreadExecutor,
		Logger log) {

		TestHeartbeatManager<I, O> heartbeatManager = new TestHeartbeatManager<>(
			heartbeatInterval,
			resourceId,
			heartbeatListener,
			mainThreadExecutor,
			log);

		heartbeatManagers.put(resourceId, heartbeatManager);
		return heartbeatManager;
	}

	@Override
	public <I, O> HeartbeatManager<I, O> createHeartbeatManagerSender(
		ResourceID resourceId,
		HeartbeatListener<I, O> heartbeatListener,
		ScheduledExecutor mainThreadExecutor,
		Logger log) {

		TestHeartbeatManager<I, O> heartbeatManager = new TestHeartbeatManager<>(
			heartbeatInterval,
			resourceId,
			heartbeatListener,
			mainThreadExecutor,
			log);

		heartbeatManagerSenders.put(resourceId, heartbeatManager);
		return heartbeatManager;
	}

	public void triggerHeartbeatTimeout(ResourceID managerResourceId, ResourceID targetResourceId) {
		heartbeatManagers.get(managerResourceId).triggerHeartbeatTimeout(targetResourceId);
	}

	public void triggerHeartbeatSenderTimeout(ResourceID managerResourceId, ResourceID targetResourceId) {
		heartbeatManagerSenders.get(managerResourceId).triggerHeartbeatTimeout(targetResourceId);
	}

	private static class TestHeartbeatManager<I, O> implements HeartbeatManager<I, O> {

		private final long heartbeatTimeoutIntervalMs;
		private final ResourceID ownResourceID;
		private final HeartbeatListener<I, O> heartbeatListener;
		private final ScheduledExecutor mainThreadExecutor;
		private final ConcurrentHashMap<ResourceID, HeartbeatTarget<O>> monitoringTargets = new ConcurrentHashMap<>();
		private volatile boolean stopped;

		public TestHeartbeatManager(
			long heartbeatTimeoutIntervalMs,
			ResourceID ownResourceID,
			HeartbeatListener<I, O> heartbeatListener,
			ScheduledExecutor mainThreadExecutor,
			Logger log) {

			this.heartbeatTimeoutIntervalMs = heartbeatTimeoutIntervalMs;
			this.ownResourceID = ownResourceID;
			this.heartbeatListener = heartbeatListener;
			this.mainThreadExecutor = mainThreadExecutor;
		}

		@Override
		public void monitorTarget(ResourceID resourceID, HeartbeatTarget<O> heartbeatTarget) {
			if (!stopped) {
				monitoringTargets.put(resourceID, heartbeatTarget);
			}
		}

		@Override
		public void unmonitorTarget(ResourceID resourceID) {
			if (!stopped) {
				monitoringTargets.remove(resourceID);
			}
		}

		@Override
		public void stop() {
			stopped = true;
			monitoringTargets.clear();
		}

		@Override
		public long getLastHeartbeatFrom(ResourceID resourceId) {
			return 0;
		}

		@Override
		public void receiveHeartbeat(ResourceID heartbeatOrigin, I heartbeatPayload) {

		}

		@Override
		public void requestHeartbeat(ResourceID requestOrigin, I heartbeatPayload) {

		}

		public void triggerHeartbeatTimeout(ResourceID targetResourceId) {
			mainThreadExecutor.execute(() -> heartbeatListener.notifyHeartbeatTimeout(targetResourceId));
		}
	}
}
