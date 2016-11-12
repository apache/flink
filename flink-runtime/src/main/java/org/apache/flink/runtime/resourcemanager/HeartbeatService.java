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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManagerSenderImpl;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Service which is responsible for heartbeat communication with TaskExecutor.
 */
public class HeartbeatService {

	private static final Logger LOG = LoggerFactory.getLogger(HeartbeatService.class);

	private final HeartbeatManagerSenderImpl<Void, Void> taskExecutorHeartbeatManager;

	/** Resource ID of the ResourceManager */
	private final ResourceID resourceManagerIdentify;

	private final ScheduledExecutorService scheduledExecutorService;

	public HeartbeatService(ResourceManagerConfiguration resourceManagerConfiguration, Executor executor) {
		this.resourceManagerIdentify = ResourceID.generate();
		this.scheduledExecutorService = Executors.newScheduledThreadPool(16);
		this.taskExecutorHeartbeatManager = new HeartbeatManagerSenderImpl<>(resourceManagerConfiguration.getHeartbeatInterval().toMilliseconds(),
			resourceManagerConfiguration.getTimeout().toMilliseconds(), resourceManagerIdentify, executor, scheduledExecutorService, LOG);
	}

	/**
	 * Starts the service with the given heartbeat listener
	 *
	 * @param heartbeatListener heartbeat listener to listener for heartbeat actions with TaskExecutor
	 */
	public void start(HeartbeatListener<Void, Void> heartbeatListener) {
		this.taskExecutorHeartbeatManager.start(heartbeatListener);
	}

	/**
	 * Stops the service
	 *
	 * @throws Exception
	 */
	public void stop() throws Exception {
		try {
			scheduledExecutorService.shutdown();
			while (!scheduledExecutorService.awaitTermination(5000L, TimeUnit.MILLISECONDS)) {
				// break util scheduleExecutorService terminates
			}
		} finally {
			this.taskExecutorHeartbeatManager.stop();
		}
	}

	/**
	 * Adds a TaskExecutor to monitor
	 *
	 * @param resourceID          ResourceId of the taskExecutor to monitor
	 * @param taskExecutorGateway the taskExecutor to monitor
	 */
	public void monitorTaskExecutor(ResourceID resourceID, final TaskExecutorGateway taskExecutorGateway) {

		this.taskExecutorHeartbeatManager.monitorTarget(resourceID, new HeartbeatTarget<Void>() {

				@Override
				public void sendHeartbeat(ResourceID resourceID, Void payload) {
					throw new UnsupportedOperationException("the sendHeartbeat is not supported here!");
				}

				@Override
				public void requestHeartbeat(ResourceID resourceID, Void payload) {
					taskExecutorGateway.requestHeartbeatFromResourceManager(resourceManagerIdentify);
				}
			}

		);
	}

	/**
	 * Removes a taskExecutor to monitor
	 *
	 * @param resourceID ResourceId of the taskExecutor to unmonitor
	 */
	public void unmonitor(ResourceID resourceID) {
		this.taskExecutorHeartbeatManager.unmonitorTarget(resourceID);
	}

	/**
	 * Receives heartbeat response from taskExecutor
	 *
	 * @param resourceID ResourceId of the taskExecutor which send the heartbeat response
	 */
	public void heartbeatResponseFromTaskExecutor(ResourceID resourceID) {
		this.taskExecutorHeartbeatManager.sendHeartbeat(resourceID, null);
	}

	/**
	 * Get the resource manager identify
	 * @return
	 */
	public ResourceID getResourceManagerIdentify() {
		return resourceManagerIdentify;
	}
}
