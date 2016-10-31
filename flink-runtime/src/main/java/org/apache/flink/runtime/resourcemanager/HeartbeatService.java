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
import org.apache.flink.runtime.concurrent.AcceptFunction;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManagerSenderImpl;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Service which is responsible for heartbeat communication with TaskExecutor.
 */
public class HeartbeatService {

	private static final Logger LOG = LoggerFactory.getLogger(HeartbeatService.class);

	private final HeartbeatManagerSenderImpl<Void, UUID> taskExecutorHeartbeatManager;

	/** Resource ID of the ResourceManager */
	private final ResourceID resourceManagerIdentify;

	private final ScheduledExecutorService scheduledExecutorService;

	private final Executor executor;

	private HeartbeatListener<Void, UUID> heartbeatListener;

	public HeartbeatService(ResourceManagerConfiguration resourceManagerConfiguration, Executor executor) {
		this.resourceManagerIdentify = ResourceID.generate();
		this.scheduledExecutorService = Executors.newScheduledThreadPool(16);
		this.executor = executor;
		this.taskExecutorHeartbeatManager = new HeartbeatManagerSenderImpl<>(resourceManagerConfiguration.getHeartbeatInterval().toMilliseconds(),
			resourceManagerConfiguration.getTimeout().toMilliseconds(), resourceManagerIdentify, executor, scheduledExecutorService, LOG);
	}

	/**
	 * Starts the service with the given heartbeat listener
	 *
	 * @param heartbeatListener heartbeat listener to listener for heartbeat actions with TaskExecutor
	 */
	public void start(HeartbeatListener<Void, UUID> heartbeatListener) {
		this.heartbeatListener = heartbeatListener;
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

		this.taskExecutorHeartbeatManager.monitorTarget(resourceID, new HeartbeatTarget<UUID>() {

				@Override
				public void sendHeartbeat(ResourceID resourceID, UUID payload) {
					throw new UnsupportedOperationException("the method is not supported now!");
				}

				@Override
				public void requestHeartbeat(ResourceID resourceID, UUID payload) {
					Future<UUID> futurePayload = heartbeatListener.retrievePayload();

					futurePayload.thenAcceptAsync(new AcceptFunction<UUID>() {
						@Override
						public void accept(UUID retrievedPayload) {
							taskExecutorGateway.requestHeartbeatResponseToResourceManager(resourceManagerIdentify, retrievedPayload);
						}
					}, executor);
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

}
