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

package org.apache.flink.runtime.rpc.resourcemanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.rpc.heartbeat.HeartbeatScheduler;
import org.apache.flink.runtime.rpc.taskexecutor.SlotReport;
import org.apache.flink.runtime.rpc.taskexecutor.TaskExecutorGateway;
import org.slf4j.Logger;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * ResourceManagerToTaskExecutorHeartbeatManager is responsible for trigger heartbeat between resourceManager and all registered taskExecutors,
 * notify ResourceManager about failure of taskManager instance which lost heartbeat
 */
public class ResourceManagerToTaskExecutorHeartbeatManager {
	/** active heartbeat schedulers between resourceManager and TaskExecutors */
	private final Map<ResourceID, ResourceManagerToTaskExecutorHeartbeatScheduler> activeHeartbeatSchedulers;

	/** resourceManager which send heartbeat */
	private final ResourceManager resourceManager;

	/** leader session id of current resourceManager */
	private final UUID leaderID;
	private final Logger log;

	/**
	 * @param resourceManager                resourceManager which handles heartbeat communication with taskManager
	 * @param resourceManagerLeaderSessionID leader session id of current resourceManager
	 * @param log
	 */
	public ResourceManagerToTaskExecutorHeartbeatManager(ResourceManager resourceManager,
		UUID resourceManagerLeaderSessionID, Logger log) {
		this.resourceManager = resourceManager;
		this.leaderID = resourceManagerLeaderSessionID;
		this.log = log;
		this.activeHeartbeatSchedulers = new HashMap<>();
	}

	/**
	 * register heartbeat target
	 * @param resourceID target taskExecutor resourceID
	 * @param targetTarget target taskExecutor gateway
	 * @param targetAddress target taskExecutor address
	 * @return heartbeat interval in millisecond
	 */
	public long registerTarget(ResourceID resourceID, TaskExecutorGateway targetTarget, String targetAddress) {
		if (activeHeartbeatSchedulers.containsKey(resourceID)) {
			log.warn("Ignore taskExecutor registration with resource id {} from {} because it is already registered,", resourceID, targetAddress);
			return -1;
		} else {
			ResourceManagerToTaskExecutorHeartbeatScheduler heartbeatScheduler = new ResourceManagerToTaskExecutorHeartbeatScheduler(
				resourceManager, leaderID, targetTarget, targetAddress, resourceID, log);
			heartbeatScheduler.start();
			activeHeartbeatSchedulers.put(resourceID, heartbeatScheduler);
			return heartbeatScheduler.getHeartbeatInterval();
		}
	}

	/**
	 * stop and clean all active heartbeat scheduler
	 */
	public void stopHeartbeatToAllTaskExecutor() {
		for (ResourceManagerToTaskExecutorHeartbeatScheduler heartbeatScheduler : activeHeartbeatSchedulers.values()) {
			heartbeatScheduler.close();
		}
		activeHeartbeatSchedulers.clear();
	}

	/**
	 * stop and clean heartbeat scheduler to the specified taskExecutor, usually happens when taskExecutor marked failed
	 * @param resourceID taskExecutor's resourceID which to stop heartbeat
	 */
	public void stopHeartbeatToTaskExecutor(ResourceID resourceID) {
		if (activeHeartbeatSchedulers.containsKey(resourceID)) {
			ResourceManagerToTaskExecutorHeartbeatScheduler heartbeatManager = activeHeartbeatSchedulers.get(resourceID);
			heartbeatManager.close();
			activeHeartbeatSchedulers.remove(resourceID);
		}
	}

	/**
	 * Heartbeat scheduler from ResourceManager to TaskExecutor
	 */
	class ResourceManagerToTaskExecutorHeartbeatScheduler extends HeartbeatScheduler<TaskExecutorGateway, SlotReport> {

		/** identify the taskManager resourceID */
		private final ResourceID resourceID;

		/** identify the resourceManager rpc endpoint */
		private final ResourceManager resourceManager;


		/**
		 * @param resourceManager                resourceManager which handles heartbeat communication with taskManager
		 * @param resourceManagerLeaderSessionID leader session id of current resourceManager
		 * @param taskExecutorGateway            taskManager which receives heartbeat from resourceManager and report its slot
		 *                                       allocation to resourceManager
		 * @param taskExecutorAddress            taskManager's address
		 * @param taskExecutorResourceID         taskManager's resourceID
		 * @param log                            log
		 */
		public ResourceManagerToTaskExecutorHeartbeatScheduler(
			ResourceManager resourceManager, UUID resourceManagerLeaderSessionID,
			TaskExecutorGateway taskExecutorGateway,
			String taskExecutorAddress, ResourceID taskExecutorResourceID, Logger log) {
			super(resourceManager.getRpcService(), resourceManagerLeaderSessionID, taskExecutorGateway, taskExecutorAddress,
				"taskExecutor " + taskExecutorResourceID.toString(), log);
			this.resourceManager = resourceManager;
			this.resourceID = checkNotNull(taskExecutorResourceID);
		}

		/**
		 * @param resourceManager                resourceManager which handles heartbeat communication with taskManager
		 * @param resourceManagerLeaderSessionID leader session id of current resourceManager
		 * @param taskExecutorGateway            taskManager which receives heartbeat from resourceManager and report its slot
		 *                                       allocation to resourceManager
		 * @param taskExecutorAddress            taskManager's address
		 * @param taskExecutorResourceID         taskManager's resourceID
		 * @param log                            log
		 * @param heartbeatInterval              heartbeat interval time in millisecond
		 * @param heartbeatTimeout               heartbeat timeout in millisecond
		 * @param maxHeartbeatTimeout            max heartbeat interval time in millisecond
		 * @param delayOnError                   Heartbeat attempt delay after an exception has occurred
		 * @param maxAttemptTime                 max retry time for one heartbeat
		 */
		public ResourceManagerToTaskExecutorHeartbeatScheduler(
			ResourceManager resourceManager, UUID resourceManagerLeaderSessionID,
			TaskExecutorGateway taskExecutorGateway,
			String taskExecutorAddress, ResourceID taskExecutorResourceID, Logger log, long heartbeatInterval,
			long heartbeatTimeout, long maxHeartbeatTimeout, long delayOnError, long maxAttemptTime) {
			super(resourceManager.getRpcService(), resourceManagerLeaderSessionID, taskExecutorGateway, taskExecutorAddress,
				"taskExecutor " + taskExecutorResourceID.toString(), log, heartbeatInterval, heartbeatTimeout, maxHeartbeatTimeout, delayOnError, maxAttemptTime);
			this.resourceManager = resourceManager;
			this.resourceID = checkNotNull(taskExecutorResourceID);
		}

		@Override
		protected Future<SlotReport> triggerHeartbeat(UUID leaderID, FiniteDuration timeout) {
			return targetGateway.triggerHeartbeatToResourceManager(leaderID, timeout);
		}

		@Override
		protected void reportHeartbeatPayload(SlotReport slotReport) {
			resourceManager.handleSlotReportFromTaskManager(slotReport);
		}

		@Override
		protected void lossHeartbeat() {
			resourceManager.notifyLostHeartbeat(resourceID);
		}

	}
}
