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

import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.rpc.taskexecutor.SlotReport;
import org.apache.flink.runtime.rpc.taskexecutor.TaskExecutorGateway;
import org.slf4j.Logger;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * heartbeat between ResourceManager and TaskManager, it is responsible for schedule heartbeat and handle
 * heartbeat lost cases
 */
public class ResourceManagerToTaskExecutorHeartbeatScheduler {

	/** default heartbeat interval time in millisecond */
	private static final long INITIAL_HEARTBEAT_INTERVAL_MILLIS = 5000;

	/** default heartbeat timeout in millisecond */
	private static final long INITIAL_HEARTBEAT_TIMEOUT_MILLIS = 100;

	/** max heartbeat interval time in millisecond (which is used in retry heartbeat case) */
	private static final long MAX_HEARTBEAT_TIMEOUT_MILLIS = 30000;

	/** if a failure except for timeout exception happened when trigger heartbeat from resourceManager to taskManager , next attemp will start after  ERROR_HEARTBEAT_DELAY_MILLIS millisecond */
	private static final long ERROR_HEARTBEAT_DELAY_MILLIS = 2000;

	/** max heartbeat retry times when lost heartbeat */
	private static final int MAX_ATTEMPT_TIMES = 10;

	private final long heartbeatInterval;

	private final long heartbeatTimeout;

	private final long maxHeartbeatTimeout;

	private final long delayOnError;

	private final int maxAttempt;


	/** taskManagerGateway to receive the heartbeat and report slot allocation */
	private final TaskExecutorGateway taskExecutorGateway;

	/** the taskManager address */
	private final String taskExecutorAddress;

	/** identify the taskManager resourceID */
	private final ResourceID resourceID;

	/** identify the resourceManager rpc endpoint */
	private final ResourceManager resourceManager;

	private final UUID resourceManagerLeaderSessionID;

	private final Logger log;

	private volatile boolean closed;

	/**
	 * ResourceManagerToTaskExecutorHeartbeatScheduler constructor
	 *
	 * @param resourceManager         resourceManager which handles heartbeat communication with taskManager
	 * @param taskExecutorGateway     taskManager which receives heartbeat from resourceManager and report its slot
	 *                                allocation to resourceManager
	 * @param taskExecutorAddress     taskManager's address
	 * @param taskExecutorResourceID  taskManager's resourceID
	 * @param log
	 */
	public ResourceManagerToTaskExecutorHeartbeatScheduler(
		ResourceManager resourceManager, UUID resourceManagerLeaderSessionID, TaskExecutorGateway taskExecutorGateway,
		String taskExecutorAddress, ResourceID taskExecutorResourceID, Logger log) {
		this(resourceManager, resourceManagerLeaderSessionID, taskExecutorGateway, taskExecutorAddress, taskExecutorResourceID,
			log, INITIAL_HEARTBEAT_INTERVAL_MILLIS, INITIAL_HEARTBEAT_TIMEOUT_MILLIS, MAX_HEARTBEAT_TIMEOUT_MILLIS,
			ERROR_HEARTBEAT_DELAY_MILLIS, MAX_ATTEMPT_TIMES);
	}

	/**
	 * ResourceManagerToTaskExecutorHeartbeatScheduler constructor
	 *
	 * @param resourceManager
	 * @param taskExecutorGateway
	 * @param taskExecutorAddress
	 * @param taskExecutorResourceID
	 * @param log
	 * @param heartbeatInterval
	 * @param heartbeatTimeout
	 * @param maxHeartbeatTimeout
	 * @param delayOnError
	 * @param maxAttempt
	 */
	public ResourceManagerToTaskExecutorHeartbeatScheduler(
		ResourceManager resourceManager, UUID resourceManagerLeaderSessionID, TaskExecutorGateway taskExecutorGateway,
		String taskExecutorAddress, ResourceID taskExecutorResourceID, Logger log, long heartbeatInterval,
		long heartbeatTimeout, long maxHeartbeatTimeout, long delayOnError, int maxAttempt) {
		this.resourceManager = resourceManager;
		this.resourceManagerLeaderSessionID = resourceManagerLeaderSessionID;
		this.taskExecutorGateway = taskExecutorGateway;
		this.taskExecutorAddress = taskExecutorAddress;
		this.resourceID = taskExecutorResourceID;
		this.log = log;
		this.heartbeatInterval = heartbeatInterval;
		this.heartbeatTimeout = heartbeatTimeout;
		this.maxHeartbeatTimeout = maxHeartbeatTimeout;
		this.delayOnError = delayOnError;
		this.maxAttempt = maxAttempt;
	}

	/**
	 * start to schedule next heartbeat
	 */
	public void start() {
		checkState(!closed, "The heartbeat connection is already closed");
		sendHeartbeatToTaskManagerLater(1, heartbeatTimeout, heartbeatInterval);
	}

	/**
	 * Checks if the heartbeat schedule was closed.
	 *
	 * @return True if the heartbeat schedule was closed, false otherwise.
	 */
	public boolean isClosed() {
		return closed;
	}

	/**
	 * stop to schedule heartbeat
	 */
	public void close() {
		closed = true;
	}

	/**
	 * get the heartbeat interval
	 *
	 * @return heartbeat interval
	 */
	public long getHeartbeatInterval() {
		return heartbeatInterval;
	}

	/**
	 * send a heartbeat attempt to taskManager, receive the slotReport from TaskManager or failed depends on the future result.
	 *
	 * @param attempt
	 * @param timeoutMillis
	 */
	private void sendHeartbeatToTaskManager(final int attempt, final long timeoutMillis) {
		// eager check for closed to avoid some unnecessary work
		if (closed) {
			return;
		}
		FiniteDuration timeout = new FiniteDuration(timeoutMillis, TimeUnit.MILLISECONDS);
		Future<SlotReport> heartbeatResponse = taskExecutorGateway.triggerHeartbeatToResourceManager(resourceManagerLeaderSessionID, timeout);

		heartbeatResponse.onSuccess(new OnSuccess<SlotReport>() {

			@Override
			public void onSuccess(SlotReport result) throws Throwable {
				if (!isClosed()) {
					// notify the slotManager and trigger next time heartbeat
					resourceManager.handleSlotReportFromTaskManager(result);
					sendHeartbeatToTaskManagerLater(1, heartbeatTimeout, heartbeatInterval);
				}
			}
		}, resourceManager.getRpcService().getExecutionContext());

		// upon failure, retry
		heartbeatResponse.onFailure(new OnFailure() {
			@Override
			public void onFailure(Throwable failure) {
				if (!isClosed()) {
					if (attempt == maxAttempt) {
						log.error("ResourceManager {} fail to keep heartbeat with taskManager {} after {} attempts",
							resourceManager.getAddress(), taskExecutorAddress, attempt);
						// mark TaskManager as failed after heartbeat interaction attempts failed for many times
						resourceManager.getSelf().notifyResourceFailure(resourceID);
					} else {
						// upon timeout exception, exponential increase timeout
						if (failure instanceof TimeoutException) {
							if (log.isDebugEnabled()) {
								log.debug("ResourceManager {} lost heartbeat in {} ms with taskManager {} at {} attempts ",
									resourceManager.getAddress(), timeoutMillis, attempt);
							}

							long retryTimeoutMillis = Math.min(2 * timeoutMillis, maxHeartbeatTimeout);
							sendHeartbeatToTaskManager(attempt + 1, retryTimeoutMillis);
						} else {
							log.error(
								"ResourceManager {} fail to keep heartbeat with taskManager due to an error",
								failure);
						}
						sendHeartbeatToTaskManagerLater(attempt + 1, timeoutMillis, delayOnError);
					}
				}
			}
		}, resourceManager.getRpcService().getExecutionContext());
	}

	/**
	 * retry to sendHeartbeatToTaskManager after a few milliseconds
	 *
	 * @param attempt
	 * @param timeoutMillis
	 * @param delayMills
	 */
	private void sendHeartbeatToTaskManagerLater(final int attempt, final long timeoutMillis, final long delayMills) {
		resourceManager.getRpcService().scheduleRunnable(new Runnable() {
			@Override
			public void run() {
				sendHeartbeatToTaskManager(attempt, timeoutMillis);
			}
		}, delayMills, TimeUnit.MILLISECONDS);
	}
}
