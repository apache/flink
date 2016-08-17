
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

public class HeartbeatManagerToTaskManager {
	private static final long INITIAL_HEARTBEAT_INTERVAL_MILLIS = 1000;
	private static final long INITIAL_HEARTBEAT_TIMEOUT_MILLIS = 100;
	private static final long MAX_HEARTBEAT_TIMEOUT_MILLIS = 1000;
	private static final int MAX_ATTEMPT_TIMES = 10;

	private final TaskExecutorGateway targetGateway;
	private final String targetTaskExecutorAddress;
	private final ResourceManager sourceRpcEndpoint;
	private final UUID resourceManagerLeaderId;
	private final ResourceID resourceID;
	private final Logger log;



	public HeartbeatManagerToTaskManager(ResourceManager sourceRpcEndpoint, UUID resourceManagerLeaderId, TaskExecutorGateway targetGateway, String taskExecutorAddress, ResourceID taskExecutorResourceID, Logger log) {
		this.sourceRpcEndpoint = sourceRpcEndpoint;
		this.targetGateway = targetGateway;
		this.targetTaskExecutorAddress = taskExecutorAddress;
		this.resourceManagerLeaderId = resourceManagerLeaderId;
		this.resourceID = taskExecutorResourceID;
		this.log = log;
	}



	public void triggerNextHeartbeat() {
		sourceRpcEndpoint.scheduleRunAsync(new Runnable() {
			@Override
			public void run() {
				sendHeartbeatToTarget(1, INITIAL_HEARTBEAT_TIMEOUT_MILLIS);
			}
		}, INITIAL_HEARTBEAT_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
	}

	public void retryHeartbeat(final int attempt, final long timeoutMillis) {
		sourceRpcEndpoint.runAsync(new Runnable() {
			@Override
			public void run() {
				sendHeartbeatToTarget(attempt, timeoutMillis);
			}
		});
	}

	public void sendHeartbeatToTarget(final int attempt, final long timeoutMillis) {
		FiniteDuration timeout = new FiniteDuration(timeoutMillis, TimeUnit.MILLISECONDS);
		Future<SlotReport> heartbeatResponse = targetGateway.triggerHeartbeatToResourceManager(resourceManagerLeaderId, timeout);
		// if the registration was successful, let the TaskExecutor know
		heartbeatResponse.onSuccess(new OnSuccess<SlotReport>() {

			@Override
			public void onSuccess(SlotReport result) throws Throwable {
				// TODO notify the slotManager and trigger next time heartbeat
				triggerNextHeartbeat();
				sourceRpcEndpoint.syncWithTaskManagerSlotReport(result);
			}
		}, sourceRpcEndpoint.getMainThreadExecutionContext());

		// upon failure, retry
		heartbeatResponse.onFailure(new OnFailure() {
			@Override
			public void onFailure(Throwable failure) {
				if(attempt == MAX_ATTEMPT_TIMES) {
					log.error("ResourceManager {} fail to keep heartbeat with taskManager {} after {} attempts", sourceRpcEndpoint.getAddress(), targetTaskExecutorAddress, attempt);
					sourceRpcEndpoint.taskManagerFailed(resourceID);
					// notify the failure of TaskManager to SlotManager
				} else {
					if (failure instanceof TimeoutException) {
						if (log.isDebugEnabled()) {
							log.debug("ResourceManager {} lost heartbeat in {} ms with taskManager {} at {} attempts ",
									  sourceRpcEndpoint.getAddress(),timeoutMillis,
									  attempt
									  );
						}

						long newTimeoutMillis = Math.min(
							2 * timeoutMillis,
							MAX_HEARTBEAT_TIMEOUT_MILLIS);
						retryHeartbeat(attempt + 1, newTimeoutMillis);
					} else {
						log.error(
							"ResourceManager {} fail to keep heartbeat with taskManager due to an error",
							failure);

						retryHeartbeat(attempt + 1, timeoutMillis);
					}
				}
			}
		}, sourceRpcEndpoint.getMainThreadExecutionContext());
	}
}
