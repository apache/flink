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

import akka.dispatch.Mapper;

import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceID;

import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.jobmaster.JobMaster;
import org.apache.flink.runtime.rpc.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.rpc.taskexecutor.SlotReport;
import org.apache.flink.runtime.rpc.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.rpc.taskexecutor.TaskExecutorRegistrationSuccess;

import scala.concurrent.Future;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * ResourceManager implementation. The resource manager is responsible for resource de-/allocation
 * and bookkeeping.
 * <p>
 * It offers the following methods as part of its rpc interface to interact with the him remotely:
 * <ul>
 * <li>{@link #registerJobMaster(JobMasterRegistration)} registers a {@link JobMaster} at the resource manager</li>
 * <li>{@link #requestSlot(SlotRequest)} requests a slot from the resource manager</li>
 * </ul>
 */
public class ResourceManager extends RpcEndpoint<ResourceManagerGateway> {
	private final Map<JobMasterGateway, InstanceID> jobMasterGateways;
	private final Map<ResourceID, TaskExecutorGateway> taskExecutorGateways;
	private final HighAvailabilityServices highAvailabilityServices;
	private LeaderElectionService leaderElectionService = null;
	private UUID leaderSessionID = null;
	private ResourceManagerToTaskExecutorHeartbeatManager heartbeatManager = null;

	public ResourceManager(RpcService rpcService, HighAvailabilityServices highAvailabilityServices) {
		super(rpcService);
		this.highAvailabilityServices = checkNotNull(highAvailabilityServices);
		this.jobMasterGateways = new HashMap<>();
		this.taskExecutorGateways = new HashMap<>();
	}

	@Override
	public void start() {
		// start a leader
		try {
			super.start();
			leaderElectionService = highAvailabilityServices.getResourceManagerLeaderElectionService();
			leaderElectionService.start(new ResourceManagerLeaderContender());
		} catch (Throwable e) {
			log.error("A fatal error happened when starting the ResourceManager", e);
			throw new RuntimeException("A fatal error happened when starting the ResourceManager", e);
		}
	}

	/**
	 * Gets the leader session id of current resourceManager.
	 *
	 * @return return the leaderSessionId of current resourceManager, this returns null until the current resourceManager is granted leadership.
	 */
	@VisibleForTesting
	UUID getLeaderSessionID() {
		return leaderSessionID;
	}

	/**
	 * Register a {@link JobMaster} at the resource manager.
	 *
	 * @param jobMasterRegistration Job master registration information
	 * @return Future registration response
	 */
	@RpcMethod
	public Future<RegistrationResponse> registerJobMaster(JobMasterRegistration jobMasterRegistration) {
		Future<JobMasterGateway> jobMasterFuture = getRpcService().connect(jobMasterRegistration.getAddress(), JobMasterGateway.class);

		return jobMasterFuture.map(new Mapper<JobMasterGateway, RegistrationResponse>() {
			@Override
			public RegistrationResponse apply(final JobMasterGateway jobMasterGateway) {
				InstanceID instanceID;

				if (jobMasterGateways.containsKey(jobMasterGateway)) {
					instanceID = jobMasterGateways.get(jobMasterGateway);
				} else {
					instanceID = new InstanceID();
					jobMasterGateways.put(jobMasterGateway, instanceID);
				}

				return new RegistrationResponse(true, instanceID);
			}
		}, getMainThreadExecutionContext());
	}

	/**
	 * Requests a slot from the resource manager.
	 *
	 * @param slotRequest Slot request
	 * @return Slot assignment
	 */
	@RpcMethod
	public AcknowledgeSlotRequest requestSlot(SlotRequest slotRequest) {
		return new AcknowledgeSlotRequest(slotRequest.getAllocationID());
	}


	/**
	 * Register a {@link org.apache.flink.runtime.rpc.taskexecutor.TaskExecutor} at the resource manager.
	 *
	 * @param resourceManagerLeaderId The fencing token for the ResourceManager leader
	 * @param taskExecutorAddress     The address of the TaskExecutor that registers
	 * @param resourceID              The resource ID of the TaskExecutor that registers
	 * @return The response by the ResourceManager.
	 */
	@RpcMethod
	public Future<org.apache.flink.runtime.rpc.registration.RegistrationResponse> registerTaskExecutor(
		final UUID resourceManagerLeaderId,
		final String taskExecutorAddress,
		final ResourceID resourceID)
	{
		log.info("Received taskExecutor registration with resource id {} from {}", resourceID, taskExecutorAddress);
		Future<TaskExecutorGateway> taskExecutorFuture =
			getRpcService().connect(taskExecutorAddress, TaskExecutorGateway.class);

		return taskExecutorFuture.map(new Mapper<TaskExecutorGateway, org.apache.flink.runtime.rpc.registration.RegistrationResponse>() {
			@Override
			public org.apache.flink.runtime.rpc.registration.RegistrationResponse apply(
				final TaskExecutorGateway taskExecutorGateway)
			{
				// decline registration if resourceManager cannot connect to the taskExecutor using the given address
				if (taskExecutorGateway == null) {
					log.warn("ResourceManager {} decline taskExecutor registration with resource id {} from {} because cannot connect to it using given address",
						getAddress(), resourceID, taskExecutorAddress);
					return new org.apache.flink.runtime.rpc.registration.RegistrationResponse.Decline("cannot connect to taskExecutor using given address");
				} else {
					// register target taskExecutor to heartbeat manager
					taskExecutorGateways.put(resourceID, taskExecutorGateway);
					long heartbeatInterval = heartbeatManager.registerTarget(resourceID, taskExecutorGateway, taskExecutorAddress);
					return new TaskExecutorRegistrationSuccess(new InstanceID(), heartbeatInterval);
				}
			}
		}, getMainThreadExecutionContext());
	}

	/**
	 * Notify lost heartbeat with specified taskExecutor
	 *
	 * @param resourceID identify the taskManager which lost heartbeat with
	 */
	void notifyLostHeartbeat(final ResourceID resourceID) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				TaskExecutorGateway failedTaskManager = taskExecutorGateways.remove(resourceID);
				if (failedTaskManager != null) {
					heartbeatManager.stopHeartbeatToTaskExecutor(resourceID);
					failedTaskManager.markedFailed(leaderSessionID);
				}
			}
		});
	}


	/**
	 * Notify slotReport which is sent by taskManager to resourceManager
	 *
	 * @param slotReport the slot allocation report from taskManager
	 */
	void handleSlotReportFromTaskManager(final SlotReport slotReport) {

	}


	/**
	 * Callback method when current resourceManager is granted leadership
	 *
	 * @param newLeaderSessionID unique leadershipID
	 */
	void handleGrantLeadership(final UUID newLeaderSessionID) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				log.info("ResourceManager {} was granted leadership with leader session ID {}", getAddress(), newLeaderSessionID);
				leaderSessionID = newLeaderSessionID;
				heartbeatManager = new ResourceManagerToTaskExecutorHeartbeatManager(ResourceManager.this, newLeaderSessionID, log);
				// confirming the leader session ID might be blocking,
				leaderElectionService.confirmLeaderSessionID(newLeaderSessionID);
			}
		});
	}

	/**
	 * Callback method when current resourceManager lose leadership.
	 */
	void handleRevokeLeadership() {
		runAsync(new Runnable() {
			@Override
			public void run() {
				log.info("ResourceManager {} was revoked leadership.", getAddress());
				jobMasterGateways.clear();
				taskExecutorGateways.clear();
				if (heartbeatManager != null) {
					heartbeatManager.stopHeartbeatToAllTaskExecutor();
					heartbeatManager = null;
				}
				leaderSessionID = null;
			}
		});
	}

	private class ResourceManagerLeaderContender implements LeaderContender {

		@Override
		public void grantLeadership(UUID leaderSessionID) {
			handleGrantLeadership(leaderSessionID);
		}

		@Override
		public void revokeLeadership() {
			handleRevokeLeadership();
		}

		@Override
		public String getAddress() {
			return getAddress();
		}

		/**
		 * Handles error occurring in the leader election service
		 *
		 * @param exception Exception being thrown in the leader election service
		 */
		@Override
		public void handleError(Exception exception) {
			log.error("ResourceManager received an error from the LeaderElectionService.", exception);
			// terminate ResourceManager in case of an error
			shutDown();
		}
	}
}
