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
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;

import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.jobmaster.JobMaster;
import org.apache.flink.runtime.rpc.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.rpc.taskexecutor.SlotAllocationResponse;
import org.apache.flink.runtime.rpc.taskexecutor.SlotReport;
import org.apache.flink.runtime.rpc.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.rpc.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.util.Preconditions;

import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;
import scala.concurrent.Future;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ResourceManager implementation. The resource manager is responsible for resource de-/allocation
 * and bookkeeping.
 *
 * It offers the following methods as part of its rpc interface to interact with the him remotely:
 * <ul>
 *     <li>{@link #registerJobMaster(JobMasterRegistration)} registers a {@link JobMaster} at the resource manager</li>
 *     <li>{@link #requestSlot(SlotRequest)} requests a slot from the resource manager</li>
 * </ul>
 */
public class ResourceManager extends RpcEndpoint<ResourceManagerGateway> {
	private final ExecutionContext executionContext;
	private final Map<JobMasterGateway, InstanceID> jobMasterGateways;
	private final Map<ResourceID, TaskExecutorGateway> taskExecutorGateways;
	private final Map<ResourceID, ResourceManagerToTaskExecutorHeartbeatScheduler> heartbeatSchedulers;
	private final LeaderElectionService leaderElectionService;
	private UUID leaderSessionID;
	// TODO private final SlotManager slotManager;

	public ResourceManager(RpcService rpcService, ExecutorService executorService, LeaderElectionService leaderElectionService) {
		super(rpcService);
		this.executionContext = ExecutionContext$.MODULE$.fromExecutor(
			Preconditions.checkNotNull(executorService));
		this.jobMasterGateways = new HashMap<>();
		this.taskExecutorGateways = new HashMap<>();
		this.heartbeatSchedulers = new HashMap<>();
		this.leaderElectionService = leaderElectionService;
		leaderSessionID = null;
		// TODO this.slotManager = null;
	}

	@Override
	public void start() {
		// start a leader
		try {
			leaderElectionService.start(new ResourceManagerLeaderContender());
			super.start();
		} catch (Exception e) {
			log.error("a fatal error happened when start resourceManager", e);
			shutDown();
		}
	}

	public UUID getLeaderSessionID() {
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
		// TODO slotManager.requestSlot(slotRequest);
		return new AcknowledgeSlotRequest(slotRequest.getAllocationID());
	}


	/**
	 * @param resourceManagerLeaderId The fencing token for the ResourceManager leader
	 * @param taskExecutorAddress     The address of the TaskExecutor that registers
	 * @param resourceID              The resource ID of the TaskExecutor that registers
	 *
	 * @return The response by the ResourceManager.
	 */
	@RpcMethod
	public Future<org.apache.flink.runtime.rpc.registration.RegistrationResponse> registerTaskExecutor(
		final UUID resourceManagerLeaderId,
		final String taskExecutorAddress,
		final ResourceID resourceID
	) {
		log.info("received register from taskExecutor {}, address {}", resourceID, taskExecutorAddress);
		Future<TaskExecutorGateway> taskExecutorFuture =
			getRpcService().connect(taskExecutorAddress, TaskExecutorGateway.class);

		return taskExecutorFuture.map(new Mapper<TaskExecutorGateway, org.apache.flink.runtime.rpc.registration.RegistrationResponse>() {
			@Override
			public org.apache.flink.runtime.rpc.registration.RegistrationResponse apply(final TaskExecutorGateway taskExecutorGateway) {
				// decline registration if resourceManager cannot connect to the taskExecutor using the given address
				if(taskExecutorGateway == null) {
					log.warn("resourceManager {} decline registration from the taskExecutor {}, cannot connect to it using given address {} ",
						getAddress(), resourceID, taskExecutorAddress);
					return new org.apache.flink.runtime.rpc.registration.RegistrationResponse.Decline("cannot connect to taskExecutor using given address");
				} else {
					// save the register taskExecutor gateway
					taskExecutorGateways.put(resourceID, taskExecutorGateway);
					// schedule the heartbeat with the registered taskExecutor
					ResourceManagerToTaskExecutorHeartbeatScheduler heartbeatScheduler = new ResourceManagerToTaskExecutorHeartbeatScheduler(
						ResourceManager.this, leaderSessionID, taskExecutorGateway, taskExecutorAddress, resourceID, log);
					heartbeatScheduler.start();
					heartbeatSchedulers.put(resourceID, heartbeatScheduler);
					return new TaskExecutorRegistrationSuccess(new InstanceID(), heartbeatScheduler.getHeartbeatInterval());
				}
			}
		}, getMainThreadExecutionContext());

	}

	/**
	 * notify resource failure to resourceManager, because of two reasons:
	 * 1. cannot keep heartbeat with taskManager for several times, mark the resource as failed
	 * 2. in some corner cases, TM will be marked as invalid by cluster manager master(e.g. yarn master), but TM itself does not realize.
	 *
	 * @param resourceID identify the taskManager which to stop
	 */
	@RpcMethod
	public void notifyResourceFailure(ResourceID resourceID) {
		log.warn("receive failure notification of resource {}", resourceID);
		TaskExecutorGateway taskManager = taskExecutorGateways.get(resourceID);
		if(taskManager == null) {
			// ignore command to stop an unregister taskManager
			log.warn("ignore stop taskManager command because {} is unregistered", resourceID);
		} else {
			taskExecutorGateways.remove(resourceID);
			closeHeartbeatToResourceIfExist(resourceID);
			// TODO notify slotManager and notify jobMaster,
			// TODO slotManager.notifyTaskManagerFailure(resourceID);
			taskManager.shutDown(leaderSessionID);
		}
	}

	/**
	 * close heartbeat triggers to resource if exist
	 * @param resourceID which resource need to stop keep heartbeat with
	 */
	private void closeHeartbeatToResourceIfExist(ResourceID resourceID) {
		if(heartbeatSchedulers.containsKey(resourceID)) {
			ResourceManagerToTaskExecutorHeartbeatScheduler heartbeatManager = heartbeatSchedulers.get(resourceID);
			heartbeatManager.close();
			heartbeatSchedulers.remove(resourceID);
		}
	}

	/**
	 * send slotRequest to the taskManager which the given slot is on
	 *
	 * @param slotRequest slot request information
	 * @param slotID      which slot is choosen
	 */
	void requestSlotToTaskManager(final SlotRequest slotRequest, final SlotID slotID) {
		ResourceID resourceID = slotID.getResourceID();
		TaskExecutorGateway taskManager = taskExecutorGateways.get(resourceID);
		if (taskManager == null) {
			// the given slot is on an unregister taskManager
			log.warn("ignore slot {} because it is on an unregister taskManager", slotID);
			// TODO slotManager.handleSlotRequestFailedAtTaskManager(slotRequest, slotID);
		} else {
			Future<SlotAllocationResponse> response = taskManager.requestSlotForJob(
				slotRequest.getAllocationID(), slotRequest.getJobID(), slotID, leaderSessionID);
			response.onSuccess(new OnSuccess<SlotAllocationResponse>() {
				@Override
				public void onSuccess(SlotAllocationResponse result) throws Throwable {
					if (result instanceof SlotAllocationResponse.Decline) {
						// TODO slotManager.handleSlotRequestFailedAtTaskManager(slotRequest, slotID);
					}
				}
			}, getMainThreadExecutionContext());
			response.onFailure(new OnFailure() {
				@Override
				public void onFailure(Throwable failure) {
					log.error("fail to request slot on taskManager because of error", failure);
					// TODO slotManager.handleSlotRequestFailedAtTaskManager(slotRequest, slotID);
				}
			}, getMainThreadExecutionContext());
		}
	}

	/**
	 * notify slotReport which is sent by taskManager to resourceManager
	 *
	 * @param slotReport the slot allocation report from taskManager
	 */
	void handleSlotReportFromTaskManager(final SlotReport slotReport) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				// TODO slotManager.updateSlot(slotReport);
			}
		});

	}


	/**
	 * callback method when current resourceManager is granted leadership
	 * @param newLeaderSessionID unique leadershipID
	 */
	void handleGrantLeadership(final UUID newLeaderSessionID) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				log.info("ResourceManager {} was granted leadership with leader session ID {}", getAddress(), newLeaderSessionID);
				leaderSessionID = newLeaderSessionID;
				// confirming the leader session ID might be blocking, thus do it concurrently
				getRpcService().scheduleRunnable(
					new Runnable() {
						@Override
						public void run() {
							leaderElectionService.confirmLeaderSessionID(newLeaderSessionID);
						}
					}, 0, TimeUnit.MILLISECONDS
				);
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
				for(JobMasterGateway jobMasterGateway : jobMasterGateways.keySet()) {
					jobMasterGateway.notifyOfResourceManagerRevokeLeadership(leaderSessionID);
				}
				jobMasterGateways.clear();
				for(TaskExecutorGateway taskExecutorGateway : taskExecutorGateways.values()) {
					taskExecutorGateway.notifyOfResourceManagerRevokeLeadership(leaderSessionID);
				}
				taskExecutorGateways.clear();
				// close all heartbeatSchedulers and clean
				for(ResourceManagerToTaskExecutorHeartbeatScheduler heartbeatScheduler : heartbeatSchedulers.values()) {
					heartbeatScheduler.close();
				}
				heartbeatSchedulers.clear();
				// notify slotManager
				// TODO slotManager.notifyRevokeLeadership()
				leaderSessionID = null;
			}
		});
	}

	private void disconnectRegisterJobMaster() {

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
