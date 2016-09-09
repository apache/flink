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

import akka.dispatch.Futures;
import akka.dispatch.Mapper;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.rpc.exceptions.LeaderSessionIDException;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.registration.RegistrationResponse;
import scala.concurrent.Future;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

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
	private final Map<JobMasterGateway, InstanceID> jobMasterGateways;

	/** ResourceID and TaskExecutorRegistration mapping relationship of registered taskExecutors */
	private final Map<ResourceID, TaskExecutorRegistration>  startedTaskExecutorGateways;

	private final HighAvailabilityServices highAvailabilityServices;
	private LeaderElectionService leaderElectionService = null;
	private UUID leaderSessionID = null;

	public ResourceManager(RpcService rpcService, HighAvailabilityServices highAvailabilityServices) {
		super(rpcService);
		this.highAvailabilityServices = checkNotNull(highAvailabilityServices);
		this.jobMasterGateways = new HashMap<>(16);
		this.startedTaskExecutorGateways = new HashMap<>(16);
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

	@Override
	public void shutDown() {
		try {
			leaderElectionService.stop();
			super.shutDown();
		} catch(Throwable e) {
			log.error("A fatal error happened when shutdown the ResourceManager", e);
			throw new RuntimeException("A fatal error happened when shutdown the ResourceManager", e);
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

				return new TaskExecutorRegistrationSuccess(instanceID, 5000);
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
	public SlotAssignment requestSlot(SlotRequest slotRequest) {
		System.out.println("SlotRequest: " + slotRequest);
		return new SlotAssignment();
	}


	/**
	 * Register a {@link org.apache.flink.runtime.taskexecutor.TaskExecutor} at the resource manager
	 *
	 * @param resourceManagerLeaderId  The fencing token for the ResourceManager leader
	 * @param taskExecutorAddress      The address of the TaskExecutor that registers
	 * @param resourceID               The resource ID of the TaskExecutor that registers
	 *
	 * @return The response by the ResourceManager.
	 */
	@RpcMethod
	public Future<RegistrationResponse> registerTaskExecutor(
		final UUID resourceManagerLeaderId,
		final String taskExecutorAddress,
		final ResourceID resourceID) {

		if(!leaderSessionID.equals(resourceManagerLeaderId)) {
			log.warn("Discard registration from TaskExecutor {} at ({}) because the expected leader session ID {} did not equal the received leader session ID  {}",
				resourceID, taskExecutorAddress, leaderSessionID, resourceManagerLeaderId);
			return Futures.failed(new LeaderSessionIDException(leaderSessionID, resourceManagerLeaderId));
		}

		Future<TaskExecutorGateway> taskExecutorGatewayFuture = getRpcService().connect(taskExecutorAddress, TaskExecutorGateway.class);

		return taskExecutorGatewayFuture.map(new Mapper<TaskExecutorGateway, RegistrationResponse>() {

			@Override
			public RegistrationResponse apply(final TaskExecutorGateway taskExecutorGateway) {
				InstanceID instanceID = null;
				TaskExecutorRegistration taskExecutorRegistration = startedTaskExecutorGateways.get(resourceID);
				if(taskExecutorRegistration != null) {
					log.warn("Receive a duplicate registration from TaskExecutor {} at ({})", resourceID, taskExecutorAddress);
					instanceID = taskExecutorRegistration.getInstanceID();
				} else {
					instanceID = new InstanceID();
					startedTaskExecutorGateways.put(resourceID, new TaskExecutorRegistration(taskExecutorGateway, instanceID));
				}

				return new TaskExecutorRegistrationSuccess(instanceID, 5000);
			}
		}, getMainThreadExecutionContext());
	}


	private class ResourceManagerLeaderContender implements LeaderContender {

		/**
		 * Callback method when current resourceManager is granted leadership
		 *
		 * @param leaderSessionID unique leadershipID
		 */
		@Override
		public void grantLeadership(final UUID leaderSessionID) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					log.info("ResourceManager {} was granted leadership with leader session ID {}", getAddress(), leaderSessionID);
					ResourceManager.this.leaderSessionID = leaderSessionID;
					// confirming the leader session ID might be blocking,
					leaderElectionService.confirmLeaderSessionID(leaderSessionID);
				}
			});
		}

		/**
		 * Callback method when current resourceManager lose leadership.
		 */
		@Override
		public void revokeLeadership() {
			runAsync(new Runnable() {
				@Override
				public void run() {
					log.info("ResourceManager {} was revoked leadership.", getAddress());
					jobMasterGateways.clear();
					startedTaskExecutorGateways.clear();
					leaderSessionID = null;
				}
			});
		}

		@Override
		public String getAddress() {
			return ResourceManager.this.getAddress();
		}

		/**
		 * Handles error occurring in the leader election service
		 *
		 * @param exception Exception being thrown in the leader election service
		 */
		@Override
		public void handleError(final Exception exception) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					log.error("ResourceManager received an error from the LeaderElectionService.", exception);
					// terminate ResourceManager in case of an error
					shutDown();
				}
			});
		}
	}
}
