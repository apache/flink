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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ApplyFunction;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class ResourceManager extends RpcEndpoint<ResourceManagerGateway> implements LeaderContender {

	private final Logger LOG = LoggerFactory.getLogger(getClass());

	private final Map<JobID, JobMasterGateway> jobMasterGateways;

	private final HighAvailabilityServices highAvailabilityServices;

	private LeaderElectionService leaderElectionService;

	private final SlotManager slotManager;

	private UUID leaderSessionID;

	public ResourceManager(
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			SlotManager slotManager) {
		super(rpcService);
		this.highAvailabilityServices = checkNotNull(highAvailabilityServices);
		this.jobMasterGateways = new HashMap<>();
		this.slotManager = slotManager;
	}

	@Override
	public void start() {
		// start a leader
		try {
			super.start();
			leaderElectionService = highAvailabilityServices.getResourceManagerLeaderElectionService();
			leaderElectionService.start(this);
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
		return this.leaderSessionID;
	}

	/**
	 * Register a {@link JobMaster} at the resource manager.
	 *
	 * @param jobMasterRegistration Job master registration information
	 * @return Future registration response
	 */
	@RpcMethod
	public Future<RegistrationResponse> registerJobMaster(JobMasterRegistration jobMasterRegistration) {
		final Future<JobMasterGateway> jobMasterFuture =
			getRpcService().connect(jobMasterRegistration.getAddress(), JobMasterGateway.class);
		final JobID jobID = jobMasterRegistration.getJobID();

		return jobMasterFuture.thenApplyAsync(new ApplyFunction<JobMasterGateway, RegistrationResponse>() {
			@Override
			public RegistrationResponse apply(JobMasterGateway jobMasterGateway) {
				final JobMasterGateway existingGateway = jobMasterGateways.put(jobID, jobMasterGateway);
				if (existingGateway != null) {
					LOG.info("Replacing existing gateway {} for JobID {} with  {}.",
						existingGateway, jobID, jobMasterGateway);
				}
				return new RegistrationResponse(true);
			}
		}, getMainThreadExecutor());
	}

	/**
	 * Requests a slot from the resource manager.
	 *
	 * @param slotRequest Slot request
	 * @return Slot assignment
	 */
	@RpcMethod
	public SlotRequestReply requestSlot(SlotRequest slotRequest) {
		final JobID jobId = slotRequest.getJobId();
		final JobMasterGateway jobMasterGateway = jobMasterGateways.get(jobId);

		if (jobMasterGateway != null) {
			return slotManager.requestSlot(slotRequest);
		} else {
			LOG.info("Ignoring slot request for unknown JobMaster with JobID {}", jobId);
			return new SlotRequestRejected(slotRequest.getAllocationId());
		}
	}


	/**
	 *
	 * @param resourceManagerLeaderId  The fencing token for the ResourceManager leader 
	 * @param taskExecutorAddress      The address of the TaskExecutor that registers
	 * @param resourceID               The resource ID of the TaskExecutor that registers
	 *
	 * @return The response by the ResourceManager.
	 */
	@RpcMethod
	public org.apache.flink.runtime.registration.RegistrationResponse registerTaskExecutor(
			UUID resourceManagerLeaderId,
			String taskExecutorAddress,
			ResourceID resourceID) {

		return new TaskExecutorRegistrationSuccess(new InstanceID(), 5000);
	}


	// ------------------------------------------------------------------------
	//  Leader Contender
	// ------------------------------------------------------------------------

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
				// confirming the leader session ID might be blocking,
				leaderElectionService.confirmLeaderSessionID(leaderSessionID);
				// notify SlotManager
				slotManager.notifyLeaderAddress(getAddress(), leaderSessionID);
				ResourceManager.this.leaderSessionID = leaderSessionID;
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
				ResourceManager.this.leaderSessionID = null;
			}
		});
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
				// notify SlotManager
				slotManager.handleError(exception);
				// terminate ResourceManager in case of an error
				shutDown();
			}
		});
	}
}
