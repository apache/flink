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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.Preconditions;

import java.util.UUID;

/**
 * JobMaster implementation. The job master is responsible for the execution of a single
 * {@link org.apache.flink.runtime.jobgraph.JobGraph}.
 * <p>
 * It offers the following methods as part of its rpc interface to interact with the JobMaster
 * remotely:
 * <ul>
 *     <li>{@link #updateTaskExecutionState(TaskExecutionState)} updates the task execution state for
 * given task</li>
 * </ul>
 */
public class JobMaster extends RpcEndpoint<JobMasterGateway> {

	/** Gateway to connected resource manager, null iff not connected */
	private ResourceManagerGateway resourceManager = null;

	/** Logical representation of the job */
	private final JobGraph jobGraph;
	private final JobID jobID;

	/** Configuration of the job */
	private final Configuration configuration;

	/** Service to contend for and retrieve the leadership of JM and RM */
	private final HighAvailabilityServices highAvailabilityServices;

	/** Leader Management */
	private LeaderElectionService leaderElectionService = null;
	private UUID leaderSessionID;

	/**
	 * The JM's Constructor
	 *
	 * @param jobGraph The representation of the job's execution plan
	 * @param configuration The job's configuration
	 * @param rpcService The RPC service at which the JM serves
	 * @param highAvailabilityService The cluster's HA service from the JM can elect and retrieve leaders.
	 */
	public JobMaster(
		JobGraph jobGraph,
		Configuration configuration,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityService) {

		super(rpcService);

		this.jobGraph = Preconditions.checkNotNull(jobGraph);
		this.jobID = Preconditions.checkNotNull(jobGraph.getJobID());

		this.configuration = Preconditions.checkNotNull(configuration);

		this.highAvailabilityServices = Preconditions.checkNotNull(highAvailabilityService);
	}

	public ResourceManagerGateway getResourceManager() {
		return resourceManager;
	}

	//----------------------------------------------------------------------------------------------
	// Initialization methods
	//----------------------------------------------------------------------------------------------
	public void start() {
		super.start();

		// register at the election once the JM starts
		registerAtElectionService();
	}


	//----------------------------------------------------------------------------------------------
	// JobMaster Leadership methods
	//----------------------------------------------------------------------------------------------

	/**
	 * Retrieves the election service and contend for the leadership.
	 */
	private void registerAtElectionService() {
		try {
			leaderElectionService = highAvailabilityServices.getJobMasterLeaderElectionService(jobID);
			leaderElectionService.start(new JobMasterLeaderContender());
		} catch (Exception e) {
			throw new RuntimeException("Fail to register at the election of JobMaster", e);
		}
	}

	/**
	 * Start the execution when the leadership is granted.
	 *
	 * @param newLeaderSessionID The identifier of the new leadership session
	 */
	public void grantJobMasterLeadership(final UUID newLeaderSessionID) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				log.info("JobManager {} grants leadership with session id {}.", getAddress(), newLeaderSessionID);

				// The operation may be blocking, but since JM is idle before it grants the leadership, it's okay that
				// JM waits here for the operation's completeness.
				leaderSessionID = newLeaderSessionID;
				leaderElectionService.confirmLeaderSessionID(newLeaderSessionID);

				// TODO:: execute the job when the leadership is granted.
			}
		});
	}

	/**
	 * Stop the execution when the leadership is revoked.
	 */
	public void revokeJobMasterLeadership() {
		runAsync(new Runnable() {
			@Override
			public void run() {
				log.info("JobManager {} was revoked leadership.", getAddress());

				// TODO:: cancel the job's execution and notify all listeners
				cancelAndClearEverything(new Exception("JobManager is no longer the leader."));

				leaderSessionID = null;
			}
		});
	}

	/**
	 * Handles error occurring in the leader election service
	 *
	 * @param exception Exception thrown in the leader election service
	 */
	public void onJobMasterElectionError(final Exception exception) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				log.error("Received an error from the LeaderElectionService.", exception);

				// TODO:: cancel the job's execution and shutdown the JM
				cancelAndClearEverything(exception);

				leaderSessionID = null;
			}
		});

	}

	//----------------------------------------------------------------------------------------------
	// RPC methods
	//----------------------------------------------------------------------------------------------

	/**
	 * Updates the task execution state for a given task.
	 *
	 * @param taskExecutionState New task execution state for a given task
	 * @return Acknowledge the task execution state update
	 */
	@RpcMethod
	public Acknowledge updateTaskExecutionState(TaskExecutionState taskExecutionState) {
		System.out.println("TaskExecutionState: " + taskExecutionState);
		return Acknowledge.get();
	}

	/**
	 * Triggers the registration of the job master at the resource manager.
	 *
	 * @param address Address of the resource manager
	 */
	@RpcMethod
	public void registerAtResourceManager(final String address) {
		//TODO:: register at the RM
	}

	//----------------------------------------------------------------------------------------------
	// Helper methods
	//----------------------------------------------------------------------------------------------

	/**
	 * Cancel the current job and notify all listeners the job's cancellation.
	 *
	 * @param cause Cause for the cancelling.
	 */
	private void cancelAndClearEverything(Throwable cause) {
		// currently, nothing to do here
	}

	// ------------------------------------------------------------------------
	//  Utility classes
	// ------------------------------------------------------------------------
	private class JobMasterLeaderContender implements LeaderContender {

		@Override
		public void grantLeadership(UUID leaderSessionID) {
			JobMaster.this.grantJobMasterLeadership(leaderSessionID);
		}

		@Override
		public void revokeLeadership() {
			JobMaster.this.revokeJobMasterLeadership();
		}

		@Override
		public String getAddress() {
			return JobMaster.this.getAddress();
		}

		@Override
		public void handleError(Exception exception) {
			onJobMasterElectionError(exception);
		}
	}
}
