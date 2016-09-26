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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.concurrent.Future;

import org.slf4j.Logger;

import java.util.UUID;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The connection between a JobMaster and the ResourceManager.
 */
public class JobMasterToResourceManagerConnection 
		extends RegisteredRpcConnection<ResourceManagerGateway, JobMasterRegistrationSuccess> {

	/** the JobMaster whose connection to the ResourceManager this represents */
	private final JobMaster jobMaster;

	private final JobID jobID;

	private final UUID jobMasterLeaderId;

	public JobMasterToResourceManagerConnection(
			Logger log,
			JobID jobID,
			JobMaster jobMaster,
			UUID jobMasterLeaderId,
			String resourceManagerAddress,
			UUID resourceManagerLeaderId,
			Executor executor) {

		super(log, resourceManagerAddress, resourceManagerLeaderId, executor);
		this.jobMaster = checkNotNull(jobMaster);
		this.jobID = checkNotNull(jobID);
		this.jobMasterLeaderId = checkNotNull(jobMasterLeaderId);
	}

	@Override
	protected RetryingRegistration<ResourceManagerGateway, JobMasterRegistrationSuccess> generateRegistration() {
		return new JobMasterToResourceManagerConnection.ResourceManagerRegistration(
			log, jobMaster.getRpcService(),
			getTargetAddress(), getTargetLeaderId(),
			jobMaster.getAddress(),jobID, jobMasterLeaderId);
	}

	@Override
	protected void onRegistrationSuccess(JobMasterRegistrationSuccess success) {
	}

	@Override
	protected void onRegistrationFailure(Throwable failure) {
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static class ResourceManagerRegistration
		extends RetryingRegistration<ResourceManagerGateway, JobMasterRegistrationSuccess> {

		private final String jobMasterAddress;

		private final JobID jobID;

		private final UUID jobMasterLeaderId;

		ResourceManagerRegistration(
			Logger log,
			RpcService rpcService,
			String targetAddress,
			UUID leaderId,
			String jobMasterAddress,
			JobID jobID,
			UUID jobMasterLeaderId) {

			super(log, rpcService, "ResourceManager", ResourceManagerGateway.class, targetAddress, leaderId);
			this.jobMasterAddress = checkNotNull(jobMasterAddress);
			this.jobID = checkNotNull(jobID);
			this.jobMasterLeaderId = checkNotNull(jobMasterLeaderId);
		}

		@Override
		protected Future<RegistrationResponse> invokeRegistration(
			ResourceManagerGateway gateway, UUID leaderId, long timeoutMillis) throws Exception {

			Time timeout = Time.milliseconds(timeoutMillis);
			return gateway.registerJobMaster(leaderId, jobMasterLeaderId,jobMasterAddress, jobID, timeout);
		}
	}
}
