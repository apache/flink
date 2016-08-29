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
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.jobmaster.JobMaster;

import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;

/**
 * The {@link ResourceManager}'s RPC gateway interface.
 */
public interface ResourceManagerGateway extends RpcGateway {

	/**
	 * Register a {@link JobMaster} at the resource manager.
	 *
	 * @param jobMasterRegistration Job master registration information
	 * @param timeout Timeout for the future to complete
	 * @return Future registration response
	 */
	Future<RegistrationResponse> registerJobMaster(
		JobMasterRegistration jobMasterRegistration,
		@RpcTimeout FiniteDuration timeout);

	/**
	 * Register a {@link JobMaster} at the resource manager.
	 *
	 * @param jobMasterRegistration Job master registration information
	 * @return Future registration response
	 */
	Future<RegistrationResponse> registerJobMaster(JobMasterRegistration jobMasterRegistration);

	/**
	 * Requests a slot from the resource manager.
	 *
	 * @param slotRequest Slot request
	 * @return Future slot assignment
	 */
	Future<SlotAssignment> requestSlot(SlotRequest slotRequest);

	/**
	 * 
	 * @param resourceManagerLeaderId  The fencing token for the ResourceManager leader 
	 * @param taskExecutorAddress      The address of the TaskExecutor that registers
	 * @param resourceID               The resource ID of the TaskExecutor that registers
	 * @param timeout                  The timeout for the response.
	 * 
	 * @return The future to the response by the ResourceManager.
	 */
	Future<org.apache.flink.runtime.registration.RegistrationResponse> registerTaskExecutor(
			UUID resourceManagerLeaderId,
			String taskExecutorAddress,
			ResourceID resourceID,
			@RpcTimeout FiniteDuration timeout);
}
