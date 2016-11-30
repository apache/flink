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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.messages.jobmanager.RMSlotRequestReply;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.taskexecutor.SlotReport;

import java.util.UUID;

/**
 * The {@link ResourceManager}'s RPC gateway interface.
 */
public interface ResourceManagerGateway extends RpcGateway {

	/**
	 * Register a {@link JobMaster} at the resource manager.
	 *
	 * @param resourceManagerLeaderId The fencing token for the ResourceManager leader
	 * @param jobMasterLeaderId The fencing token for the JobMaster leader
	 * @param jobMasterAddress        The address of the JobMaster that registers
	 * @param jobID                   The Job ID of the JobMaster that registers
	 * @param timeout                 Timeout for the future to complete
	 * @return Future registration response
	 */
	Future<RegistrationResponse> registerJobManager(
		UUID resourceManagerLeaderId,
		UUID jobMasterLeaderId,
		String jobMasterAddress,
		JobID jobID,
		@RpcTimeout Time timeout);


	/**
	 * Requests a slot from the resource manager.
	 *
	 * @param resourceManagerLeaderID leader if of the ResourceMaster
	 * @param jobMasterLeaderID leader if of the JobMaster
	 * @param slotRequest The slot to request
	 * @return The confirmation that the slot gets allocated
	 */
	Future<RMSlotRequestReply> requestSlot(
		UUID resourceManagerLeaderID,
		UUID jobMasterLeaderID,
		SlotRequest slotRequest,
		@RpcTimeout Time timeout);

	/**
	 * Register a {@link org.apache.flink.runtime.taskexecutor.TaskExecutor} at the resource manager.
	 *
	 * @param resourceManagerLeaderId  The fencing token for the ResourceManager leader
	 * @param taskExecutorAddress     The address of the TaskExecutor that registers
	 * @param resourceID              The resource ID of the TaskExecutor that registers
	 * @param slotReport              The slot report containing free and allocated task slots
	 * @param timeout                 The timeout for the response.
	 *
	 * @return The future to the response by the ResourceManager.
	 */
	Future<RegistrationResponse> registerTaskExecutor(
		UUID resourceManagerLeaderId,
		String taskExecutorAddress,
		ResourceID resourceID,
		SlotReport slotReport,
		@RpcTimeout Time timeout);

	/**
	 * Sent by the TaskExecutor to notify the ResourceManager that a slot has become available.
	 *
	 * @param resourceManagerLeaderId The ResourceManager leader id
	 * @param instanceId TaskExecutor's instance id
	 * @param slotID The SlotID of the freed slot
	 */
	void notifySlotAvailable(
		UUID resourceManagerLeaderId,
		InstanceID instanceId,
		SlotID slotID);

	/**
	 * Registers an infoMessage listener
	 *
	 * @param infoMessageListenerAddress address of infoMessage listener to register to this resource manager
	 */
	void registerInfoMessageListener(String infoMessageListenerAddress);

	/**
	 * Unregisters an infoMessage listener
	 *
	 * @param infoMessageListenerAddress address of infoMessage listener to unregister from this resource manager
	 *
	 */
	void unRegisterInfoMessageListener(String infoMessageListenerAddress);

	/**
	 * shutdown cluster
	 * @param finalStatus
	 * @param optionalDiagnostics
	 */
	void shutDownCluster(final ApplicationStatus finalStatus, final String optionalDiagnostics);

	/**
	 * Gets the currently registered number of TaskManagers.
	 * 
	 * @param leaderSessionId The leader session ID with which to address the ResourceManager.
	 * @return The future to the number of registered TaskManagers.
	 */
	Future<Integer> getNumberOfRegisteredTaskManagers(UUID leaderSessionId);
}
