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

package org.apache.flink.runtime.rpc.taskexecutor;

import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.rpc.resourcemanager.SlotRequest;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;

/**
 * {@link TaskExecutor} RPC gateway interface
 */
public interface TaskExecutorGateway extends RpcGateway {

	/**
	 * handle a slot request from ResourceManager, allocate the slot to the allocationID, or reject it if the slot was
	 * already occupied
	 *
	 * @param slotRequest             slot request from resourceManager
	 * @param slotID                  slotID identifying the choosen slot
	 * @param resourceManagerLeaderId id to identify a resourceManager which is granted leadership
	 * @return response ack request if allocate slot successful; decline request if the slot was already occupied
	 */
	Future<SlotAllocationResponse> requestSlotForJob(SlotRequest slotRequest, SlotID slotID,
		UUID resourceManagerLeaderId);

	/**
	 * trigger the heartbeat from ResourceManager, taskManager send the SlotReport which is about the current status
	 * of all slots of the TaskExecutor
	 *
	 * @param resourceManagerLeaderId id to identify a resourceManager which is granted leadership
	 * @param timeout                 Timeout for the future to complete
	 * @return Future SlotReport response
	 */
	Future<SlotReport> triggerHeartbeatToResourceManager(UUID resourceManagerLeaderId,
		@RpcTimeout FiniteDuration timeout);

	/**
	 * receive notification from resourceManager that current taskExecutor is marked failed.
	 *
	 * @param resourceManagerLeaderId id to identify a resourceManager which is granted leadership
	 */
	void markedFailed(UUID resourceManagerLeaderId);

	// ------------------------------------------------------------------------
	//  ResourceManager handlers
	// ------------------------------------------------------------------------

	void notifyOfNewResourceManagerLeader(String address, UUID resourceManagerLeaderId);
}
