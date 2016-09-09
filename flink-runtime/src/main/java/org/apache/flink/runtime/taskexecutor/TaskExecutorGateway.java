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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.resourcemanager.SlotRequestReply;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;

/**
 * {@link TaskExecutor} RPC gateway interface
 */
public interface TaskExecutorGateway extends RpcGateway {

	// ------------------------------------------------------------------------
	//  ResourceManager handlers
	// ------------------------------------------------------------------------

	void notifyOfNewResourceManagerLeader(String address, UUID resourceManagerLeaderId);

	/**
	 * Send by the ResourceManager to the TaskExecutor
	 * @param allocationID id for the request
	 * @param resourceManagerLeaderID current leader id of the ResourceManager
	 * @return SlotRequestReply Answer to the request
	 */

	Future<SlotRequestReply> requestSlot(
		AllocationID allocationID,
		UUID resourceManagerLeaderID,
		@RpcTimeout FiniteDuration timeout);
}
