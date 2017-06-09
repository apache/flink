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

package org.apache.flink.runtime.instance;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.slots.AllocatedSlot;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.UUID;

/**
 * The gateway for calls on the {@link SlotPool}. 
 */
public interface SlotPoolGateway extends RpcGateway {

	// ------------------------------------------------------------------------
	//  shutdown
	// ------------------------------------------------------------------------

	void suspend();

	// ------------------------------------------------------------------------
	//  resource manager connection
	// ------------------------------------------------------------------------

	/**
	 * Connects the SlotPool to the given ResourceManager. After this method is called, the
	 * SlotPool will be able to request resources from the given ResourceManager.
	 * 
	 * @param resourceManagerLeaderId The leader session ID of the resource manager.
	 * @param resourceManagerGateway  The RPC gateway for the resource manager.
	 */
	void connectToResourceManager(UUID resourceManagerLeaderId, ResourceManagerGateway resourceManagerGateway);

	/**
	 * Disconnects the slot pool from its current Resource Manager. After this call, the pool will not
	 * be able to request further slots from the Resource Manager, and all currently pending requests
	 * to the resource manager will be canceled.
	 * 
	 * <p>The slot pool will still be able to serve slots from its internal pool.
	 */
	void disconnectResourceManager();

	// ------------------------------------------------------------------------
	//  registering / un-registering TaskManagers and slots
	// ------------------------------------------------------------------------

	void registerTaskManager(ResourceID resourceID);

	void releaseTaskManager(ResourceID resourceID);

	Future<Boolean> offerSlot(AllocatedSlot slot);

	Future<Iterable<SlotOffer>> offerSlots(Iterable<Tuple2<AllocatedSlot, SlotOffer>> offers);
	
	void failAllocation(AllocationID allocationID, Exception cause);

	// ------------------------------------------------------------------------
	//  allocating and disposing slots
	// ------------------------------------------------------------------------

	Future<SimpleSlot> allocateSlot(
			ScheduledUnit task,
			ResourceProfile resources,
			Iterable<TaskManagerLocation> locationPreferences,
			@RpcTimeout Time timeout);

	void returnAllocatedSlot(Slot slot);
}
