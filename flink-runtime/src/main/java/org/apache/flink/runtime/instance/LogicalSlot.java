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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

/**
 * A logical slot represents a resource on a TaskManager into
 * which a single task can be deployed.
 */
public interface LogicalSlot {

	/**
	 * Return the TaskManager location of this slot
	 *
	 * @return TaskManager location of this slot
	 */
	TaskManagerLocation getTaskManagerLocation();

	/**
	 * Return the TaskManager gateway to talk to the TaskManager.
	 *
	 * @return TaskManager gateway to talk to the TaskManager
	 */
	TaskManagerGateway getTaskManagerGateway();

	/**
	 * True if the slot is still alive.
	 *
	 * @return True if the slot is still alive, otherwise false
	 */
	boolean isAlive();

	/**
	 * True if the slot is canceled.
	 *
	 * @return True if the slot is canceled, otherwise false
	 */
	boolean isCanceled();

	/**
	 * True if the slot is released.
	 *
	 * @return True if the slot is released, otherwise false
	 */
	boolean isReleased();

	/**
	 * Sets the execution for this slot.
	 *
	 * @param execution to set for this slot
	 * @return true if the slot could be set, otherwise false
	 */
	boolean setExecution(Execution execution);

	/**
	 * Releases this slot.
	 */
	void releaseSlot();

	/**
	 * Gets the slot number on the TaskManager.
	 *
	 * @return slot number
	 */
	int getPhysicalSlotNumber();

	/**
	 * Gets the allocation id of this slot.
	 *
	 * @return allocation id of this slot
	 */
	AllocationID getAllocationId();
}
