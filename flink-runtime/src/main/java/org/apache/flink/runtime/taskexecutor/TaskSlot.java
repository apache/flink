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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * Container for multiple {@link Task} belonging to the same slot.
 */
public class TaskSlot {
	private final AllocationID allocationID;
	private final ResourceID resourceID;
	private final Map<ExecutionAttemptID, Task> tasks;

	public TaskSlot(AllocationID allocationID, ResourceID resourceID) {
		this.allocationID = Preconditions.checkNotNull(allocationID);
		this.resourceID = Preconditions.checkNotNull(resourceID);
		tasks = new HashMap<>(4);
	}

	public AllocationID getAllocationID() {
		return allocationID;
	}

	public ResourceID getResourceID() {
		return resourceID;
	}

	public boolean add(Task task) {
		// sanity check
		Preconditions.checkArgument(allocationID.equals(task.getAllocationID()));

		Task oldTask = tasks.put(task.getExecutionId(), task);

		if (oldTask != null) {
			tasks.put(task.getExecutionId(), oldTask);
			return false;
		} else {
			return true;
		}
	}

	public Task remove(Task task) {
		return tasks.remove(task.getExecutionId());
	}

	public void clear() {
		tasks.clear();
	}
}
