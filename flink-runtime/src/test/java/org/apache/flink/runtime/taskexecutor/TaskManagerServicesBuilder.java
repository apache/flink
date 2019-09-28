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

import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import static org.mockito.Mockito.mock;

/**
 * Builder for the {@link TaskManagerServices}.
 */
public class TaskManagerServicesBuilder {

	/** TaskManager services. */
	private TaskManagerLocation taskManagerLocation;
	private MemoryManager memoryManager;
	private IOManager ioManager;
	private ShuffleEnvironment<?, ?> shuffleEnvironment;
	private KvStateService kvStateService;
	private BroadcastVariableManager broadcastVariableManager;
	private TaskSlotTable taskSlotTable;
	private JobManagerTable jobManagerTable;
	private JobLeaderService jobLeaderService;
	private TaskExecutorLocalStateStoresManager taskStateManager;
	private TaskEventDispatcher taskEventDispatcher;

	public TaskManagerServicesBuilder() {
		taskManagerLocation = new LocalTaskManagerLocation();
		memoryManager = new MemoryManager(
			MemoryManager.MIN_PAGE_SIZE,
			1,
			MemoryManager.MIN_PAGE_SIZE,
			MemoryType.HEAP,
			false);
		ioManager = mock(IOManager.class);
		shuffleEnvironment = mock(ShuffleEnvironment.class);
		kvStateService = new KvStateService(new KvStateRegistry(), null, null);
		broadcastVariableManager = new BroadcastVariableManager();
		taskEventDispatcher = new TaskEventDispatcher();
		taskSlotTable = mock(TaskSlotTable.class);
		jobManagerTable = new JobManagerTable();
		jobLeaderService = new JobLeaderService(taskManagerLocation, RetryingRegistrationConfiguration.defaultConfiguration());
		taskStateManager = mock(TaskExecutorLocalStateStoresManager.class);
	}

	public TaskManagerServicesBuilder setTaskManagerLocation(TaskManagerLocation taskManagerLocation) {
		this.taskManagerLocation = taskManagerLocation;
		return this;
	}

	public TaskManagerServicesBuilder setMemoryManager(MemoryManager memoryManager) {
		this.memoryManager = memoryManager;
		return this;
	}

	public TaskManagerServicesBuilder setIoManager(IOManager ioManager) {
		this.ioManager = ioManager;
		return this;
	}

	public TaskManagerServicesBuilder setShuffleEnvironment(ShuffleEnvironment<?, ?> shuffleEnvironment) {
		this.shuffleEnvironment = shuffleEnvironment;
		return this;
	}

	public TaskManagerServicesBuilder setKvStateService(KvStateService kvStateService) {
		this.kvStateService = kvStateService;
		return this;
	}

	public TaskManagerServicesBuilder setBroadcastVariableManager(BroadcastVariableManager broadcastVariableManager) {
		this.broadcastVariableManager = broadcastVariableManager;
		return this;
	}

	public TaskManagerServicesBuilder setTaskSlotTable(TaskSlotTable taskSlotTable) {
		this.taskSlotTable = taskSlotTable;
		return this;
	}

	public TaskManagerServicesBuilder setJobManagerTable(JobManagerTable jobManagerTable) {
		this.jobManagerTable = jobManagerTable;
		return this;
	}

	public TaskManagerServicesBuilder setJobLeaderService(JobLeaderService jobLeaderService) {
		this.jobLeaderService = jobLeaderService;
		return this;
	}

	public TaskManagerServicesBuilder setTaskStateManager(TaskExecutorLocalStateStoresManager taskStateManager) {
		this.taskStateManager = taskStateManager;
		return this;
	}

	public TaskManagerServices build() {
		return new TaskManagerServices(
			taskManagerLocation,
			memoryManager,
			ioManager,
			shuffleEnvironment,
			kvStateService,
			broadcastVariableManager,
			taskSlotTable,
			jobManagerTable,
			jobLeaderService,
			taskStateManager,
			taskEventDispatcher);
	}
}
