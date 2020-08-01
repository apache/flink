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

import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.TestingLibraryCacheManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TestingTaskSlotTable;
import org.apache.flink.runtime.taskmanager.LocalUnresolvedTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.mock;

/**
 * Builder for the {@link TaskManagerServices}.
 */
public class TaskManagerServicesBuilder {

	/** TaskManager services. */
	private UnresolvedTaskManagerLocation unresolvedTaskManagerLocation;
	private IOManager ioManager;
	private ShuffleEnvironment<?, ?> shuffleEnvironment;
	private KvStateService kvStateService;
	private BroadcastVariableManager broadcastVariableManager;
	private TaskSlotTable<Task> taskSlotTable;
	private JobTable jobTable;
	private JobLeaderService jobLeaderService;
	private TaskExecutorLocalStateStoresManager taskStateManager;
	private TaskEventDispatcher taskEventDispatcher;
	private ExecutorService ioExecutor;
	private LibraryCacheManager libraryCacheManager;

	public TaskManagerServicesBuilder() {
		unresolvedTaskManagerLocation = new LocalUnresolvedTaskManagerLocation();
		ioManager = mock(IOManager.class);
		shuffleEnvironment = mock(ShuffleEnvironment.class);
		kvStateService = new KvStateService(new KvStateRegistry(), null, null);
		broadcastVariableManager = new BroadcastVariableManager();
		taskEventDispatcher = new TaskEventDispatcher();
		taskSlotTable = TestingTaskSlotTable.<Task>newBuilder().closeAsyncReturns(CompletableFuture.completedFuture(null)).build();
		jobTable = DefaultJobTable.create();
		jobLeaderService = new DefaultJobLeaderService(unresolvedTaskManagerLocation, RetryingRegistrationConfiguration.defaultConfiguration());
		taskStateManager = mock(TaskExecutorLocalStateStoresManager.class);
		ioExecutor = TestingUtils.defaultExecutor();
		libraryCacheManager = TestingLibraryCacheManager.newBuilder().build();
	}

	public TaskManagerServicesBuilder setUnresolvedTaskManagerLocation(UnresolvedTaskManagerLocation unresolvedTaskManagerLocation) {
		this.unresolvedTaskManagerLocation = unresolvedTaskManagerLocation;
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

	public TaskManagerServicesBuilder setTaskSlotTable(TaskSlotTable<Task> taskSlotTable) {
		this.taskSlotTable = taskSlotTable;
		return this;
	}

	public TaskManagerServicesBuilder setJobTable(JobTable jobTable) {
		this.jobTable = jobTable;
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

	public TaskManagerServicesBuilder setIOExecutorService(ExecutorService ioExecutor) {
		this.ioExecutor = ioExecutor;
		return this;
	}

	public TaskManagerServicesBuilder setLibraryCacheManager(LibraryCacheManager libraryCacheManager) {
		this.libraryCacheManager = libraryCacheManager;
		return this;
	}

	public TaskManagerServices build() {
		return new TaskManagerServices(
			unresolvedTaskManagerLocation,
			MemoryManager.MIN_PAGE_SIZE,
			ioManager,
			shuffleEnvironment,
			kvStateService,
			broadcastVariableManager,
			taskSlotTable,
			jobTable,
			jobLeaderService,
			taskStateManager,
			taskEventDispatcher,
			ioExecutor,
			libraryCacheManager);
	}
}
