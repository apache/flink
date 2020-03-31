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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.taskexecutor.SlotReport;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Testing implementation of {@link TaskSlotTable} for tests.
 */
public class TestingTaskSlotTable<T extends TaskSlotPayload> implements TaskSlotTable<T> {
	private final Supplier<SlotReport> createSlotReportSupplier;
	private final Supplier<Boolean> allocateSlotSupplier;
	private final BiFunction<JobID, AllocationID, Boolean> tryMarkSlotActiveBiFunction;
	private final Function<T, Boolean> addTaskFunction;
	private final Function<AllocationID, MemoryManager> memoryManagerGetter;
	private final Supplier<CompletableFuture<Void>> closeAsyncSupplier;

	private TestingTaskSlotTable(
			Supplier<SlotReport> createSlotReportSupplier,
			Supplier<Boolean> allocateSlotSupplier,
			BiFunction<JobID, AllocationID, Boolean> tryMarkSlotActiveBiFunction,
			Function<T, Boolean> addTaskFunction,
			Function<AllocationID, MemoryManager> memoryManagerGetter,
			Supplier<CompletableFuture<Void>> closeAsyncSupplier) {
		this.createSlotReportSupplier = createSlotReportSupplier;
		this.allocateSlotSupplier = allocateSlotSupplier;
		this.tryMarkSlotActiveBiFunction = tryMarkSlotActiveBiFunction;
		this.addTaskFunction = addTaskFunction;
		this.memoryManagerGetter = memoryManagerGetter;
		this.closeAsyncSupplier = closeAsyncSupplier;
	}

	@Override
	public void start(SlotActions initialSlotActions, ComponentMainThreadExecutor mainThreadExecutor) {

	}

	@Override
	public Set<AllocationID> getAllocationIdsPerJob(JobID jobId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SlotReport createSlotReport(ResourceID resourceId) {
		return createSlotReportSupplier.get();
	}

	@Override
	public boolean allocateSlot(int index, JobID jobId, AllocationID allocationId, Time slotTimeout) {
		return allocateSlotSupplier.get();
	}

	@Override
	public boolean allocateSlot(int index, JobID jobId, AllocationID allocationId, ResourceProfile resourceProfile, Time slotTimeout) {
		return allocateSlotSupplier.get();
	}

	@Override
	public boolean markSlotActive(AllocationID allocationId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean markSlotInactive(AllocationID allocationId, Time slotTimeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int freeSlot(AllocationID allocationId, Throwable cause) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isValidTimeout(AllocationID allocationId, UUID ticket) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isAllocated(int index, JobID jobId, AllocationID allocationId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean tryMarkSlotActive(JobID jobId, AllocationID allocationId) {
		return tryMarkSlotActiveBiFunction.apply(jobId, allocationId);
	}

	@Override
	public boolean isSlotFree(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasAllocatedSlots(JobID jobId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<TaskSlot<T>> getAllocatedSlots(JobID jobId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<AllocationID> getActiveSlots(JobID jobId) {
		throw new UnsupportedOperationException();
	}

	@Nullable
	@Override
	public JobID getOwningJob(AllocationID allocationId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addTask(T task) {
		return addTaskFunction.apply(task);
	}

	@Override
	public T removeTask(ExecutionAttemptID executionAttemptID) {
		throw new UnsupportedOperationException();
	}

	@Override
	public T getTask(ExecutionAttemptID executionAttemptID) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<T> getTasks(JobID jobId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public AllocationID getCurrentAllocation(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MemoryManager getTaskMemoryManager(AllocationID allocationID) {
		return memoryManagerGetter.apply(allocationID);
	}

	@Override
	public void notifyTimeout(AllocationID key, UUID ticket) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return closeAsyncSupplier.get();
	}

	public static <T extends TaskSlotPayload> TestingTaskSlotTableBuilder<T> newBuilder() {
		return new TestingTaskSlotTableBuilder<>();
	}

	/**
	 * Builder for {@link TestingTaskSlotTable}.
	 */
	public static class TestingTaskSlotTableBuilder<T extends TaskSlotPayload> {
		private Supplier<SlotReport> createSlotReportSupplier = SlotReport::new;
		private Supplier<Boolean> allocateSlotSupplier = () -> false;
		private BiFunction<JobID, AllocationID, Boolean> tryMarkSlotActiveBiFunction = (ignoredA, ignoredB) -> false;
		private Function<T, Boolean> addTaskFunction = (ignored) -> false;
		private Function<AllocationID, MemoryManager> memoryManagerGetter = ignored -> {
			throw new UnsupportedOperationException("No memory manager getter has been set.");
		};
		private Supplier<CompletableFuture<Void>> closeAsyncSupplier = FutureUtils::completedVoidFuture;

		public TestingTaskSlotTableBuilder<T> createSlotReportSupplier(Supplier<SlotReport> createSlotReportSupplier) {
			this.createSlotReportSupplier = createSlotReportSupplier;
			return this;
		}

		public TestingTaskSlotTableBuilder<T> allocateSlotReturns(boolean toReturn) {
			this.allocateSlotSupplier = () -> toReturn;
			return this;
		}

		public TestingTaskSlotTableBuilder<T> tryMarkSlotActiveReturns(boolean toReturn) {
			this.tryMarkSlotActiveBiFunction = (stub1, stub2) -> toReturn;
			return this;
		}

		public TestingTaskSlotTableBuilder<T> addTaskReturns(boolean toReturn) {
			this.addTaskFunction = stub -> toReturn;
			return this;
		}

		public TestingTaskSlotTableBuilder<T> memoryManagerGetterReturns(MemoryManager toReturn) {
			this.memoryManagerGetter = stub -> toReturn;
			return this;
		}

		public TestingTaskSlotTableBuilder<T> closeAsyncReturns(CompletableFuture<Void> toReturn) {
			this.closeAsyncSupplier = () -> toReturn;
			return this;
		}

		public TaskSlotTable<T> build() {
			return new TestingTaskSlotTable<>(
				createSlotReportSupplier,
				allocateSlotSupplier,
				tryMarkSlotActiveBiFunction,
				addTaskFunction,
				memoryManagerGetter,
				closeAsyncSupplier);
		}
	}
}
