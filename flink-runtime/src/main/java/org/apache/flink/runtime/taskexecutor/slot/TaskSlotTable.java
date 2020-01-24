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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceBudgetManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;

/**
 * Container for multiple {@link TaskSlot} instances. Additionally, it maintains multiple indices
 * for faster access to tasks and sets of allocated slots.
 *
 * <p>The task slot table automatically registers timeouts for allocated slots which cannot be assigned
 * to a job manager.
 *
 * <p>Before the task slot table can be used, it must be started via the {@link #start} method.
 */
public class TaskSlotTable<T extends TaskSlotPayload> implements TimeoutListener<AllocationID> {

	private static final Logger LOG = LoggerFactory.getLogger(TaskSlotTable.class);

	/**
	 * Number of slots in static slot allocation.
	 * If slot is requested with an index, the requested index must within the range of [0, numberSlots).
	 * When generating slot report, we should always generate slots with index in [0, numberSlots) even the slot does not exist.
	 */
	private final int numberSlots;

	/** Slot resource profile for static slot allocation. */
	private final ResourceProfile defaultSlotResourceProfile;

	/** Page size for memory manager. */
	private final int memoryPageSize;

	/** Timer service used to time out allocated slots. */
	private final TimerService<AllocationID> timerService;

	/** The list of all task slots. */
	private final Map<Integer, TaskSlot<T>> taskSlots;

	/** Mapping from allocation id to task slot. */
	private final Map<AllocationID, TaskSlot<T>> allocatedSlots;

	/** Mapping from execution attempt id to task and task slot. */
	private final Map<ExecutionAttemptID, TaskSlotMapping<T>> taskSlotMappings;

	/** Mapping from job id to allocated slots for a job. */
	private final Map<JobID, Set<AllocationID>> slotsPerJob;

	/** Interface for slot actions, such as freeing them or timing them out. */
	private SlotActions slotActions;

	/** Whether the table has been started. */
	private volatile boolean started;

	private final ResourceBudgetManager budgetManager;

	public TaskSlotTable(
		final int numberSlots,
		final ResourceProfile totalAvailableResourceProfile,
		final ResourceProfile defaultSlotResourceProfile,
		final int memoryPageSize,
		final TimerService<AllocationID> timerService) {

		Preconditions.checkArgument(0 < numberSlots, "The number of task slots must be greater than 0.");

		this.numberSlots = numberSlots;
		this.defaultSlotResourceProfile = Preconditions.checkNotNull(defaultSlotResourceProfile);
		this.memoryPageSize = memoryPageSize;

		this.taskSlots = new HashMap<>(numberSlots);

		this.timerService = Preconditions.checkNotNull(timerService);

		budgetManager = new ResourceBudgetManager(Preconditions.checkNotNull(totalAvailableResourceProfile));

		allocatedSlots = new HashMap<>(numberSlots);

		taskSlotMappings = new HashMap<>(4 * numberSlots);

		slotsPerJob = new HashMap<>(4);

		slotActions = null;
		started = false;
	}

	/**
	 * Start the task slot table with the given slot actions.
	 *
	 * @param initialSlotActions to use for slot actions
	 */
	public void start(SlotActions initialSlotActions) {
		this.slotActions = Preconditions.checkNotNull(initialSlotActions);

		timerService.start(this);

		started = true;
	}

	/**
	 * Stop the task slot table.
	 */
	public void stop() {
		started = false;
		timerService.stop();
		allocatedSlots
			.values()
			.stream()
			.filter(slot -> !taskSlots.containsKey(slot.getIndex()))
			.forEach(TaskSlot::close);
		allocatedSlots.clear();
		taskSlots.values().forEach(TaskSlot::close);
		taskSlots.clear();
		slotActions = null;
	}

	@VisibleForTesting
	public boolean isStopped() {
		return !started &&
			taskSlots.values().stream().allMatch(slot -> slot.getMemoryManager().isShutdown()) &&
			allocatedSlots.values().stream().allMatch(slot -> slot.getMemoryManager().isShutdown());
	}

	/**
	 * Returns the all {@link AllocationID} for the given job.
	 *
	 * @param jobId for which to return the set of {@link AllocationID}.
	 * @return Set of {@link AllocationID} for the given job
	 */
	public Set<AllocationID> getAllocationIdsPerJob(JobID jobId) {
		final Set<AllocationID> allocationIds = slotsPerJob.get(jobId);

		if (allocationIds == null) {
			return Collections.emptySet();
		} else {
			return Collections.unmodifiableSet(allocationIds);
		}
	}

	// ---------------------------------------------------------------------
	// Slot report methods
	// ---------------------------------------------------------------------

	public SlotReport createSlotReport(ResourceID resourceId) {
		List<SlotStatus> slotStatuses = new ArrayList<>();

		for (int i = 0; i < numberSlots; i++) {
			SlotID slotId = new SlotID(resourceId, i);
			SlotStatus slotStatus;
			if (taskSlots.containsKey(i)) {
				TaskSlot<T> taskSlot = taskSlots.get(i);

				slotStatus = new SlotStatus(
					slotId,
					taskSlot.getResourceProfile(),
					taskSlot.getJobId(),
					taskSlot.getAllocationId());
			} else {
				slotStatus = new SlotStatus(
					slotId,
					defaultSlotResourceProfile,
					null,
					null);
			}

			slotStatuses.add(slotStatus);
		}

		for (TaskSlot<T> taskSlot : allocatedSlots.values()) {
			if (taskSlot.getIndex() < 0) {
				SlotID slotID = SlotID.generateDynamicSlotID(resourceId);
				SlotStatus slotStatus = new SlotStatus(
					slotID,
					taskSlot.getResourceProfile(),
					taskSlot.getJobId(),
					taskSlot.getAllocationId());
				slotStatuses.add(slotStatus);
			}
		}

		final SlotReport slotReport = new SlotReport(slotStatuses);

		return slotReport;
	}

	// ---------------------------------------------------------------------
	// Slot methods
	// ---------------------------------------------------------------------

	/**
	 * Allocate the slot with the given index for the given job and allocation id. If negative index is
	 * given, a new auto increasing index will be generated. Returns true if the slot could be allocated.
	 * Otherwise it returns false.
	 *
	 * @param index of the task slot to allocate, use negative value for dynamic slot allocation
	 * @param jobId to allocate the task slot for
	 * @param allocationId identifying the allocation
	 * @param slotTimeout until the slot times out
	 * @return True if the task slot could be allocated; otherwise false
	 */
	@VisibleForTesting
	public boolean allocateSlot(int index, JobID jobId, AllocationID allocationId, Time slotTimeout) {
		return allocateSlot(index, jobId, allocationId, defaultSlotResourceProfile, slotTimeout);
	}

	/**
	 * Allocate the slot with the given index for the given job and allocation id. If negative index is
	 * given, a new auto increasing index will be generated. Returns true if the slot could be allocated.
	 * Otherwise it returns false.
	 *
	 * @param index of the task slot to allocate, use negative value for dynamic slot allocation
	 * @param jobId to allocate the task slot for
	 * @param allocationId identifying the allocation
	 * @param resourceProfile of the requested slot, used only for dynamic slot allocation and will be ignored otherwise
	 * @param slotTimeout until the slot times out
	 * @return True if the task slot could be allocated; otherwise false
	 */
	public boolean allocateSlot(int index, JobID jobId, AllocationID allocationId, ResourceProfile resourceProfile, Time slotTimeout) {
		checkInit();

		Preconditions.checkArgument(index < numberSlots);

		TaskSlot<T> taskSlot = allocatedSlots.get(allocationId);
		if (taskSlot != null) {
			LOG.info("Allocation ID {} is already allocated in {}.", allocationId, taskSlot);
			return false;
		}

		if (taskSlots.containsKey(index)) {
			TaskSlot<T> duplicatedTaskSlot = taskSlots.get(index);
			LOG.info("Slot with index {} already exist, with resource profile {}, job id {} and allocation id {}.",
				index,
				duplicatedTaskSlot.getResourceProfile(),
				duplicatedTaskSlot.getJobId(),
				duplicatedTaskSlot.getAllocationId());
			return duplicatedTaskSlot.getJobId().equals(jobId) &&
				duplicatedTaskSlot.getAllocationId().equals(allocationId);
		} else if (allocatedSlots.containsKey(allocationId)) {
			return true;
		}

		resourceProfile = index >= 0 ? defaultSlotResourceProfile : resourceProfile;

		if (!budgetManager.reserve(resourceProfile)) {
			LOG.info("Cannot allocate the requested resources. Trying to allocate {}, "
					+ "while the currently remaining available resources are {}, total is {}.",
				resourceProfile,
				budgetManager.getAvailableBudget(),
				budgetManager.getTotalBudget());
			return false;
		}

		taskSlot = new TaskSlot<>(index, resourceProfile, memoryPageSize, jobId, allocationId);
		if (index >= 0) {
			taskSlots.put(index, taskSlot);
		}

		// update the allocation id to task slot map
		allocatedSlots.put(allocationId, taskSlot);

		// register a timeout for this slot since it's in state allocated
		timerService.registerTimeout(allocationId, slotTimeout.getSize(), slotTimeout.getUnit());

		// add this slot to the set of job slots
		Set<AllocationID> slots = slotsPerJob.get(jobId);

		if (slots == null) {
			slots = new HashSet<>(4);
			slotsPerJob.put(jobId, slots);
		}

		slots.add(allocationId);

		return true;
	}

	/**
	 * Marks the slot under the given allocation id as active. If the slot could not be found, then
	 * a {@link SlotNotFoundException} is thrown.
	 *
	 * @param allocationId to identify the task slot to mark as active
	 * @throws SlotNotFoundException if the slot could not be found for the given allocation id
	 * @return True if the slot could be marked active; otherwise false
	 */
	public boolean markSlotActive(AllocationID allocationId) throws SlotNotFoundException {
		checkInit();

		TaskSlot<T> taskSlot = getTaskSlot(allocationId);

		if (taskSlot != null) {
			if (taskSlot.markActive()) {
				// unregister a potential timeout
				LOG.info("Activate slot {}.", allocationId);

				timerService.unregisterTimeout(allocationId);

				return true;
			} else {
				return false;
			}
		} else {
			throw new SlotNotFoundException(allocationId);
		}
	}

	/**
	 * Marks the slot under the given allocation id as inactive. If the slot could not be found,
	 * then a {@link SlotNotFoundException} is thrown.
	 *
	 * @param allocationId to identify the task slot to mark as inactive
	 * @param slotTimeout until the slot times out
	 * @throws SlotNotFoundException if the slot could not be found for the given allocation id
	 * @return True if the slot could be marked inactive
	 */
	public boolean markSlotInactive(AllocationID allocationId, Time slotTimeout) throws SlotNotFoundException {
		checkInit();

		TaskSlot<T> taskSlot = getTaskSlot(allocationId);

		if (taskSlot != null) {
			if (taskSlot.markInactive()) {
				// register a timeout to free the slot
				timerService.registerTimeout(allocationId, slotTimeout.getSize(), slotTimeout.getUnit());

				return true;
			} else {
				return false;
			}
		} else {
			throw new SlotNotFoundException(allocationId);
		}
	}

	/**
	 * Try to free the slot. If the slot is empty it will set the state of the task slot to free
	 * and return its index. If the slot is not empty, then it will set the state of the task slot
	 * to releasing, fail all tasks and return -1.
	 *
	 * @param allocationId identifying the task slot to be freed
	 * @throws SlotNotFoundException if there is not task slot for the given allocation id
	 * @return Index of the freed slot if the slot could be freed; otherwise -1
	 */
	public int freeSlot(AllocationID allocationId) throws SlotNotFoundException {
		return freeSlot(allocationId, new Exception("The task slot of this task is being freed."));
	}

	/**
	 * Tries to free the slot. If the slot is empty it will set the state of the task slot to free
	 * and return its index. If the slot is not empty, then it will set the state of the task slot
	 * to releasing, fail all tasks and return -1.
	 *
	 * @param allocationId identifying the task slot to be freed
	 * @param cause to fail the tasks with if slot is not empty
	 * @throws SlotNotFoundException if there is not task slot for the given allocation id
	 * @return The freed TaskSlot. If the TaskSlot cannot be freed then null.
	 */
	public int freeSlot(AllocationID allocationId, Throwable cause) throws SlotNotFoundException {
		checkInit();

		TaskSlot<T> taskSlot = getTaskSlot(allocationId);

		if (taskSlot != null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Free slot {}.", taskSlot, cause);
			} else {
				LOG.info("Free slot {}.", taskSlot);
			}

			final JobID jobId = taskSlot.getJobId();

			if (taskSlot.isEmpty()) {
				// remove the allocation id to task slot mapping
				allocatedSlots.remove(allocationId);

				// unregister a potential timeout
				timerService.unregisterTimeout(allocationId);

				Set<AllocationID> slots = slotsPerJob.get(jobId);

				if (slots == null) {
					throw new IllegalStateException("There are no more slots allocated for the job " + jobId +
						". This indicates a programming bug.");
				}

				slots.remove(allocationId);

				if (slots.isEmpty()) {
					slotsPerJob.remove(jobId);
				}

				taskSlot.close();
				taskSlots.remove(taskSlot.getIndex());
				budgetManager.release(taskSlot.getResourceProfile());

				return taskSlot.getIndex();
			} else {
				// we couldn't free the task slot because it still contains task, fail the tasks
				// and set the slot state to releasing so that it gets eventually freed
				taskSlot.markReleasing();

				Iterator<T> taskIterator = taskSlot.getTasks();

				while (taskIterator.hasNext()) {
					taskIterator.next().failExternally(cause);
				}

				return -1;
			}
		} else {
			throw new SlotNotFoundException(allocationId);
		}
	}

	/**
	 * Check whether the timeout with ticket is valid for the given allocation id.
	 *
	 * @param allocationId to check against
	 * @param ticket of the timeout
	 * @return True if the timeout is valid; otherwise false
	 */
	public boolean isValidTimeout(AllocationID allocationId, UUID ticket) {
		checkInit();

		return timerService.isValid(allocationId, ticket);
	}

	/**
	 * Check whether the slot for the given index is allocated for the given job and allocation id.
	 *
	 * @param index of the task slot
	 * @param jobId for which the task slot should be allocated
	 * @param allocationId which should match the task slot's allocation id
	 * @return True if the given task slot is allocated for the given job and allocation id
	 */
	public boolean isAllocated(int index, JobID jobId, AllocationID allocationId) {
		TaskSlot<T> taskSlot = taskSlots.get(index);
		if (taskSlot != null) {
			return taskSlot.isAllocated(jobId, allocationId);
		} else if (index < 0) {
			return allocatedSlots.containsKey(allocationId);
		} else {
			return false;
		}
	}

	/**
	 * Try to mark the specified slot as active if it has been allocated by the given job.
	 *
	 * @param jobId of the allocated slot
	 * @param allocationId identifying the allocation
	 * @return True if the task slot could be marked active.
	 */
	public boolean tryMarkSlotActive(JobID jobId, AllocationID allocationId) {
		TaskSlot<T> taskSlot = getTaskSlot(allocationId);

		if (taskSlot != null && taskSlot.isAllocated(jobId, allocationId)) {
			return taskSlot.markActive();
		} else {
			return false;
		}
	}

	/**
	 * Check whether the task slot with the given index is free.
	 *
	 * @param index of the task slot
	 * @return True if the task slot is free; otherwise false
	 */
	public boolean isSlotFree(int index) {
		return !taskSlots.containsKey(index);
	}

	/**
	 * Check whether the job has allocated (not active) slots.
	 *
	 * @param jobId for which to check for allocated slots
	 * @return True if there are allocated slots for the given job id.
	 */
	public boolean hasAllocatedSlots(JobID jobId) {
		return getAllocatedSlots(jobId).hasNext();
	}

	/**
	 * Return an iterator of allocated slots for the given job id.
	 *
	 * @param jobId for which to return the allocated slots
	 * @return Iterator of allocated slots.
	 */
	public Iterator<TaskSlot<T>> getAllocatedSlots(JobID jobId) {
		return new TaskSlotIterator(jobId, TaskSlotState.ALLOCATED);
	}

	/**
	 * Return an iterator of active slots (their application ids) for the given job id.
	 *
	 * @param jobId for which to return the active slots
	 * @return Iterator of allocation ids of active slots
	 */
	public Iterator<AllocationID> getActiveSlots(JobID jobId) {
		return new AllocationIDIterator(jobId, TaskSlotState.ACTIVE);
	}

	/**
	 * Returns the owning job of the {@link TaskSlot} identified by the
	 * given {@link AllocationID}.
	 *
	 * @param allocationId identifying the slot for which to retrieve the owning job
	 * @return Owning job of the specified {@link TaskSlot} or null if there is no slot for
	 * the given allocation id or if the slot has no owning job assigned
	 */
	@Nullable
	public JobID getOwningJob(AllocationID allocationId) {
		final TaskSlot<T> taskSlot = getTaskSlot(allocationId);

		if (taskSlot != null) {
			return taskSlot.getJobId();
		} else {
			return null;
		}
	}

	// ---------------------------------------------------------------------
	// Task methods
	// ---------------------------------------------------------------------

	/**
	 * Add the given task to the slot identified by the task's allocation id.
	 *
	 * @param task to add to the task slot with the respective allocation id
	 * @throws SlotNotFoundException if there was no slot for the given allocation id
	 * @throws SlotNotActiveException if there was no slot active for task's job and allocation id
	 * @return True if the task could be added to the task slot; otherwise false
	 */
	public boolean addTask(T task) throws SlotNotFoundException, SlotNotActiveException {
		Preconditions.checkNotNull(task);

		TaskSlot<T> taskSlot = getTaskSlot(task.getAllocationId());

		if (taskSlot != null) {
			if (taskSlot.isActive(task.getJobID(), task.getAllocationId())) {
				if (taskSlot.add(task)) {
					taskSlotMappings.put(task.getExecutionId(), new TaskSlotMapping<>(task, taskSlot));

					return true;
				} else {
					return false;
				}
			} else {
				throw new SlotNotActiveException(task.getJobID(), task.getAllocationId());
			}
		} else {
			throw new SlotNotFoundException(task.getAllocationId());
		}
	}

	/**
	 * Remove the task with the given execution attempt id from its task slot. If the owning task
	 * slot is in state releasing and empty after removing the task, the slot is freed via the
	 * slot actions.
	 *
	 * @param executionAttemptID identifying the task to remove
	 * @return The removed task if there is any for the given execution attempt id; otherwise null
	 */
	public T removeTask(ExecutionAttemptID executionAttemptID) {
		checkInit();

		TaskSlotMapping<T> taskSlotMapping = taskSlotMappings.remove(executionAttemptID);

		if (taskSlotMapping != null) {
			T task = taskSlotMapping.getTask();
			TaskSlot<T> taskSlot = taskSlotMapping.getTaskSlot();

			taskSlot.remove(task.getExecutionId());

			if (taskSlot.isReleasing() && taskSlot.isEmpty()) {
				slotActions.freeSlot(taskSlot.getAllocationId());
			}

			return task;
		} else {
			return null;
		}
	}

	/**
	 * Get the task for the given execution attempt id. If none could be found, then return null.
	 *
	 * @param executionAttemptID identifying the requested task
	 * @return The task for the given execution attempt id if it exist; otherwise null
	 */
	public T getTask(ExecutionAttemptID executionAttemptID) {
		TaskSlotMapping<T> taskSlotMapping = taskSlotMappings.get(executionAttemptID);

		if (taskSlotMapping != null) {
			return taskSlotMapping.getTask();
		} else {
			return null;
		}
	}

	/**
	 * Return an iterator over all tasks for a given job.
	 *
	 * @param jobId identifying the job of the requested tasks
	 * @return Iterator over all task for a given job
	 */
	public Iterator<T> getTasks(JobID jobId) {
		return new TaskIterator(jobId);
	}

	/**
	 * Get the current allocation for the task slot with the given index.
	 *
	 * @param index identifying the slot for which the allocation id shall be retrieved
	 * @return Allocation id of the specified slot if allocated; otherwise null
	 */
	public AllocationID getCurrentAllocation(int index) {
		TaskSlot<T> taskSlot = taskSlots.get(index);
		if (taskSlot == null) {
			return null;
		}
		return taskSlot.getAllocationId();
	}

	/**
	 * Get the memory manager of the slot allocated for the task.
	 *
	 * @param allocationID allocation id of the slot allocated for the task
	 * @return the memory manager of the slot allocated for the task
	 */
	public MemoryManager getTaskMemoryManager(AllocationID allocationID) throws SlotNotFoundException {
		TaskSlot<T> taskSlot = getTaskSlot(allocationID);
		if (taskSlot != null) {
			return taskSlot.getMemoryManager();
		} else {
			throw new SlotNotFoundException(allocationID);
		}
	}

	// ---------------------------------------------------------------------
	// TimeoutListener methods
	// ---------------------------------------------------------------------

	@Override
	public void notifyTimeout(AllocationID key, UUID ticket) {
		checkInit();

		if (slotActions != null) {
			slotActions.timeoutSlot(key, ticket);
		}
	}

	// ---------------------------------------------------------------------
	// Internal methods
	// ---------------------------------------------------------------------

	@Nullable
	private TaskSlot<T> getTaskSlot(AllocationID allocationId) {
		Preconditions.checkNotNull(allocationId);

		return allocatedSlots.get(allocationId);
	}

	private void checkInit() {
		Preconditions.checkState(started, "The %s has to be started.", TaskSlotTable.class.getSimpleName());
	}

	// ---------------------------------------------------------------------
	// Static utility classes
	// ---------------------------------------------------------------------

	/**
	 * Mapping class between a {@link TaskSlotPayload} and a {@link TaskSlot}.
	 */
	private static final class TaskSlotMapping<T extends TaskSlotPayload> {
		private final T task;
		private final TaskSlot<T> taskSlot;

		private TaskSlotMapping(T task, TaskSlot<T> taskSlot) {
			this.task = Preconditions.checkNotNull(task);
			this.taskSlot = Preconditions.checkNotNull(taskSlot);
		}

		public T getTask() {
			return task;
		}

		public TaskSlot<T> getTaskSlot() {
			return taskSlot;
		}
	}

	/**
	 * Iterator over {@link AllocationID} of the {@link TaskSlot} of a given job. Additionally,
	 * the task slots identified by the allocation ids are in the given state.
	 */
	private final class AllocationIDIterator implements Iterator<AllocationID> {
		private final Iterator<TaskSlot<T>> iterator;

		private AllocationIDIterator(JobID jobId, TaskSlotState state) {
			iterator = new TaskSlotIterator(jobId, state);
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public AllocationID next() {
			try {
				return iterator.next().getAllocationId();
			} catch (NoSuchElementException e) {
				throw new NoSuchElementException("No more allocation ids.");
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Cannot remove allocation ids via this iterator.");
		}
	}

	/**
	 * Iterator over {@link TaskSlot} which fulfill a given state condition and belong to the given
	 * job.
	 */
	private final class TaskSlotIterator implements Iterator<TaskSlot<T>> {
		private final Iterator<AllocationID> allSlots;
		private final TaskSlotState state;

		private TaskSlot<T> currentSlot;

		private TaskSlotIterator(JobID jobId, TaskSlotState state) {

			Set<AllocationID> allocationIds = slotsPerJob.get(jobId);

			if (allocationIds == null || allocationIds.isEmpty()) {
				allSlots = Collections.emptyIterator();
			} else {
				allSlots = allocationIds.iterator();
			}

			this.state = Preconditions.checkNotNull(state);

			this.currentSlot = null;
		}

		@Override
		public boolean hasNext() {
			while (currentSlot == null && allSlots.hasNext()) {
				AllocationID tempSlot = allSlots.next();

				TaskSlot<T> taskSlot = getTaskSlot(tempSlot);

				if (taskSlot != null && taskSlot.getState() == state) {
					currentSlot = taskSlot;
				}
			}

			return currentSlot != null;
		}

		@Override
		public TaskSlot<T> next() {
			if (currentSlot != null) {
				TaskSlot<T> result = currentSlot;

				currentSlot = null;

				return result;
			} else {
				while (true) {
					AllocationID tempSlot;

					try {
						tempSlot = allSlots.next();
					} catch (NoSuchElementException e) {
						throw new NoSuchElementException("No more task slots.");
					}

					TaskSlot<T> taskSlot = getTaskSlot(tempSlot);

					if (taskSlot != null && taskSlot.getState() == state) {
						return taskSlot;
					}
				}
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Cannot remove task slots via this iterator.");
		}
	}

	/**
	 * Iterator over all {@link TaskSlotPayload} for a given job.
	 */
	private final class TaskIterator implements Iterator<T> {
		private final Iterator<TaskSlot<T>> taskSlotIterator;

		private Iterator<T> currentTasks;

		private TaskIterator(JobID jobId) {
			this.taskSlotIterator = new TaskSlotIterator(jobId, TaskSlotState.ACTIVE);

			this.currentTasks = null;
		}

		@Override
		public boolean hasNext() {
			while ((currentTasks == null || !currentTasks.hasNext()) && taskSlotIterator.hasNext()) {
				TaskSlot<T> taskSlot = taskSlotIterator.next();

				currentTasks = taskSlot.getTasks();
			}

			return (currentTasks != null && currentTasks.hasNext());
		}

		@Override
		public T next() {
			while ((currentTasks == null || !currentTasks.hasNext())) {
				TaskSlot<T> taskSlot;

				try {
					taskSlot = taskSlotIterator.next();
				} catch (NoSuchElementException e) {
					throw new NoSuchElementException("No more tasks.");
				}

				currentTasks = taskSlot.getTasks();
			}

			return currentTasks.next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Cannot remove tasks via this iterator.");
		}
	}
}
