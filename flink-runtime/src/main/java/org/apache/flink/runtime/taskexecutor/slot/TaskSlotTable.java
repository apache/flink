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
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
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
public class TaskSlotTable implements TimeoutListener<AllocationID> {

	private static final Logger LOG = LoggerFactory.getLogger(TaskSlotTable.class);

	/** Timer service used to time out allocated slots. */
	private final TimerService<AllocationID> timerService;

	/** The list of all task slots. */
	private final List<TaskSlot> taskSlots;

	/** Mapping from allocation id to task slot. */
	private final Map<AllocationID, TaskSlot> allocationIDTaskSlotMap;

	/** Mapping from execution attempt id to task and task slot. */
	private final Map<ExecutionAttemptID, TaskSlotMapping> taskSlotMappings;

	/** Mapping from job id to allocated slots for a job. */
	private final Map<JobID, Set<AllocationID>> slotsPerJob;

	/** Interface for slot actions, such as freeing them or timing them out. */
	private SlotActions slotActions;

	/** Whether the table has been started. */
	private boolean started;

	public TaskSlotTable(
		final Collection<ResourceProfile> resourceProfiles,
		final TimerService<AllocationID> timerService) {

		int numberSlots = resourceProfiles.size();

		Preconditions.checkArgument(0 < numberSlots, "The number of task slots must be greater than 0.");

		this.timerService = Preconditions.checkNotNull(timerService);

		taskSlots = Arrays.asList(new TaskSlot[numberSlots]);

		int index = 0;

		// create the task slots for the given resource profiles
		for (ResourceProfile resourceProfile: resourceProfiles) {
			taskSlots.set(index, new TaskSlot(index, resourceProfile));
			++index;
		}

		allocationIDTaskSlotMap = new HashMap<>(numberSlots);

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
		slotActions = null;
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
		final int numberSlots = taskSlots.size();

		List<SlotStatus> slotStatuses = Arrays.asList(new SlotStatus[numberSlots]);

		for (int i = 0; i < numberSlots; i++) {
			TaskSlot taskSlot = taskSlots.get(i);
			SlotID slotId = new SlotID(resourceId, taskSlot.getIndex());

			SlotStatus slotStatus = new SlotStatus(
				slotId,
				taskSlot.getResourceProfile(),
				taskSlot.getJobId(),
				taskSlot.getAllocationId());

			slotStatuses.set(i, slotStatus);
		}

		final SlotReport slotReport = new SlotReport(slotStatuses);

		return slotReport;
	}

	// ---------------------------------------------------------------------
	// Slot methods
	// ---------------------------------------------------------------------

	/**
	 * Allocate the slot with the given index for the given job and allocation id. Returns true if
	 * the slot could be allocated. Otherwise it returns false.
	 *
	 * @param index of the task slot to allocate
	 * @param jobId to allocate the task slot for
	 * @param allocationId identifying the allocation
	 * @param slotTimeout until the slot times out
	 * @return True if the task slot could be allocated; otherwise false
	 */
	public boolean allocateSlot(int index, JobID jobId, AllocationID allocationId, Time slotTimeout) {
		checkInit();

		TaskSlot taskSlot = taskSlots.get(index);

		boolean result = taskSlot.allocate(jobId, allocationId);

		if (result) {
			// update the allocation id to task slot map
			allocationIDTaskSlotMap.put(allocationId, taskSlot);

			// register a timeout for this slot since it's in state allocated
			timerService.registerTimeout(allocationId, slotTimeout.getSize(), slotTimeout.getUnit());

			// add this slot to the set of job slots
			Set<AllocationID> slots = slotsPerJob.get(jobId);

			if (slots == null) {
				slots = new HashSet<>(4);
				slotsPerJob.put(jobId, slots);
			}

			slots.add(allocationId);
		}

		return result;
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

		TaskSlot taskSlot = getTaskSlot(allocationId);

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

		TaskSlot taskSlot = getTaskSlot(allocationId);

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

		TaskSlot taskSlot = getTaskSlot(allocationId);

		if (taskSlot != null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Free slot {}.", taskSlot, cause);
			} else {
				LOG.info("Free slot {}.", taskSlot);
			}

			final JobID jobId = taskSlot.getJobId();

			if (taskSlot.markFree()) {
				// remove the allocation id to task slot mapping
				allocationIDTaskSlotMap.remove(allocationId);

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

				return taskSlot.getIndex();
			} else {
				// we couldn't free the task slot because it still contains task, fail the tasks
				// and set the slot state to releasing so that it gets eventually freed
				taskSlot.markReleasing();

				Iterator<Task> taskIterator = taskSlot.getTasks();

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
		TaskSlot taskSlot = taskSlots.get(index);

		return taskSlot.isAllocated(jobId, allocationId);
	}

	/**
	 * Try to mark the specified slot as active if it has been allocated by the given job.
	 *
	 * @param jobId of the allocated slot
	 * @param allocationId identifying the allocation
	 * @return True if the task slot could be marked active.
	 */
	public boolean tryMarkSlotActive(JobID jobId, AllocationID allocationId) {
		TaskSlot taskSlot = getTaskSlot(allocationId);

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
		TaskSlot taskSlot = taskSlots.get(index);

		return taskSlot.isFree();
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
	public Iterator<TaskSlot> getAllocatedSlots(JobID jobId) {
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
		final TaskSlot taskSlot = getTaskSlot(allocationId);

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
	public boolean addTask(Task task) throws SlotNotFoundException, SlotNotActiveException {
		Preconditions.checkNotNull(task);

		TaskSlot taskSlot = getTaskSlot(task.getAllocationId());

		if (taskSlot != null) {
			if (taskSlot.isActive(task.getJobID(), task.getAllocationId())) {
				if (taskSlot.add(task)) {
					taskSlotMappings.put(task.getExecutionId(), new TaskSlotMapping(task, taskSlot));

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
	public Task removeTask(ExecutionAttemptID executionAttemptID) {
		checkInit();

		TaskSlotMapping taskSlotMapping = taskSlotMappings.remove(executionAttemptID);

		if (taskSlotMapping != null) {
			Task task = taskSlotMapping.getTask();
			TaskSlot taskSlot = taskSlotMapping.getTaskSlot();

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
	public Task getTask(ExecutionAttemptID executionAttemptID) {
		TaskSlotMapping taskSlotMapping = taskSlotMappings.get(executionAttemptID);

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
	public Iterator<Task> getTasks(JobID jobId) {
		return new TaskIterator(jobId);
	}

	/**
	 * Get the current allocation for the task slot with the given index.
	 *
	 * @param index identifying the slot for which the allocation id shall be retrieved
	 * @return Allocation id of the specified slot if allocated; otherwise null
	 */
	public AllocationID getCurrentAllocation(int index) {
		return taskSlots.get(index).getAllocationId();
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
	private TaskSlot getTaskSlot(AllocationID allocationId) {
		Preconditions.checkNotNull(allocationId);

		return allocationIDTaskSlotMap.get(allocationId);
	}

	private void checkInit() {
		Preconditions.checkState(started, "The %s has to be started.", TaskSlotTable.class.getSimpleName());
	}

	// ---------------------------------------------------------------------
	// Static utility classes
	// ---------------------------------------------------------------------

	/**
	 * Mapping class between a {@link Task} and a {@link TaskSlot}.
	 */
	private static final class TaskSlotMapping {
		private final Task task;
		private final TaskSlot taskSlot;

		private TaskSlotMapping(Task task, TaskSlot taskSlot) {
			this.task = Preconditions.checkNotNull(task);
			this.taskSlot = Preconditions.checkNotNull(taskSlot);
		}

		public Task getTask() {
			return task;
		}

		public TaskSlot getTaskSlot() {
			return taskSlot;
		}
	}

	/**
	 * Iterator over {@link AllocationID} of the {@link TaskSlot} of a given job. Additionally,
	 * the task slots identified by the allocation ids are in the given state.
	 */
	private final class AllocationIDIterator implements Iterator<AllocationID> {
		private final Iterator<TaskSlot> iterator;

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
	private final class TaskSlotIterator implements Iterator<TaskSlot> {
		private final Iterator<AllocationID> allSlots;
		private final TaskSlotState state;

		private TaskSlot currentSlot;

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

				TaskSlot taskSlot = getTaskSlot(tempSlot);

				if (taskSlot != null && taskSlot.getState() == state) {
					currentSlot = taskSlot;
				}
			}

			return currentSlot != null;
		}

		@Override
		public TaskSlot next() {
			if (currentSlot != null) {
				TaskSlot result = currentSlot;

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

					TaskSlot taskSlot = getTaskSlot(tempSlot);

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
	 * Iterator over all {@link Task} for a given job.
	 */
	private final class TaskIterator implements Iterator<Task> {
		private final Iterator<TaskSlot> taskSlotIterator;

		private Iterator<Task> currentTasks;

		private TaskIterator(JobID jobId) {
			this.taskSlotIterator = new TaskSlotIterator(jobId, TaskSlotState.ACTIVE);

			this.currentTasks = null;
		}

		@Override
		public boolean hasNext() {
			while ((currentTasks == null || !currentTasks.hasNext()) && taskSlotIterator.hasNext()) {
				TaskSlot taskSlot = taskSlotIterator.next();

				currentTasks = taskSlot.getTasks();
			}

			return (currentTasks != null && currentTasks.hasNext());
		}

		@Override
		public Task next() {
			while ((currentTasks == null || !currentTasks.hasNext())) {
				TaskSlot taskSlot;

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
