/**
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

package org.apache.flink.runtime.jobmanager.scheduler;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceDiedException;
import org.apache.flink.runtime.instance.InstanceListener;

/**
 * The scheduler is responsible for distributing the ready-to-run tasks and assigning them to instances and
 * slots.
 */
public class DefaultScheduler implements InstanceListener, SlotAvailablilityListener {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultScheduler.class);
	
	
	private final Object globalLock = new Object();
	
	
	/** All instances that the scheduler can deploy to */
	private final Set<Instance> allInstances = new HashSet<Instance>();
	
	/** All instances that still have available resources */
	private final Queue<Instance> instancesWithAvailableResources = new SetQueue<Instance>();
	
	/** All tasks pending to be scheduled */
	private final Queue<QueuedTask> taskQueue = new ArrayDeque<QueuedTask>();
	
	
	private int unconstrainedAssignments = 0;
	
	private int localizedAssignments = 0;
	
	private int nonLocalizedAssignments = 0;
	
	
	public DefaultScheduler() {
	}
	
	/**
	 * Shuts the scheduler down. After shut down no more tasks can be added to the scheduler.
	 */
	public void shutdown() {
		synchronized (globalLock) {
			for (Instance i : allInstances) {
				i.removeSlotListener();
				i.cancelAndReleaseAllSlots();
			}
			allInstances.clear();
			instancesWithAvailableResources.clear();
			taskQueue.clear();
		}
	}

	/**
	 * 
	 * NOTE: In the presence of multi-threaded operations, this number may be inexact.
	 * 
	 * @return The number of empty slots, for tasks.
	 */
	public int getNumberOfAvailableSlots() {
		int count = 0;
		
		synchronized (globalLock) {
			for (Instance instance : instancesWithAvailableResources) {
				count += instance.getNumberOfAvailableSlots();
			}
		}
		
		return count;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Scheduling
	// --------------------------------------------------------------------------------------------
	
	public AllocatedSlot scheduleImmediately(ScheduledUnit task) throws NoResourceAvailableException {
		Object ret = scheduleTask(task, false);
		if (ret instanceof AllocatedSlot) {
			return (AllocatedSlot) ret;
		}
		else {
			throw new RuntimeException();
		}
	}
	
	public SlotAllocationFuture scheduleQueued(ScheduledUnit task) throws NoResourceAvailableException {
		Object ret = scheduleTask(task, true);
		if (ret instanceof AllocatedSlot) {
			return new SlotAllocationFuture((AllocatedSlot) ret);
		}
		if (ret instanceof SlotAllocationFuture) {
			return (SlotAllocationFuture) ret;
		}
		else {
			throw new RuntimeException();
		}
	}
	
	/**
	 * Returns either an {@link AllocatedSlot}, or an {@link SlotAllocationFuture}.
	 */
	private Object scheduleTask(ScheduledUnit task, boolean queueIfNoResource) throws NoResourceAvailableException {
		if (task == null) {
			throw new IllegalArgumentException();
		}
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Scheduling task " + task);
		}
		
		final ExecutionVertex vertex = task.getTaskToExecute().getVertex();
	
		synchronized (globalLock) {
			// 1) If the task has a strict co-schedule hint, obey it, if it has been assigned.
//			CoLocationHint hint = task.getCoScheduleHint();
//			if (hint != null) {
//				
//				// try to add to the slot, or make it wait on the hint and schedule the hint itself
//				if () {
//					return slot;
//				}
//			}
		
			// 2) See if we can place the task somewhere together with another existing task.
			//    This is defined by the slot sharing groups
			SlotSharingGroup sharingUnit = task.getSlotSharingGroup();
			if (sharingUnit != null) {
				// see if we can add the task to the current sharing group.
				SlotSharingGroupAssignment assignment = sharingUnit.getTaskAssignment();
				AllocatedSlot slot = assignment.getSlotForTask(vertex.getJobvertexId(), vertex);
				if (slot != null) {
					return slot;
				}
			}
		
			// 3) We could not schedule it to an existing slot, so we need to get a new one or queue the task
			
			// we need potentially to loop multiple times, because there may be false positives
			// in the set-with-available-instances
			while (true) {
				
				
				Instance instanceToUse = getFreeInstanceForTask(task.getTaskToExecute().getVertex());
			
				if (instanceToUse != null) {
					try {
						AllocatedSlot slot = instanceToUse.allocateSlot(vertex.getJobId());
						
						// if the instance has further available slots, re-add it to the set of available resources.
						if (instanceToUse.hasResourcesAvailable()) {
							this.instancesWithAvailableResources.add(instanceToUse);
						}
						
						if (slot != null) {
							
							// if the task is in a shared group, assign the slot to that group
							// and get a sub slot in turn
							if (sharingUnit != null) {
								slot = sharingUnit.getTaskAssignment().addSlotWithTask(slot, task.getJobVertexId());
							}
							
							return slot;
						}
					}
					catch (InstanceDiedException e) {
						// the instance died it has not yet been propagated to this scheduler
						// remove the instance from the set of available instances
						this.allInstances.remove(instanceToUse);
						this.instancesWithAvailableResources.remove(instanceToUse);
					}
				}
				else {
					// no resource available now, so queue the request
					if (queueIfNoResource) {
						SlotAllocationFuture future = new SlotAllocationFuture();
						this.taskQueue.add(new QueuedTask(task, future));
						return future;
					}
					else {
						throw new NoResourceAvailableException(task);
					}
				}
			}
		}
	}
		
	/**
	 * Gets a suitable instance to schedule the vertex execution to.
	 * <p>
	 * NOTE: This method does is not thread-safe, it needs to be synchronized by the caller.
	 * 
	 * @param vertex The task to run. 
	 * @return The instance to run the vertex on, it {@code null}, if no instance is available.
	 */
	protected Instance getFreeInstanceForTask(ExecutionVertex vertex) {
		if (this.instancesWithAvailableResources.isEmpty()) {
			return null;
		}
		
		Iterable<Instance> locationsIterable = vertex.getPreferredLocations();
		Iterator<Instance> locations = locationsIterable == null ? null : locationsIterable.iterator();
		
		if (locations != null && locations.hasNext()) {
			
			while (locations.hasNext()) {
				Instance location = locations.next();
				
				if (location != null && this.instancesWithAvailableResources.remove(location)) {
					
					localizedAssignments++;
					if (LOG.isDebugEnabled()) {
						LOG.debug("Local assignment: " + vertex.getSimpleName() + " --> " + location);
					}
					
					return location;
				}
			}
			
			Instance instance = this.instancesWithAvailableResources.poll();
			nonLocalizedAssignments++;
			if (LOG.isDebugEnabled()) {
				LOG.debug("Non-local assignment: " + vertex.getSimpleName() + " --> " + instance);
			}
			return instance;
		}
		else {
			Instance instance = this.instancesWithAvailableResources.poll();
			unconstrainedAssignments++;
			if (LOG.isDebugEnabled()) {
				LOG.debug("Unconstrained assignment: " + vertex.getSimpleName() + " --> " + instance);
			}
			
			return instance;
		}
	}
	
	@Override
	public void newSlotAvailable(Instance instance) {
		
		// global lock before instance lock, so that the order of acquiring locks is always 1) global, 2) instance
		synchronized (globalLock) {
			QueuedTask queued = taskQueue.peek();
			
			// the slot was properly released, we can allocate a new one from that instance
			
			if (queued != null) {
				ScheduledUnit task = queued.getTask();
				ExecutionVertex vertex = task.getTaskToExecute().getVertex();
				
				try {
					AllocatedSlot newSlot = instance.allocateSlot(vertex.getJobId());
					if (newSlot != null) {
						
						// success, remove from the task queue and notify the future
						taskQueue.poll();
						if (queued.getFuture() != null) {
							try {
								queued.getFuture().setSlot(newSlot);
							}
							catch (Throwable t) {
								LOG.error("Error calling allocation future for task " + vertex.getSimpleName(), t);
								task.getTaskToExecute().fail(t);
							}
						}
					}
				}
				catch (InstanceDiedException e) {
					this.allInstances.remove(instance);
					if (LOG.isDebugEnabled()) {
						LOG.debug("Instance " + instance + " was marked dead asynchronously.");
					}
				}
			}
			else {
				this.instancesWithAvailableResources.add(instance);
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Instance Availability
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void newInstanceAvailable(Instance instance) {
		if (instance == null) {
			throw new IllegalArgumentException();
		}
		if (instance.getNumberOfAvailableSlots() <= 0) {
			throw new IllegalArgumentException("The given instance has no resources.");
		}
		if (!instance.isAlive()) {
			throw new IllegalArgumentException("The instance is not alive.");
		}
		
		// synchronize globally for instance changes
		synchronized (this.globalLock) {
			
			// check we do not already use this instance
			if (!this.allInstances.add(instance)) {
				throw new IllegalArgumentException("The instance is already contained.");
			}
			
			try {
				instance.setSlotAvailabilityListener(this);
			}
			catch (IllegalStateException e) {
				this.allInstances.remove(instance);
				LOG.error("Scheduler could not attach to the instance as a listener.");
			}
			
			
			// add it to the available resources and let potential waiters know
			this.instancesWithAvailableResources.add(instance);
			
			for (int i = 0; i < instance.getNumberOfAvailableSlots(); i++) {
				newSlotAvailable(instance);
			}
		}
	}
	
	@Override
	public void instanceDied(Instance instance) {
		if (instance == null) {
			throw new IllegalArgumentException();
		}
		
		instance.markDead();
		
		// we only remove the instance from the pools, we do not care about the 
		synchronized (this.globalLock) {
			// the instance must not be available anywhere any more
			this.allInstances.remove(instance);
			this.instancesWithAvailableResources.remove(instance);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Status reporting
	// --------------------------------------------------------------------------------------------

	public int getNumberOfAvailableInstances() {
		return allInstances.size();
	}
	
	public int getNumberOfInstancesWithAvailableSlots() {
		return instancesWithAvailableResources.size();
	}
	
	public int getNumberOfUnconstrainedAssignments() {
		return unconstrainedAssignments;
	}
	
	public int getNumberOfLocalizedAssignments() {
		return localizedAssignments;
	}
	
	public int getNumberOfNonLocalizedAssignments() {
		return nonLocalizedAssignments;
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final class QueuedTask {
		
		private final ScheduledUnit task;
		
		private final SlotAllocationFuture future;
		
		
		public QueuedTask(ScheduledUnit task, SlotAllocationFuture future) {
			this.task = task;
			this.future = future;
		}

		public ScheduledUnit getTask() {
			return task;
		}

		public SlotAllocationFuture getFuture() {
			return future;
		}
	}
}
