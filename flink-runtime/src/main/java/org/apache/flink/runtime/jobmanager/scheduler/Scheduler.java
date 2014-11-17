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

package org.apache.flink.runtime.jobmanager.scheduler;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import akka.dispatch.Futures;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceDiedException;
import org.apache.flink.runtime.instance.InstanceListener;
import org.apache.flink.util.ExceptionUtils;

/**
 * The scheduler is responsible for distributing the ready-to-run tasks and assigning them to instances and
 * slots.
 */
public class Scheduler implements InstanceListener, SlotAvailabilityListener {

	static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
	
	
	private final Object globalLock = new Object();
	
	/** All instances that the scheduler can deploy to */
	private final Set<Instance> allInstances = new HashSet<Instance>();
	
	/** All instances that still have available resources */
	private final Queue<Instance> instancesWithAvailableResources = new SetQueue<Instance>();
	
	/** All tasks pending to be scheduled */
	private final Queue<QueuedTask> taskQueue = new ArrayDeque<QueuedTask>();
	
	private final BlockingQueue<Instance> newlyAvailableInstances;
	
	
	private int unconstrainedAssignments;
	
	private int localizedAssignments;
	
	private int nonLocalizedAssignments;
	
	public Scheduler() {
		this.newlyAvailableInstances = new LinkedBlockingQueue<Instance>();
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
	
	public int getTotalNumberOfSlots() {
		int count = 0;
		
		synchronized (globalLock) {
			for (Instance instance : allInstances) {
				if (instance.isAlive()) {
					count += instance.getTotalNumberOfSlots();
				}
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
		
			// 1)  === If the task has a slot sharing group, schedule with shared slots ===
			
			SlotSharingGroup sharingUnit = task.getSlotSharingGroup();
			if (sharingUnit != null) {
				
				if (queueIfNoResource) {
					throw new IllegalArgumentException("A task with a vertex sharing group was scheduled in a queued fashion.");
				}
				
				final SlotSharingGroupAssignment assignment = sharingUnit.getTaskAssignment();
				final CoLocationConstraint constraint = task.getLocationConstraint();
				
				// get a slot from the group, if the group has one for us (and can fulfill the constraint)
				SubSlot slotFromGroup;
				if (constraint == null) {
					slotFromGroup = assignment.getSlotForTask(vertex);
				}
				else {
					slotFromGroup = assignment.getSlotForTask(vertex, constraint);
				}
				
				AllocatedSlot newSlot = null;
				
				// the following needs to make sure any allocated slot is released in case of an error
				try {
					
					// check whether the slot from the group is already what we want
					if (slotFromGroup != null) {
						// local (or unconstrained in the current group)
						if (slotFromGroup.getLocality() != Locality.NON_LOCAL) {
							updateLocalityCounters(slotFromGroup.getLocality());
							return slotFromGroup;
						}
					}
					
					final Iterable<Instance> locations = (constraint == null || constraint.isUnassigned()) ?
							vertex.getPreferredLocations() : Collections.singleton(constraint.getLocation());
					
					// get a new slot, since we could not place it into the group, or we could not place it locally
					newSlot = getFreeSlotForTask(vertex, locations);
					
					SubSlot toUse;
					
					if (newSlot == null) {
						if (slotFromGroup == null) {
							// both null
							if (constraint == null || constraint.isUnassigned()) {
								throw new NoResourceAvailableException(task, getNumberOfAvailableInstances(), getTotalNumberOfSlots());
							} else {
								throw new NoResourceAvailableException("Could not allocate a slot on instance " + 
											constraint.getLocation() + ", as required by the co-location constraint.");
							}
						} else {
							// got a non-local from the group, and no new one
							toUse = slotFromGroup;
						}
					}
					else if (slotFromGroup == null || newSlot.getLocality() == Locality.LOCAL) {
						// new slot is preferable
						if (slotFromGroup != null) {
							slotFromGroup.releaseSlot();
						}
						
						if (constraint == null) {
							toUse = assignment.addNewSlotWithTask(newSlot, vertex);
						} else {
							toUse = assignment.addNewSlotWithTask(newSlot, vertex, constraint);
						}
					}
					else {
						// both are available and usable. neither is local
						newSlot.releaseSlot();
						toUse = slotFromGroup;
					}
					
					// assign to the co-location hint, if we have one and it is unassigned
					// if it was assigned before and the new one is not local, it is a fail
					if (constraint != null) {
						if (constraint.isUnassigned() || toUse.getLocality() == Locality.LOCAL) {
							constraint.setSharedSlot(toUse.getSharedSlot());
						} else {
							// the fail
							throw new NoResourceAvailableException("Could not allocate a slot on instance " + 
									constraint.getLocation() + ", as required by the co-location constraint.");
						}
					}
					
					updateLocalityCounters(toUse.getLocality());
					return toUse;
				}
				catch (NoResourceAvailableException e) {
					throw e;
				}
				catch (Throwable t) {
					if (slotFromGroup != null) {
						slotFromGroup.releaseSlot();
					}
					if (newSlot != null) {
						newSlot.releaseSlot();
					}
					
					ExceptionUtils.rethrow(t, "An error occurred while allocating a slot in a sharing group");
				}
			}
		
			// 2) === schedule without hints and sharing ===
			
			AllocatedSlot slot = getFreeSlotForTask(vertex, vertex.getPreferredLocations());
			if (slot != null) {
				updateLocalityCounters(slot.getLocality());
				return slot;
			}
			else {
				// no resource available now, so queue the request
				if (queueIfNoResource) {
					SlotAllocationFuture future = new SlotAllocationFuture();
					this.taskQueue.add(new QueuedTask(task, future));
					return future;
				}
				else {
					throw new NoResourceAvailableException(getNumberOfAvailableInstances(), getTotalNumberOfSlots());
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
	protected AllocatedSlot getFreeSlotForTask(ExecutionVertex vertex, Iterable<Instance> requestedLocations) {
		
		// we need potentially to loop multiple times, because there may be false positives
		// in the set-with-available-instances
		while (true) {
			if (this.instancesWithAvailableResources.isEmpty()) {
				// check if the asynchronous calls did not yet return the queues
				Instance queuedInstance = this.newlyAvailableInstances.poll();
				if (queuedInstance == null) {
					return null;
				} else {
					this.instancesWithAvailableResources.add(queuedInstance);
				}
			}
			
			Iterator<Instance> locations = requestedLocations == null ? null : requestedLocations.iterator();
			
			Instance instanceToUse = null;
			Locality locality = Locality.UNCONSTRAINED;
			
			if (locations != null && locations.hasNext()) {
				// we have a locality preference
				
				while (locations.hasNext()) {
					Instance location = locations.next();
					
					if (location != null && this.instancesWithAvailableResources.remove(location)) {
						instanceToUse = location;
						locality = Locality.LOCAL;
						
						if (LOG.isDebugEnabled()) {
							LOG.debug("Local assignment: " + vertex.getSimpleName() + " --> " + location);
						}
						
						break;
					}
				}
				
				if (instanceToUse == null) {
					instanceToUse = this.instancesWithAvailableResources.poll();
					locality = Locality.NON_LOCAL;
					if (LOG.isDebugEnabled()) {
						LOG.debug("Non-local assignment: " + vertex.getSimpleName() + " --> " + instanceToUse);
					}
				}
			}
			else {
				instanceToUse = this.instancesWithAvailableResources.poll();
				if (LOG.isDebugEnabled()) {
					LOG.debug("Unconstrained assignment: " + vertex.getSimpleName() + " --> " + instanceToUse);
				}
			}
			
			try {
				AllocatedSlot slot = instanceToUse.allocateSlot(vertex.getJobId());
				
				// if the instance has further available slots, re-add it to the set of available resources.
				if (instanceToUse.hasResourcesAvailable()) {
					this.instancesWithAvailableResources.add(instanceToUse);
				}
				
				if (slot != null) {
					slot.setLocality(locality);
					return slot;
				}
			}
			catch (InstanceDiedException e) {
				// the instance died it has not yet been propagated to this scheduler
				// remove the instance from the set of available instances
				this.allInstances.remove(instanceToUse);
				this.instancesWithAvailableResources.remove(instanceToUse);
			}
			
			// if we failed to get a slot, fall through the loop
		}
	}
	
	@Override
	public void newSlotAvailable(final Instance instance) {
		
		// WARNING: The asynchrony here is necessary, because  we cannot guarantee the order
		// of lock acquisition (global scheduler, instance) and otherwise lead to potential deadlocks:
		// 
		// -> The scheduler needs to grab them (1) global scheduler lock
		//                                     (2) slot/instance lock
		// -> The slot releasing grabs (1) slot/instance (for releasing) and
		//                             (2) scheduler (to check whether to take a new task item
		// 
		// that leads with a high probability to deadlocks, when scheduling fast
		
		this.newlyAvailableInstances.add(instance);

		Futures.future(new Callable<Object>() {
			@Override
			public Object call() throws Exception {
				handleNewSlot();
				return null;
			}
		}, AkkaUtils.globalExecutionContext());
	}
	
	private void handleNewSlot() {
		
		synchronized (globalLock) {
			Instance instance = this.newlyAvailableInstances.poll();
			if (instance == null || !instance.hasResourcesAvailable()) {
				// someone else took it
				return;
			}
			
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
	
	private void updateLocalityCounters(Locality locality) {
		switch (locality) {
		case UNCONSTRAINED:
			this.unconstrainedAssignments++;
			break;
		case LOCAL:
			this.localizedAssignments++;
			break;
		case NON_LOCAL:
			this.nonLocalizedAssignments++;
			break;
		default:
			throw new RuntimeException(locality.name());
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
