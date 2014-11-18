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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import akka.dispatch.Futures;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.util.AbstractID;
import org.apache.flink.runtime.instance.SharedSlot;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
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
	
	/** All instances by hostname */
	private final HashMap<String, Set<Instance>> allInstancesByHost = new HashMap<String, Set<Instance>>();
	
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
			allInstancesByHost.clear();
			instancesWithAvailableResources.clear();
			taskQueue.clear();
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Scheduling
	// --------------------------------------------------------------------------------------------
	
	public SimpleSlot scheduleImmediately(ScheduledUnit task) throws NoResourceAvailableException {
		Object ret = scheduleTask(task, false);
		if (ret instanceof SimpleSlot) {
			return (SimpleSlot) ret;
		}
		else {
			throw new RuntimeException();
		}
	}
	
	public SlotAllocationFuture scheduleQueued(ScheduledUnit task) throws NoResourceAvailableException {
		Object ret = scheduleTask(task, true);
		if (ret instanceof SimpleSlot) {
			return new SlotAllocationFuture((SimpleSlot) ret);
		}
		if (ret instanceof SlotAllocationFuture) {
			return (SlotAllocationFuture) ret;
		}
		else {
			throw new RuntimeException();
		}
	}
	
	/**
	 * Returns either an {@link org.apache.flink.runtime.instance.SimpleSlot}, or an {@link SlotAllocationFuture}.
	 */
	private Object scheduleTask(ScheduledUnit task, boolean queueIfNoResource) throws NoResourceAvailableException {
		if (task == null) {
			throw new IllegalArgumentException();
		}
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Scheduling task " + task);
		}

		final ExecutionVertex vertex = task.getTaskToExecute().getVertex();
		
		final Iterable<Instance> preferredLocations = vertex.getPreferredLocations();
		final boolean forceExternalLocation = vertex.isScheduleLocalOnly() &&
									preferredLocations != null && preferredLocations.iterator().hasNext();
	
		synchronized (globalLock) {
		
			// 1)  === If the task has a slot sharing group, schedule with shared slots ===
			
			SlotSharingGroup sharingUnit = task.getSlotSharingGroup();
			if (sharingUnit != null) {
				
				if (queueIfNoResource) {
					throw new IllegalArgumentException("A task with a vertex sharing group was scheduled in a queued fashion.");
				}
				
				final SlotSharingGroupAssignment assignment = sharingUnit.getTaskAssignment();
				final CoLocationConstraint constraint = task.getLocationConstraint();
				
				// sanity check that we do not use an externally forced location and a co-location constraint together
				if (constraint != null && forceExternalLocation) {
					throw new IllegalArgumentException("The scheduling cannot be contrained simultaneously by a "
							+ "co-location constriaint and an external location constraint.");
				}
				
				// get a slot from the group, if the group has one for us (and can fulfill the constraint)
				SimpleSlot slotFromGroup;
				if (constraint == null) {
					slotFromGroup = assignment.getSlotForTask(vertex);
				}
				else {
					slotFromGroup = assignment.getSlotForTask(vertex, constraint);
				}

				SimpleSlot newSlot = null;
				SimpleSlot toUse = null;

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
					newSlot = getFreeSubSlotForTask(vertex, locations, assignment, constraint, forceExternalLocation);

					if (newSlot == null) {
						if (slotFromGroup == null) {
							// both null
							if (constraint == null || constraint.isUnassigned()) {
								if (forceExternalLocation) {
									// could not satisfy the external location constraint
									String hosts = getHostnamesFromInstances(preferredLocations);
									throw new NoResourceAvailableException("Could not schedule task " + vertex
											+ " to any of the required hosts: " + hosts);
								}
								else {
									// simply nothing is available
									throw new NoResourceAvailableException(task, getNumberOfAvailableInstances(),
											getTotalNumberOfSlots(), getNumberOfAvailableSlots());
								}
							}
							else {
								// nothing is available on the node where the co-location constraint pushes us
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
						
						toUse = newSlot;
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
							constraint.setSharedSlot(toUse.getParent());
						} else {
							// the fail
							throw new NoResourceAvailableException("Could not allocate a slot on instance " + 
									constraint.getLocation() + ", as required by the co-location constraint.");
						}
					}
					
					updateLocalityCounters(toUse.getLocality());
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

				return toUse;
			} else {
				// 2) === schedule without hints and sharing ===
				SimpleSlot slot = getFreeSlotForTask(vertex, preferredLocations, forceExternalLocation);
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
					else if (forceExternalLocation) {
						String hosts = getHostnamesFromInstances(preferredLocations);
						throw new NoResourceAvailableException("Could not schedule task " + vertex
								+ " to any of the required hosts: " + hosts);
					}
					else {
						throw new NoResourceAvailableException(getNumberOfAvailableInstances(), getTotalNumberOfSlots(), getNumberOfAvailableSlots());
					}
				}
			}
		}
	}
	
	private String getHostnamesFromInstances(Iterable<Instance> instances) {
		StringBuilder bld = new StringBuilder();
		
		for (Instance i : instances) {
			bld.append(i.getInstanceConnectionInfo().getHostname());
			bld.append(", ");
		}
		
		if (bld.length() == 0) {
			return "";
		}
		else {
			bld.setLength(bld.length() - 2);
			return bld.toString();
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
	protected SimpleSlot getFreeSlotForTask(ExecutionVertex vertex, Iterable<Instance> requestedLocations, boolean localOnly) {
		
		// we need potentially to loop multiple times, because there may be false positives
		// in the set-with-available-instances
		while (true) {
			Pair<Instance, Locality> instanceLocalityPair = findInstance(requestedLocations, localOnly);

			if (instanceLocalityPair == null){
				return null;
			}

			Instance instanceToUse = instanceLocalityPair.getLeft();
			Locality locality = instanceLocalityPair.getRight();

			if (LOG.isDebugEnabled()){
				if(locality == Locality.LOCAL){
					LOG.debug("Local assignment: " + vertex.getSimpleName() + " --> " + instanceToUse);
				}else if(locality == Locality.NON_LOCAL){
					LOG.debug("Non-local assignment: " + vertex.getSimpleName() + " --> " + instanceToUse);
				}else if(locality == Locality.UNCONSTRAINED) {
					LOG.debug("Unconstrained assignment: " + vertex.getSimpleName() + " --> " + instanceToUse);
				}
			}

			try {
				SimpleSlot slot = instanceToUse.allocateSimpleSlot(vertex.getJobId(), vertex.getJobvertexId());
				
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
				removeInstance(instanceToUse);
			}
			
			// if we failed to get a slot, fall through the loop
		}
	}

	protected SimpleSlot getFreeSubSlotForTask(ExecutionVertex vertex,
											Iterable<Instance> requestedLocations,
											SlotSharingGroupAssignment groupAssignment,
											CoLocationConstraint constraint,
											boolean localOnly) {
		// we need potentially to loop multiple times, because there may be false positives
		// in the set-with-available-instances
		while (true) {
			Pair<Instance, Locality> instanceLocalityPair = findInstance(requestedLocations, localOnly);

			if (instanceLocalityPair == null) {
				return null;
			}

			Instance instanceToUse = instanceLocalityPair.getLeft();
			Locality locality = instanceLocalityPair.getRight();

			if (LOG.isDebugEnabled()) {
				if (locality == Locality.LOCAL) {
					LOG.debug("Local assignment: " + vertex.getSimpleName() + " --> " + instanceToUse);
				} else if(locality == Locality.NON_LOCAL) {
					LOG.debug("Non-local assignment: " + vertex.getSimpleName() + " --> " + instanceToUse);
				} else if(locality == Locality.UNCONSTRAINED) {
					LOG.debug("Unconstrained assignment: " + vertex.getSimpleName() + " --> " + instanceToUse);
				}
			}

			try {
				AbstractID groupID = constraint == null ? vertex.getJobvertexId() : constraint.getGroupId();

				// root SharedSlot
				SharedSlot sharedSlot = instanceToUse.allocateSharedSlot(vertex.getJobId(), groupAssignment, groupID);

				// if the instance has further available slots, re-add it to the set of available resources.
				if (instanceToUse.hasResourcesAvailable()) {
					this.instancesWithAvailableResources.add(instanceToUse);
				}

				if(sharedSlot != null){
					// If constraint != null, then slot nested in a SharedSlot nested in sharedSlot
					// If constraint == null, then slot nested in sharedSlot
					SimpleSlot slot = groupAssignment.addSharedSlotAndAllocateSubSlot(sharedSlot,
							locality, groupID, constraint);

					if(slot != null){
						return slot;
					} else {
						// release shared slot
						sharedSlot.releaseSlot();
					}
				}
			}
			catch (InstanceDiedException e) {
				// the instance died it has not yet been propagated to this scheduler
				// remove the instance from the set of available instances
				removeInstance(instanceToUse);
			}

			// if we failed to get a slot, fall through the loop
		}
	}

	/**
	 * NOTE: This method is not thread-safe, it needs to be synchronized by the caller.
	 *
	 * Tries to find a requested instance. If no such instance is available it will return a non-
	 * local instance. If no such instance exists (all slots occupied), then return null.
	 *
	 * @param requestedLocations
	 */
	private Pair<Instance, Locality> findInstance(Iterable<Instance> requestedLocations, boolean localOnly){
		
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
					break;
				}
			}

			if (instanceToUse == null) {
				if (localOnly) {
					return null;
				}
				else {
					instanceToUse = this.instancesWithAvailableResources.poll();
					locality = Locality.NON_LOCAL;
				}
			}
		}
		else {
			instanceToUse = this.instancesWithAvailableResources.poll();
		}

		return new ImmutablePair<Instance, Locality>(instanceToUse, locality);
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
					SimpleSlot newSlot = instance.allocateSimpleSlot(vertex.getJobId(), vertex.getJobvertexId());
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
					if (LOG.isDebugEnabled()) {
						LOG.debug("Instance " + instance + " was marked dead asynchronously.");
					}
					
					removeInstance(instance);
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
				// make sure we get notifications about slots becoming available
				instance.setSlotAvailabilityListener(this);
				
				// store the instance in the by-host-lookup
				String instanceHostName = instance.getInstanceConnectionInfo().getHostname();
				Set<Instance> instanceSet = allInstancesByHost.get(instanceHostName);
				if (instanceSet == null) {
					instanceSet = new HashSet<Instance>();
					allInstancesByHost.put(instanceHostName, instanceSet);
				}
				instanceSet.add(instance);
				
					
				// add it to the available resources and let potential waiters know
				this.instancesWithAvailableResources.add(instance);
	
				// add all slots as available
				for (int i = 0; i < instance.getNumberOfAvailableSlots(); i++) {
					newSlotAvailable(instance);
				}
			}
			catch (Throwable t) {
				LOG.error("Scheduler could not add new instance " + instance, t);
				removeInstance(instance);
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
			removeInstance(instance);
		}
	}
	
	private void removeInstance(Instance instance) {
		if (instance == null) {
			throw new NullPointerException();
		}
		
		allInstances.remove(instance);
		instancesWithAvailableResources.remove(instance);
		
		String instanceHostName = instance.getInstanceConnectionInfo().getHostname();
		Set<Instance> instanceSet = allInstancesByHost.get(instanceHostName);
		if (instanceSet != null) {
			instanceSet.remove(instance);
			if (instanceSet.isEmpty()) {
				allInstancesByHost.remove(instanceHostName);
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Status reporting
	// --------------------------------------------------------------------------------------------

	/**
	 *
	 * NOTE: In the presence of multi-threaded operations, this number may be inexact.
	 *
	 * @return The number of empty slots, for tasks.
	 */
	public int getNumberOfAvailableSlots() {
		int count = 0;

		synchronized (globalLock) {
			processNewlyAvailableInstances();

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

	public int getNumberOfAvailableInstances() {
		int numberAvailableInstances = 0;
		synchronized (this.globalLock) {
			for (Instance instance: allInstances ){
				if (instance.isAlive()){
					numberAvailableInstances++;
				}
			}
		}

		return numberAvailableInstances;
	}
	
	public int getNumberOfInstancesWithAvailableSlots() {
		synchronized (globalLock) {
			processNewlyAvailableInstances();

			return instancesWithAvailableResources.size();
		}
	}
	
	public Map<String, List<Instance>> getInstancesByHost() {
		synchronized (globalLock) {
			HashMap<String, List<Instance>> copy = new HashMap<String, List<Instance>>();
			
			for (Map.Entry<String, Set<Instance>> entry : allInstancesByHost.entrySet()) {
				copy.put(entry.getKey(), new ArrayList<Instance>(entry.getValue()));
			}
			return copy;
		}
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

	private void processNewlyAvailableInstances() {
		synchronized (globalLock) {
			Instance instance;

			while((instance = newlyAvailableInstances.poll()) != null){
				if(instance.hasResourcesAvailable()){
					instancesWithAvailableResources.add(instance);
				}
			}
		}
	}
	
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
