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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceDiedException;
import org.apache.flink.runtime.instance.InstanceListener;
import org.apache.flink.runtime.jobgraph.JobID;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * The scheduler is responsible for distributing the ready-to-run tasks and assigning them to instances and
 * slots.
 * <p>
 * The scheduler's bookkeeping on the available instances is lazy: It is not modified once an
 * instance is dead, but it will lazily remove the instance from its pool as soon as it tries
 * to allocate a resource on that instance and it fails with an {@link InstanceDiedException}.
 */
public class DefaultScheduler implements InstanceListener {

	protected static final Logger LOG = LoggerFactory.getLogger(DefaultScheduler.class);

	
	private final Object lock = new Object();
	
	/** All instances that the scheduler can deploy to */
	private final Set<Instance> allInstances = new HashSet<Instance>();
	
	/** All instances that still have available resources */
	private final Queue<Instance> instancesWithAvailableResources = new LifoSetQueue<Instance>();

	
	private final ConcurrentHashMap<ResourceId, AllocatedSlot> allocatedSlots = new ConcurrentHashMap<ResourceId, AllocatedSlot>();
	
//	/** A cache that remembers the last resource IDs it has seen, to co-locate future
//	 *  deployments of tasks with the same resource ID to the same instance.
//	 */
//	private final Cache<ResourceId, Instance> ghostCache;
	
	/** All tasks pending to be scheduled */
	private final Queue<ScheduledUnit> taskQueue = new ArrayDeque<ScheduledUnit>();

	
	/** The thread that runs the scheduling loop, picking up tasks to be scheduled and scheduling them. */
	private final Thread schedulerThread;
	
	
	/** Atomic flag to safely control the shutdown */
	private final AtomicBoolean shutdown = new AtomicBoolean(false);
	
	/** Flag indicating whether the scheduler should reject a unit if it cannot find a resource
	 * for it at the time of scheduling */
	private final boolean rejectIfNoResourceAvailable;
	

	
	public DefaultScheduler() {
		this(true);
	}
	
	public DefaultScheduler(boolean rejectIfNoResourceAvailable) {
		this.rejectIfNoResourceAvailable = rejectIfNoResourceAvailable;
		
//		this.ghostCache = CacheBuilder.newBuilder()
//				.initialCapacity(64)	// easy start
//				.maximumSize(1024)		// retain some history
//				.weakValues()			// do not prevent dead instances from being collected
//				.build();
		
		// set up (but do not start) the scheduling thread
		Runnable loopRunner = new Runnable() {
			@Override
			public void run() {
				runSchedulerLoop();
			}
		};
		this.schedulerThread = new Thread(loopRunner, "Scheduling Thread");
	}
	
	public void start() {
		if (shutdown.get()) {
			throw new IllegalStateException("Scheduler has been shut down.");
		}
		
		try {
			this.schedulerThread.start();
		}
		catch (IllegalThreadStateException e) {
			throw new IllegalStateException("The scheduler has already been started.");
		}
	}
	
	/**
	 * Shuts the scheduler down. After shut down no more tasks can be added to the scheduler.
	 */
	public void shutdown() {
		if (this.shutdown.compareAndSet(false, true)) {
			// clear the task queue and add the termination signal, to let
			// the scheduling loop know that things are done
			this.taskQueue.clear();
			this.taskQueue.add(TERMINATION_SIGNAL);
			
			// interrupt the scheduling thread, in case it was waiting for resources to
			// show up to deploy a task
			this.schedulerThread.interrupt();
		}
	}
	
	public void setUncaughtExceptionHandler(UncaughtExceptionHandler handler) {
		if (this.schedulerThread.getState() != Thread.State.NEW) {
			throw new IllegalStateException("Can only add exception handler before starting the scheduler.");
		}
		this.schedulerThread.setUncaughtExceptionHandler(handler);
	}

	
	// --------------------------------------------------------------------------------------------
	//  Scheduling
	// --------------------------------------------------------------------------------------------
	
	/**
	 * @param task
	 * @param queueIfNoResource If true, this call will queue the request if no resource is immediately
	 *                          available. If false, it will throw a {@link NoResourceAvailableException}
	 *                          if no resource is immediately available.
	 */
	public void scheduleTask(ScheduledUnit task, boolean queueIfNoResource) {
		if (task == null) {
			throw new IllegalArgumentException();
		}
		
		// if there is already a slot for that resource
		AllocatedSlot existing = this.allocatedSlots.get(task.getSharedResourceId());
		if (existing != null) {
			// try to attach to the existing slot
			if (existing.runTask(task.getTaskVertex())) {
				// all good, we are done
				return;
			}
			// else: the slot was deallocated, we need to proceed as if there was none
		}
		
		// check if there is a slot that has an available sub-slot for that group-vertex
		// TODO
		
		
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
		synchronized (this.lock) {
			// check we do not already use this instance
			if (!this.allInstances.add(instance)) {
				throw new IllegalArgumentException("The instance is already contained.");
			}
			
			// add it to the available resources and let potential waiters know
			this.instancesWithAvailableResources.add(instance);
			this.lock.notifyAll();
		}
	}
	
	@Override
	public void instanceDied(Instance instance) {
		if (instance == null) {
			throw new IllegalArgumentException();
		}
		
		instance.markDead();
		
		// we only remove the instance from the pools, we do not care about the 
		synchronized (this.lock) {
			// the instance must not be available anywhere any more
			this.allInstances.remove(instance);
			this.instancesWithAvailableResources.remove(instance);
		}
	}
	
	public int getNumberOfAvailableInstances() {
		synchronized (lock) {
			return allInstances.size();
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Scheduling
	// --------------------------------------------------------------------------------------------
	
//	/**
//	 * Schedules the given unit to an available resource. This call blocks if no resource
//	 * is currently available
//	 * 
//	 * @param unit The unit to be scheduled.
//	 */
//	protected void scheduleQueuedUnit(ScheduledUnit unit) {
//		if (unit == null) {
//			throw new IllegalArgumentException("Unit to schedule must not be null.");
//		}
//		
//		// see if the resource Id has already an assigned resource
//		AllocatedSlot resource = this.allocatedSlots.get(unit.getSharedResourceId());
//		
//		if (resource == null) {
//			// not yet allocated. find a slot to schedule to
//			try {
//				resource = getResourceToScheduleUnit(unit, this.rejectIfNoResourceAvailable);
//				if (resource == null) {
//					throw new RuntimeException("Error: The resource to schedule to is null.");
//				}
//			}
//			catch (Exception e) {
//				// we cannot go on, the task needs to know what to do now.
//				unit.getTaskVertex().handleException(e);
//				return;
//			}
//		}
//		
//		resource.runTask(unit.getTaskVertex());
//	}
	
	/**
	 * Acquires a resource to schedule the given unit to. This call may block if no
	 * resource is currently available, or throw an exception, based on the given flag.
	 * 
	 * @param unit The unit to find a resource for.
	 * @return The resource to schedule the execution of the given unit on.
	 * 
	 * @throws NoResourceAvailableException If the {@code exceptionOnNoAvailability} flag is true and the scheduler
	 *                                      has currently no resources available.
	 */
	protected AllocatedSlot getNewSlotForTask(ScheduledUnit unit, boolean queueIfNoResource) 
		throws NoResourceAvailableException
	{
		synchronized (this.lock) {
			Instance instanceToUse = this.instancesWithAvailableResources.poll();
			
			// if there is nothing, throw an exception or wait, depending on what is configured
			if (instanceToUse != null) {
				try {
					AllocatedSlot slot = instanceToUse.allocateSlot(unit.getJobId(), unit.getSharedResourceId());
					
					// if the instance has further available slots, re-add it to the set of available resources.
					if (instanceToUse.hasResourcesAvailable()) {
						this.instancesWithAvailableResources.add(instanceToUse);
					}
					
					if (slot != null) {
						AllocatedSlot previous = this.allocatedSlots.putIfAbsent(unit.getSharedResourceId(), slot);
						if (previous != null) {
							// concurrently, someone allocated a slot for that ID
							// release the new one
							slot.cancelResource();
							slot = previous;
						}
					}
					// else fall through the loop
				}
				catch (InstanceDiedException e) {
					// the instance died it has not yet been propagated to this scheduler
					// remove the instance from the set of available instances
					this.allInstances.remove(instanceToUse);
				}
			}
				
			
			if (queueIfNoResource) {
				this.taskQueue.add(unit);
			}
			else {
				throw new NoResourceAvailableException(unit);
			}
				// at this point, we have an instance. request a slot from the instance
				
				
				// if the instance has further available slots, re-add it to the set of available
				// resources.
				// if it does not, but asynchronously a slot became available, we may attempt to add the
				// instance twice, which does not matter because of the set semantics of the "instancesWithAvailableResources"
				if (instanceToUse.hasResourcesAvailable()) {
					this.instancesWithAvailableResources.add(instanceToUse);
				}
				
				if (slot != null) {
					AllocatedSlot previous = this.allocatedSlots.putIfAbsent(unit.getSharedResourceId(), slot);
					if (previous != null) {
						// concurrently, someone allocated a slot for that ID
						// release the new one
						slot.cancelResource();
						slot = previous;
					}
				}
				// else fall through the loop
			}
		}
		
		return slot;
	}
	
	protected void runSchedulerLoop() {
		// while the scheduler is alive
		while (!shutdown.get()) {
			
			// get the next unit
			ScheduledUnit next = null;
			try {
				next = this.taskQueue.take();
			}
			catch (InterruptedException e) {
				if (shutdown.get()) {
					return;
				} else {
					LOG.error("Scheduling loop was interrupted.");
				}
			}
			
			// if we see this special unit, it means we are done
			if (next == TERMINATION_SIGNAL) {
				return;
			}
			
			// deploy the next scheduling unit
			try {
				scheduleNextUnit(next);
			}
			catch (Throwable t) {
				// ignore the errors in the presence of a shutdown
				if (!shutdown.get()) {
					if (t instanceof Error) {
						throw (Error) t;
					} else if (t instanceof RuntimeException) {
						throw (RuntimeException) t;
					} else {
						throw new RuntimeException("Critical error in scheduler thread.", t);
					}
				}
			}
		}
	}
	
	private static final ScheduledUnit TERMINATION_SIGNAL = new ScheduledUnit();
}
