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

package org.apache.flink.runtime.instance;

import org.apache.flink.runtime.AbstractID;
import org.apache.flink.runtime.jobgraph.JobID;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Base class for slots.
 */
public abstract class Slot {
	protected static final AtomicIntegerFieldUpdater<Slot> STATUS_UPDATER =
			AtomicIntegerFieldUpdater.newUpdater(Slot.class, "status");

	protected static final int ALLOCATED_AND_ALIVE = 0;		// tasks may be added and might be running
	protected static final int CANCELLED = 1;					// no more tasks may run
	protected static final int RELEASED = 2;					// has been given back to the instance

	/** The ID of the job this slice belongs to. */
	protected final JobID jobID;

	/** The instance on which the slot is allocated */
	protected final Instance instance;

	/** The number of the slot on which the task is deployed */
	protected final int slotNumber;

	/** The state of the vertex, only atomically updated */
	protected volatile int status = ALLOCATED_AND_ALIVE;

	/** Indicates whether this slot was marked dead by the system */
	private boolean dead = false;

	private final AbstractID groupID;

	private final SharedSlot parent;

	private boolean disposed = false;


	public Slot(JobID jobID, Instance instance, int slotNumber, SharedSlot parent, AbstractID groupID) {
		if (jobID == null || instance == null || slotNumber < 0) {
			throw new IllegalArgumentException();
		}

		this.jobID = jobID;
		this.instance = instance;
		this.slotNumber = slotNumber;
		this.parent = parent;
		this.groupID = groupID;

	}
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the ID of the job this allocated slot belongs to.
	 *
	 * @return the ID of the job this allocated slot belongs to
	 */
	public JobID getJobID() {
		return this.jobID;
	}

	public Instance getInstance() {
		return instance;
	}

	public int getSlotNumber() {
		return slotNumber;
	}

	public AbstractID getGroupID() {
		return groupID;
	}

	public SharedSlot getParent() {
		return parent;
	}

	public Slot getRoot() {
		if(parent == null){
			return this;
		} else {
			return parent.getRoot();
		}
	}

	public abstract int getNumberLeaves();

	// --------------------------------------------------------------------------------------------
	//  Status and life cycle
	// --------------------------------------------------------------------------------------------

	public boolean isAlive() {
		return status == ALLOCATED_AND_ALIVE;
	}

	public boolean isCanceled() {
		return status != ALLOCATED_AND_ALIVE;
	}

	public boolean isReleased() {
		return status == RELEASED;
	}

	public abstract void cancel();

	public abstract void releaseSlot();

	public boolean markReleased() {
		return STATUS_UPDATER.compareAndSet(this, CANCELLED, RELEASED);
	}

	public boolean markCancelled() {
		return STATUS_UPDATER.compareAndSet(this, ALLOCATED_AND_ALIVE, CANCELLED);
	}

	/**
	 * Marks this shared slot to be dead. Returns if the slot was alive before. Should only
	 * be called through the {@link org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroupAssignment} attribute assignmentGroup.
	 *
	 * @return if the slot was alive before
	 */
	public boolean markDead() {
		boolean result = !dead;

		dead = true;

		return result;
	}

	public boolean isDead() {
		return dead;
	}

	public boolean markDisposed() {
		boolean result = !disposed;

		disposed = true;

		return result;
	}

	public boolean isDisposed() {
		return disposed;
	}

	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return hierarchy() + " - " + instance.getId() + " - " + getStateName(status);
	}

	protected String hierarchy() {
		return "(" + slotNumber + ")" + (getParent() != null ? getParent().hierarchy() : "");
	}

	private static final String getStateName(int state) {
		switch (state) {
			case ALLOCATED_AND_ALIVE:
				return "ALLOCATED/ALIVE";
			case CANCELLED:
				return "CANCELLED";
			case RELEASED:
				return "RELEASED";
			default:
				return "(unknown)";
		}
	}
}
