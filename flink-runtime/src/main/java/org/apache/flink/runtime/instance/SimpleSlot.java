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

import org.apache.flink.util.AbstractID;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Class which represents a single slot on a machine or within a shared slot. If this slot is part
 * of a [[SharedSlot]], then its parent attribute is set to this instance. If not, then the parent
 * attribute is null.
 *
 * IMPORTANT: This class has no synchronization. Thus it has to be synchronized by the calling
 * object.
 */
public class SimpleSlot extends Slot {

	private static final AtomicReferenceFieldUpdater<SimpleSlot, Execution> VERTEX_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(SimpleSlot.class, Execution.class, "executedTask");

	/** Task being executed in the slot. Volatile to force a memory barrier and allow for correct double-checking */
	private volatile Execution executedTask;

	private Locality locality = Locality.UNCONSTRAINED;

	public SimpleSlot(JobID jobID, Instance instance, int slotNumber, SharedSlot parent, AbstractID groupID){
		super(jobID, instance, slotNumber, parent, groupID);
	}

	@Override
	public int getNumberLeaves() {
		return 1;
	}


	public Execution getExecution() {
		return executedTask;
	}

	public Locality getLocality() {
		return locality;
	}

	public void setLocality(Locality locality) {
		this.locality = locality;
	}

	public boolean setExecutedVertex(Execution executedVertex) {
		if (executedVertex == null) {
			throw new NullPointerException();
		}

		// check that we can actually run in this slot
		if (getStatus() != ALLOCATED_AND_ALIVE) {
			return false;
		}

		// atomically assign the vertex
		if (!VERTEX_UPDATER.compareAndSet(this, null, executedVertex)) {
			return false;
		}

		// we need to do a double check that we were not cancelled in the meantime
		if (getStatus() != ALLOCATED_AND_ALIVE) {
			this.executedTask = null;
			return false;
		}

		return true;
	}

	@Override
	public void cancel() {
		if (markCancelled()) {
			// kill all tasks currently running in this slot
			Execution exec = this.executedTask;
			if (exec != null && !exec.isFinished()) {
				exec.fail(new Exception("The slot in which the task was scheduled has been killed (probably loss of TaskManager). Instance:"+getInstance()));
			}
		}
	}

	@Override
	public void releaseSlot() {
		// cancel everything, if there is something. since this is atomically status based,
		// it will not happen twice if another attempt happened before or concurrently
		try {
			cancel();
		} finally {
			if (getParent() != null) {
				// we have to ask our parent to dispose us
				getParent().disposeChild(this);
			} else {
				// we have to give back the slot to the owning instance
				getInstance().returnAllocatedSlot(this);
			}
		}
	}

	@Override
	public String toString() {
		return "SimpleSlot " + super.toString();
	}
}
