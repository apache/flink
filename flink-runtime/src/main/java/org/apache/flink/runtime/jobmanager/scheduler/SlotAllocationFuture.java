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

import org.apache.flink.runtime.instance.SimpleSlot;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * 
 */
public class SlotAllocationFuture {

	private final Object monitor = new Object();

	private volatile SimpleSlot slot;

	private volatile SlotAllocationFutureAction action;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a future that is uncompleted.
	 */
	public SlotAllocationFuture() {}

	/**
	 * Creates a future that is immediately completed.
	 * 
	 * @param slot The task slot that completes the future.
	 */
	public SlotAllocationFuture(SimpleSlot slot) {
		this.slot = slot;
	}

	// --------------------------------------------------------------------------------------------

	public SimpleSlot waitTillCompleted() throws InterruptedException {
		synchronized (monitor) {
			while (slot == null) {
				monitor.wait();
			}
			return slot;
		}
	}

	public SimpleSlot waitTillCompleted(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException {
		checkArgument(timeout >= 0, "timeout may not be negative");
		checkNotNull(timeUnit, "timeUnit");

		if (timeout == 0) {
			return waitTillCompleted();
		} else {
			final long deadline = System.nanoTime() + timeUnit.toNanos(timeout);
			long millisToWait;

			synchronized (monitor) {
				while (slot == null && (millisToWait = (deadline - System.nanoTime()) / 1_000_000) > 0) {
					monitor.wait(millisToWait);
				}

				if (slot != null) {
					return slot;
				} else {
					throw new TimeoutException();
				}
			}
		}
	}

	/**
	 * Gets the slot from this future. This method throws an exception, if the future has not been completed.
	 * This method never blocks.
	 * 
	 * @return The slot with which this future was completed.
	 * @throws IllegalStateException Thrown, if this method is called before the future is completed.
	 */
	public SimpleSlot get() {
		final SimpleSlot slot = this.slot;
		if (slot != null) {
			return slot;
		} else {
			throw new IllegalStateException("The future is not complete - not slot available");
		}
	}

	public void setFutureAction(SlotAllocationFutureAction action) {
		checkNotNull(action);

		synchronized (monitor) {
			checkState(this.action == null, "Future already has an action registered.");

			this.action = action;

			if (this.slot != null) {
				action.slotAllocated(this.slot);
			}
		}
	}

	/**
	 * Completes the future with a slot.
	 */
	public void setSlot(SimpleSlot slot) {
		checkNotNull(slot);

		synchronized (monitor) {
			checkState(this.slot == null, "The future has already been assigned a slot.");

			this.slot = slot;
			monitor.notifyAll();

			if (action != null) {
				action.slotAllocated(slot);
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return slot == null ? "PENDING" : "DONE";
	}
}
