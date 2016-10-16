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

package org.apache.flink.runtime.jobmanager.slots;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.SlotPool;
import org.apache.flink.runtime.instance.SlotProvider;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A simple pool based slot provider with {@link SlotPool} as the underlying storage.
 */
public class PooledSlotProvider implements SlotProvider {

	/** The pool which holds all the slots. */
	private final SlotPool slotPool;

	/** The timeout for allocation. */
	private final Time timeout;

	public PooledSlotProvider(final SlotPool slotPool, final Time timeout) {
		this.slotPool = slotPool;
		this.timeout = timeout;
	}

	@Override
	public Future<SimpleSlot> allocateSlot(ScheduledUnit task,
			boolean allowQueued) throws NoResourceAvailableException
	{
		checkNotNull(task);

		final JobID jobID = task.getTaskToExecute().getVertex().getJobId();
		final Future<SimpleSlot> future = slotPool.allocateSimpleSlot(jobID, ResourceProfile.UNKNOWN);
		try {
			final SimpleSlot slot = future.get(timeout.getSize(), timeout.getUnit());
			return FlinkCompletableFuture.completed(slot);
		} catch (InterruptedException e) {
			throw new NoResourceAvailableException("Could not allocate a slot because it's interrupted.");
		} catch (ExecutionException e) {
			throw new NoResourceAvailableException("Could not allocate a slot because some error occurred " +
					"during allocation, " + e.getMessage());
		} catch (TimeoutException e) {
			throw new NoResourceAvailableException("Could not allocate a slot within time limit: " + timeout);
		}
	}
}
