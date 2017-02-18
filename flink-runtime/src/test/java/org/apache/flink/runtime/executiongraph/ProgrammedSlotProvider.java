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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.SlotProvider;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A slot provider where one can pre-set the slot futures for tasks based on
 * vertex ID and subtask index.
 */
class ProgrammedSlotProvider implements SlotProvider {

	private final Map<JobVertexID, Future<SimpleSlot>[]> slotFutures = new HashMap<>();

	private final int parallelism;

	public ProgrammedSlotProvider(int parallelism) {
		checkArgument(parallelism > 0);
		this.parallelism = parallelism;
	}

	public void addSlot(JobVertexID vertex, int subtaskIndex, Future<SimpleSlot> future) {
		checkNotNull(vertex);
		checkNotNull(future);
		checkArgument(subtaskIndex >= 0 && subtaskIndex < parallelism);

		Future<SimpleSlot>[] futures = slotFutures.get(vertex);
		if (futures == null) {
			@SuppressWarnings("unchecked")
			Future<SimpleSlot>[] newArray = (Future<SimpleSlot>[]) new Future<?>[parallelism];
			futures = newArray;
			slotFutures.put(vertex, futures);
		}

		futures[subtaskIndex] = future;
	}

	public void addSlots(JobVertexID vertex, Future<SimpleSlot>[] futures) {
		checkNotNull(vertex);
		checkNotNull(futures);
		checkArgument(futures.length == parallelism);

		slotFutures.put(vertex, futures);
	}

	@Override
	public Future<SimpleSlot> allocateSlot(ScheduledUnit task, boolean allowQueued) {
		JobVertexID vertexId = task.getTaskToExecute().getVertex().getJobvertexId();
		int subtask = task.getTaskToExecute().getParallelSubtaskIndex();

		Future<SimpleSlot>[] forTask = slotFutures.get(vertexId);
		if (forTask != null) {
			Future<SimpleSlot> future = forTask[subtask];
			if (future != null) {
				return future;
			}
		}

		throw new IllegalArgumentException("No registered slot future for task " + vertexId + " (" + subtask + ')');
	}
}
