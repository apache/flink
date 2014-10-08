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

package org.apache.flink.runtime.io.network;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.api.writer.BufferWriter;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.util.event.EventListener;

import java.util.ArrayList;
import java.util.List;

/**
 * The task event dispatcher dispatches events flowing backwards from a consumer
 * to a producer. It only supports programs, where the producer and consumer
 * are running at the same time.
 * <p>
 * The publish method is either called from the local input channel or the
 * network I/O thread.
 */
public class TaskEventDispatcher {

	Table<ExecutionAttemptID, IntermediateResultPartitionID, BufferWriter> registeredWriters = HashBasedTable.create();

	public void registerWriterForIncomingTaskEvents(ExecutionAttemptID executionId, IntermediateResultPartitionID partitionId, BufferWriter listener) {
		synchronized (registeredWriters) {
			if (registeredWriters.put(executionId, partitionId, listener) != null) {
				throw new IllegalStateException("Event dispatcher already contains buffer writer.");
			}
		}
	}

	public void unregisterWriters(ExecutionAttemptID executionId) {
		synchronized (registeredWriters) {
			List<IntermediateResultPartitionID> writersToUnregister = new ArrayList<IntermediateResultPartitionID>();

			for (IntermediateResultPartitionID partitionId : registeredWriters.row(executionId).keySet()) {
				writersToUnregister.add(partitionId);
			}

			for(IntermediateResultPartitionID partitionId : writersToUnregister) {
				registeredWriters.remove(executionId, partitionId);
			}
		}
	}

	/**
	 * Publishes the event to the registered {@link EventListener} instance.
	 * <p>
	 * This method is either called from a local input channel or the network
	 * I/O thread on behalf of a remote input channel.
	 */
	public boolean publish(ExecutionAttemptID executionId, IntermediateResultPartitionID partitionId, TaskEvent event) {
		EventListener<TaskEvent> listener = registeredWriters.get(executionId, partitionId);

		if (listener != null) {
			listener.onEvent(event);
			return true;
		}

		return false;
	}

	int getNumberOfRegisteredWriters() {
		synchronized (registeredWriters) {
			return registeredWriters.size();
		}
	}
}
