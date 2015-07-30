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

import com.google.common.collect.Maps;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.util.event.EventListener;

import java.util.Map;

/**
 * The task event dispatcher dispatches events flowing backwards from a consuming task to the task
 * producing the consumed result.
 *
 * <p> Backwards events only work for tasks, which produce pipelined results, where both the
 * producing and consuming task are running at the same time.
 */
public class TaskEventDispatcher {

	private final Map<ResultPartitionID, ResultPartitionWriter> registeredWriters = Maps.newHashMap();

	public void registerWriterForIncomingTaskEvents(ResultPartitionID partitionId, ResultPartitionWriter writer) {
		synchronized (registeredWriters) {
			if (registeredWriters.put(partitionId, writer) != null) {
				throw new IllegalStateException("Already registered at task event dispatcher.");
			}
		}
	}

	public void unregisterWriter(ResultPartitionWriter writer) {
		synchronized (registeredWriters) {
			registeredWriters.remove(writer.getPartitionId());
		}
	}

	/**
	 * Publishes the event to the registered {@link ResultPartitionWriter} instances.
	 * <p>
	 * This method is either called directly from a {@link LocalInputChannel} or the network I/O
	 * thread on behalf of a {@link RemoteInputChannel}.
	 */
	public boolean publish(ResultPartitionID partitionId, TaskEvent event) {
		EventListener<TaskEvent> listener = registeredWriters.get(partitionId);

		if (listener != null) {
			listener.onEvent(event);
			return true;
		}

		return false;
	}

	public void clearAll() {
		synchronized (registeredWriters) {
			registeredWriters.clear();
		}
	}

	/**
	 * Returns the number of currently registered writers.
	 */
	int getNumberOfRegisteredWriters() {
		synchronized (registeredWriters) {
			return registeredWriters.size();
		}
	}
}
