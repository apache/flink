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

import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.util.event.EventListener;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The task event handler dispatches events flowing backwards from a receiver
 * to a producer. It only supports programs, where the producer and consumer
 * are running at the same time.
 * <p/>
 * The publish method is either called from the local input channel or the
 * network I/O thread.
 * <p/>
 * This class is thread-safe.
 */
public class TaskEventDispatcher {

	ConcurrentMap<IntermediateResultPartitionID, EventListener<TaskEvent>> listeners =
			new ConcurrentHashMap<IntermediateResultPartitionID, EventListener<TaskEvent>>();

	public void register(IntermediateResultPartitionID id, EventListener<TaskEvent> listener) {
		if (listeners.putIfAbsent(id, listener) != null) {
			throw new IllegalStateException("Event dispatcher already contains event listener.");
		}
	}

	public void unregister(IntermediateResultPartitionID id) {
		listeners.remove(id);
	}

	/**
	 * Publishes the event to the registered {@link EventListener} instance.
	 * <p/>
	 * This method is called either called from the local input channel or the
	 * network I/O thread.
	 */
	public boolean publish(IntermediateResultPartitionID id, TaskEvent event) {
		EventListener<TaskEvent> listener = listeners.get(id);

		if (listener != null) {
			listener.onEvent(event);
			return true;
		}

		return false;
	}
}
