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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.streaming.runtime.tasks.CheckpointBarrier;

import java.io.IOException;

/**
 * The CheckpointBarrierHandler reacts to checkpoint barrier arriving from the input channels.
 * Different implementations may either simply track barriers, or block certain inputs on
 * barriers.
 */
public interface CheckpointBarrierHandler {

	/**
	 * Returns the next {@link BufferOrEvent} that the operator may consume.
	 * This call blocks until the next BufferOrEvent is available, ir until the stream
	 * has been determined to be finished.
	 * 
	 * @return The next BufferOrEvent, or {@code null}, if the stream is finished.
	 * @throws java.io.IOException Thrown, if the network or local disk I/O fails.
	 * @throws java.lang.InterruptedException Thrown, if the thread is interrupted while blocking during
	 *                                        waiting for the next BufferOrEvent to become available.
	 */
	BufferOrEvent getNextNonBlocked() throws IOException, InterruptedException;

	void registerCheckpointEventHandler(EventListener<CheckpointBarrier> checkpointHandler);
	
	void cleanup() throws IOException;

	/**
	 * Checks if the barrier handler has buffered any data internally.
	 * @return True, if no data is buffered internally, false otherwise.
	 */
	boolean isEmpty();
}
