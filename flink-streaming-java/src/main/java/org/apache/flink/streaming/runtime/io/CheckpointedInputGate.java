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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.AsyncDataInput;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;

import java.io.IOException;

/**
 * The {@link CheckpointedInputGate} uses {@link CheckpointBarrierHandler} to handle incoming
 * {@link org.apache.flink.runtime.io.network.api.CheckpointBarrier} from the {@link org.apache.flink.runtime.io.network.partition.consumer.InputGate}.
 */
@Internal
public interface CheckpointedInputGate extends AsyncDataInput<BufferOrEvent> {
	/**
	 * Cleans up all internally held resources.
	 *
	 * @throws IOException Thrown if the cleanup of I/O resources failed.
	 */
	void cleanup() throws IOException;

	/**
	 * Checks if the barrier handler has buffered any data internally.
	 * @return {@code True}, if no data is buffered internally, {@code false} otherwise.
	 */
	boolean isEmpty();

	/**
	 * Gets the time that the latest alignment took, in nanoseconds.
	 * If there is currently an alignment in progress, it will return the time spent in the
	 * current alignment so far.
	 *
	 * @return The duration in nanoseconds
	 */
	long getAlignmentDurationNanos();

	/**
	 * @return number of underlying input channels.
	 */
	int getNumberOfInputChannels();
}
