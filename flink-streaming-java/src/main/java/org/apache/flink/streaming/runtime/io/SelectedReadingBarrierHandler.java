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
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

import java.io.IOException;

/**
 * The CheckpointBarrierHandler reacts to checkpoint barrier arriving from the input channels.
 * Different implementations may either simply track barriers, or block certain inputs on
 * barriers.
 */
@Internal
public interface SelectedReadingBarrierHandler extends CheckpointBarrierHandler {

	/**
	 * Returns the next {@link BufferOrEvent} on the given sub {@link InputGate} that
	 * the operator may consume, if input is a complex {@link InputGate}
	 * (e.g. {@link org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate}).
	 * This call blocks until the next BufferOrEvent is available, or until the stream
	 * has been determined to be finished.
	 *
	 * @param subInputGate is a sub InputGate of input
	 *
	 * @return The next BufferOrEvent, or {@code null}, if the stream is finished.
	 *
	 * @throws IOException Thrown if the network or local disk I/O fails.
	 *
	 * @throws InterruptedException Thrown if the thread is interrupted while blocking during
	 *                              waiting for the next BufferOrEvent to become available.
	 * @throws Exception Thrown in case that a checkpoint fails that is started as the result of receiving
	 *                   the last checkpoint barrier
	 */
	BufferOrEvent getNextNonBlocked(InputGate subInputGate) throws Exception;

	/**
	 * Returns the next {@link BufferOrEvent} on the given sub {@link InputGate} that
	 * the operator may consume, if input is a complex {@link InputGate}
	 * (e.g. {@link org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate}).
	 * This call will not block if there is no BufferOrEvent available.
	 *
	 * @param subInputGate is a sub InputGate of input
	 *
	 * @return The next BufferOrEvent, or {@code null}, if there is no BufferOrEvent available.
	 *
	 * @throws IOException Thrown if the network or local disk I/O fails.
	 *
	 * @throws InterruptedException Thrown if the thread is interrupted while blocking during
	 *                              waiting for the next BufferOrEvent to become available.
	 * @throws Exception Thrown in case that a checkpoint fails that is started as the result of receiving
	 *                   the last checkpoint barrier
	 */
	BufferOrEvent pollNext(InputGate subInputGate) throws Exception;

	/**
	 * Get the number of all reading sub {@link InputGate}.
	 *
	 * @return see {@link InputGate#getSubInputGateCount()}.
	 */
	int getSubInputGateCount();

	/**
	 * Get a sub {@link InputGate} for selected reading.
	 *
	 * @param index The index of the sub InputGate.
	 * @return see {@link InputGate#getSubInputGate(int)}.
	 */
	InputGate getSubInputGate(int index);

	/**
	 * Get the number of all {@link org.apache.flink.runtime.io.network.partition.consumer.InputChannel}.
	 *
	 * @return the number of all channels.
	 */
	int getNumberOfInputChannels();

	/**
	 * Get all the {@link InputChannel}s in this {@link SelectedReadingBarrierHandler}.
	 *
	 * @return an array contains all the input channels, the index of an input channel
	 *         in the array equals to its channel index in this barrier handler.
	 */
	InputChannel[] getAllInputChannels();
}
