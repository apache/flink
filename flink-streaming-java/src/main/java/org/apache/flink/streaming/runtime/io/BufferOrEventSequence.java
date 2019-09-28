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

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * This class represents a sequence of buffers and events which are blocked by
 * {@link CheckpointedInputGate}. The sequence of buffers and events can be
 * read back using the method {@link #getNext()}.
 */
@Internal
public interface BufferOrEventSequence {

	/**
	 * Initializes the sequence for reading.
	 */
	void open();

	/**
	 * Gets the next BufferOrEvent from the sequence, or {@code null}, if the
	 * sequence is exhausted.
	 *
	 * @return The next BufferOrEvent from the buffered sequence, or {@code null} (end of sequence).
	 */
	@Nullable
	BufferOrEvent getNext() throws IOException;

	/**
	 * Cleans up all the resources held by the sequence.
	 */
	void cleanup() throws IOException;

	/**
	 * Gets the size of the sequence.
	 */
	long size();
}
