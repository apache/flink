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

import java.io.IOException;
import java.util.Optional;

/**
 * The {@link BufferStorage} takes the buffers and events from a data stream and adds them in a sequence.
 * After a number of elements have been added, the {@link BufferStorage} can {@link #rollOver() "roll over"}.
 * After rolling over, previously stored buffers are available for reading via {@link #pollNext()}.
 */
@Internal
public interface BufferStorage extends AutoCloseable {

	/**
	 * Adds a buffer or event to the {@link BufferStorage}.
	 *
	 * @param boe The buffer or event to be added into the blocker.
	 */
	void add(BufferOrEvent boe) throws IOException;

	/**
	 * @return true if size limit was exceeded.
	 */
	boolean isFull();

	/**
	 * Start returning next sequence of stored {@link BufferOrEvent}s.
	 */
	void rollOver() throws IOException;

	/**
	 * @return the number of pending bytes blocked in the current sequence - bytes that are have not
	 * been yet rolled, but are already blocked.
	 */
	long getPendingBytes();

	/**
	 * @return the number of already rolled bytes in in blocked sequences.
	 */
	long getRolledBytes();

	/**
	 * @return true if this {@link BufferStorage} doesn't store and data.
	 */
	boolean isEmpty();

	Optional<BufferOrEvent> pollNext() throws IOException;

	long getMaxBufferedBytes();

	/**
	 * Cleans up all the resources in the current sequence.
	 */
	@Override
	void close() throws IOException;
}
