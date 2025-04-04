/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Collection;

/**
 * A flexible wrapper interface for managing buffered request entries in an async sink. This allows
 * sink implementations to define and optimize their own data structures for request buffering.
 *
 * <p>{@link RequestEntryWrapper} is buffered instead of raw request entries (like {@code InputT})
 * to support metadata tracking (e.g., entry size, retry priority). This makes it easier to manage
 * retries and batch sizing without burdening the sink logic.
 *
 * <p>Sink developers can provide custom implementations of this interface (e.g., circular buffer,
 * priority queue) to control how entries are buffered.
 *
 * @param <RequestEntryT> The type of request entries being buffered.
 */
@PublicEvolving
public interface RequestBuffer<RequestEntryT extends Serializable> {

    /**
     * Adds an entry ({@code RequestEntryWrapper<RequestEntryT>}) to the buffer. Implementations can
     * decide how to store the entry.
     *
     * @param entry The request entry to add.
     * @param prioritize If true, the entry should be prioritized (e.g. retried before others)
     *     required to maintain ordering on retries.
     */
    void add(RequestEntryWrapper<RequestEntryT> entry, boolean prioritize);

    /**
     * Retrieves and removes the next available request entry from the buffer. The removal order is
     * determined by the implementation.
     *
     * @return The removed request entry, or null if the buffer is empty.
     */
    RequestEntryWrapper<RequestEntryT> poll();

    /**
     * Retrieves, but does not remove, the next available request entry from the buffer. This allows
     * checking the next request before processing.
     *
     * @return The next request entry, or null if the buffer is empty.
     */
    RequestEntryWrapper<RequestEntryT> peek();

    /**
     * Checks whether the buffer is empty. Useful for determining if there are pending entries
     * before flushing.
     *
     * @return True if the buffer contains no entries, false otherwise.
     */
    boolean isEmpty();

    /**
     * Returns the number of request entries currently in the buffer. Can be used for batching
     * decisions.
     *
     * @return The total number of buffered entries.
     */
    int size();

    /**
     * Retrieves all buffered request entries as a collection. Implementations should return a
     * snapshot of the buffer for checkpointing.
     *
     * <p>The returned collection:
     *
     * <ul>
     *   <li>Must preserve the order in which entries were added (FIFO).
     *   <li>Must not modify or clear the internal buffer.
     * </ul>
     *
     * @return A collection of all buffered request entries.
     */
    Collection<RequestEntryWrapper<RequestEntryT>> getBufferedState();

    /**
     * Returns the total size of all buffered request entries in bytes.
     *
     * <p>Tracks the cumulative size of all elements in {@code bufferedRequestEntries} to facilitate
     * the criterion for flushing after maxBatchSizeInBytes is reached.
     *
     * @return The total buffered size in bytes.
     */
    long totalSizeInBytes();
}
