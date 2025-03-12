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
 * @param <RequestEntryT> The type of request entries being buffered.
 */
@PublicEvolving
public interface BufferWrapper<RequestEntryT extends Serializable> {

    /**
     * Adds an entry (RequestEntryWrapper<RequestEntryT>) to the buffer. Implementations can decide
     * how to store the entry.
     *
     * @param entry The request entry to add.
     * @param prioritize If true, the entry should be prioritized (e.g. retried before others).
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

    /**
     * Generic builder interface for creating instances of {@link BufferWrapper}.
     *
     * @param <R> The type of {@link BufferWrapper} that the builder will create.
     * @param <RequestEntryT> The type of request entries that the buffer wrapper will store.
     */
    interface Builder<R extends BufferWrapper<RequestEntryT>, RequestEntryT extends Serializable> {
        /**
         * Constructs and returns an instance of {@link BufferWrapper} with the configured
         * parameters.
         *
         * @return A new instance of {@link BufferWrapper}.
         */
        R build();
    }
}
