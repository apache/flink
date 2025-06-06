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

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;

/**
 * Default wrapper implementation that uses an {@link ArrayDeque} as the underlying data structure.
 */
@Internal
public class DequeRequestBuffer<RequestEntryT extends Serializable>
        implements RequestBuffer<RequestEntryT> {

    private final Deque<RequestEntryWrapper<RequestEntryT>> buffer;
    private long totalSizeInBytes;

    /** Creates an empty buffer backed by an {@link ArrayDeque}. */
    public DequeRequestBuffer() {
        buffer = new ArrayDeque<>();
        totalSizeInBytes = 0L;
    }

    /**
     * Adds a request entry to the buffer. If {@code prioritize} is true, the entry is inserted at
     * the front (for retries). Otherwise, it is added at the end following FIFO order.
     *
     * @param entry The request entry to add.
     * @param prioritize If true, insert at the front; otherwise, add at the end.
     */
    @Override
    public void add(RequestEntryWrapper<RequestEntryT> entry, boolean prioritize) {
        if (prioritize) {
            buffer.addFirst(entry);
        } else {
            buffer.add(entry);
        }
        totalSizeInBytes += entry.getSize();
    }

    /** {@inheritDoc} */
    @Override
    public RequestEntryWrapper<RequestEntryT> peek() {
        return buffer.peek();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isEmpty() {
        return buffer.isEmpty();
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
        return buffer.size();
    }

    /** {@inheritDoc} */
    @Override
    public Collection<RequestEntryWrapper<RequestEntryT>> getBufferedState() {
        return new ArrayList<>(buffer);
    }

    /** {@inheritDoc} */
    @Override
    public RequestEntryWrapper<RequestEntryT> poll() {
        RequestEntryWrapper<RequestEntryT> entry = buffer.poll();
        if (entry != null) {
            totalSizeInBytes = Math.max(0, totalSizeInBytes - entry.getSize());
        }
        return entry;
    }

    /** {@inheritDoc} */
    @Override
    public long totalSizeInBytes() {
        return totalSizeInBytes;
    }
}
