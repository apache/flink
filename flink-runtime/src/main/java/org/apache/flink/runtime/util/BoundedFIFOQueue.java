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

package org.apache.flink.runtime.util;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * {@code BoundedFIFOQueue} collects elements up to given amount. Reaching this limit will result in
 * removing the oldest element from this queue (First-In/First-Out; FIFO).
 *
 * @param <T> The type of elements collected.
 */
public class BoundedFIFOQueue<T> implements Iterable<T>, Serializable {

    private static final long serialVersionUID = -890727339944580409L;

    private final int maxSize;
    private final Queue<T> elements;

    /**
     * Creates a {@code BoundedFIFOQueue} with the given maximum size.
     *
     * @param maxSize The maximum size of this queue. Exceeding this limit would result in removing
     *     the oldest element (FIFO).
     * @throws IllegalArgumentException If {@code maxSize} is less than 0.
     */
    public BoundedFIFOQueue(int maxSize) {
        Preconditions.checkArgument(maxSize >= 0, "The maximum size should be at least 0.");

        this.maxSize = maxSize;
        this.elements = new LinkedList<>();
    }

    /**
     * Adds an element to the end of the queue. An element will be removed from the head of the
     * queue if the queue would exceed its maximum size by adding the new element.
     *
     * @param element The element that should be added to the end of the queue.
     * @throws NullPointerException If {@code null} is passed as an element.
     */
    public void add(T element) {
        Preconditions.checkNotNull(element);
        if (elements.add(element) && elements.size() > maxSize) {
            elements.poll();
        }
    }

    /**
     * Returns the number of currently stored elements.
     *
     * @return The number of currently stored elements.
     */
    public int size() {
        return this.elements.size();
    }

    /**
     * Returns the {@code BoundedFIFOQueue}'s {@link Iterator}.
     *
     * @return The queue's {@code Iterator}.
     */
    @Override
    public Iterator<T> iterator() {
        return elements.iterator();
    }
}
