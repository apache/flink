/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;

/**
 * Interface for collection that gives in order access to elements w.r.t their priority.
 *
 * @param <T> type of elements in the ordered set.
 */
@Internal
public interface InternalPriorityQueue<T> {

    /**
     * Retrieves and removes the first element (w.r.t. the order) of this set, or returns {@code
     * null} if this set is empty.
     *
     * @return the first element of this ordered set, or {@code null} if this set is empty.
     */
    @Nullable
    T poll();

    /**
     * Retrieves, but does not remove, the element (w.r.t. order) of this set, or returns {@code
     * null} if this set is empty.
     *
     * @return the first element (w.r.t. order) of this ordered set, or {@code null} if this set is
     *     empty.
     */
    @Nullable
    T peek();

    /**
     * Adds the given element to the set, if it is not already contained.
     *
     * @param toAdd the element to add to the set.
     * @return <code>true</code> if the operation changed the head element or if it is unclear if
     *     the head element changed. Only returns <code>false</code> if the head element was not
     *     changed by this operation.
     */
    boolean add(@Nonnull T toAdd);

    /**
     * Removes the given element from the set, if is contained in the set.
     *
     * @param toRemove the element to remove.
     * @return <code>true</code> if the operation changed the head element or if it is unclear if
     *     the head element changed. Only returns <code>false</code> if the head element was not
     *     changed by this operation.
     */
    boolean remove(@Nonnull T toRemove);

    /**
     * Check if the set contains any elements.
     *
     * @return true if the set is empty, i.e. no element is contained.
     */
    boolean isEmpty();

    /**
     * Returns the number of elements in this set.
     *
     * @return the number of elements in this set.
     */
    @Nonnegative
    int size();

    /** Adds all the given elements to the set. */
    void addAll(@Nullable Collection<? extends T> toAdd);

    /** Iterator over all elements, no order guaranteed. Iterator must be closed after usage. */
    @Nonnull
    CloseableIterator<T> iterator();
}
