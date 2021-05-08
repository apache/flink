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

package org.apache.flink.table.planner.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Copied from {@link org.apache.flink.runtime.util.SingleElementIterator}. Fix iterator to set
 * available true.
 */
public final class SingleElementIterator<E> implements Iterator<E>, Iterable<E> {

    private E current;
    private boolean available = false;

    /**
     * Resets the element. After this call, the iterator has one element available, which is the
     * given element.
     *
     * @param current The element to make available to the iterator.
     */
    public void set(E current) {
        this.current = current;
        this.available = true;
    }

    @Override
    public boolean hasNext() {
        return available;
    }

    @Override
    public E next() {
        if (available) {
            available = false;
            return current;
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<E> iterator() {
        available = true;
        return this;
    }
}
