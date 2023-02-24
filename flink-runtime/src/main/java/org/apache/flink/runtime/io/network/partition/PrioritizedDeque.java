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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A deque-like data structure that supports prioritization of elements, such they will be polled
 * before any non-priority elements.
 *
 * <p>{@implNote The current implementation deliberately does not implement the respective interface
 * to minimize the maintenance effort. Furthermore, it's optimized for handling non-priority
 * elements, such that all operations for adding priority elements are much slower than the
 * non-priority counter-parts.}
 *
 * <p>Note that all element tests are performed by identity.
 *
 * @param <T> the element type.
 */
@Internal
public final class PrioritizedDeque<T> implements Iterable<T> {
    private final Deque<T> deque = new ArrayDeque<>();
    private int numPriorityElements;

    /**
     * Adds a priority element to this deque, such that it will be polled after all existing
     * priority elements but before any non-priority element.
     *
     * @param element the element to add
     */
    public void addPriorityElement(T element) {
        // priority elements are rather rare and short-lived, so most of there are none
        if (numPriorityElements == 0) {
            deque.addFirst(element);
        } else if (numPriorityElements == deque.size()) {
            // no non-priority elements
            deque.add(element);
        } else {
            // remove all priority elements
            final ArrayDeque<T> priorPriority = new ArrayDeque<>(numPriorityElements);
            for (int index = 0; index < numPriorityElements; index++) {
                priorPriority.addFirst(deque.poll());
            }
            deque.addFirst(element);
            // read them before the newly added element
            for (final T priorityEvent : priorPriority) {
                deque.addFirst(priorityEvent);
            }
        }
        numPriorityElements++;
    }

    /**
     * Adds a non-priority element to this deque, which will be polled last.
     *
     * @param element the element to add
     */
    public void add(T element) {
        deque.add(element);
    }

    /**
     * Convenience method for adding an element with optional priority and prior removal.
     *
     * @param element the element to add
     * @param priority flag indicating if it's a priority or non-priority element
     * @param prioritize flag that hints that the element is already in this deque, potentially as
     *     non-priority element.
     */
    public void add(T element, boolean priority, boolean prioritize) {
        if (!priority) {
            add(element);
        } else {
            if (prioritize) {
                prioritize(element);
            } else {
                addPriorityElement(element);
            }
        }
    }

    /**
     * Prioritizes an already existing element. Note that this method assumes identity.
     *
     * <p>{@implNote Since this method removes the element and reinserts it in a priority position
     * in general, some optimizations for special cases are used.}
     *
     * @param element the element to prioritize.
     */
    public void prioritize(T element) {
        final Iterator<T> iterator = deque.iterator();
        // Already prioritized? Then, do not reorder elements.
        for (int i = 0; i < numPriorityElements && iterator.hasNext(); i++) {
            if (iterator.next() == element) {
                return;
            }
        }
        // If the next non-priority element is the given element, we can simply include it in the
        // priority section
        if (iterator.hasNext() && iterator.next() == element) {
            numPriorityElements++;
            return;
        }
        // Remove the given element.
        while (iterator.hasNext()) {
            if (iterator.next() == element) {
                iterator.remove();
                break;
            }
        }
        addPriorityElement(element);
    }

    /** Returns an unmodifiable collection view. */
    public Collection<T> asUnmodifiableCollection() {
        return Collections.unmodifiableCollection(deque);
    }

    /**
     * Find first element matching the {@link Predicate}, remove it from the {@link
     * PrioritizedDeque} and return it.
     *
     * @return removed element
     */
    public T getAndRemove(Predicate<T> preCondition) {
        Iterator<T> iterator = deque.iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            T next = iterator.next();
            if (preCondition.test(next)) {
                if (i < numPriorityElements) {
                    numPriorityElements--;
                }
                iterator.remove();
                return next;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * Polls the first priority element or non-priority element if the former does not exist.
     *
     * @return the first element or null.
     */
    @Nullable
    public T poll() {
        final T polled = deque.poll();
        if (polled != null && numPriorityElements > 0) {
            numPriorityElements--;
        }
        return polled;
    }

    /**
     * Returns the first priority element or non-priority element if the former does not exist.
     *
     * @return the first element or null.
     */
    @Nullable
    public T peek() {
        return deque.peek();
    }

    /** Returns the current number of priority elements ([0; {@link #size()}]). */
    public int getNumPriorityElements() {
        return numPriorityElements;
    }

    /**
     * Returns whether the given element is a known priority element. Test is performed by identity.
     */
    public boolean containsPriorityElement(T element) {
        if (numPriorityElements == 0) {
            return false;
        }
        final Iterator<T> iterator = deque.iterator();
        for (int i = 0; i < numPriorityElements && iterator.hasNext(); i++) {
            if (iterator.next() == element) {
                return true;
            }
        }
        return false;
    }

    /** Returns the number of priority and non-priority elements. */
    public int size() {
        return deque.size();
    }

    /** @return read-only iterator */
    public Iterator<T> iterator() {
        return Collections.unmodifiableCollection(deque).iterator();
    }

    /** Removes all priority and non-priority elements. */
    public void clear() {
        deque.clear();
        numPriorityElements = 0;
    }

    /** Returns true if there are no elements. */
    public boolean isEmpty() {
        return deque.isEmpty();
    }

    /**
     * Returns whether the given element is contained in this list. Test is performed by identity.
     */
    public boolean contains(T element) {
        if (deque.isEmpty()) {
            return false;
        }
        final Iterator<T> iterator = deque.iterator();
        while (iterator.hasNext()) {
            if (iterator.next() == element) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PrioritizedDeque<?> that = (PrioritizedDeque<?>) o;
        return numPriorityElements == that.numPriorityElements && deque.equals(that.deque);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deque, numPriorityElements);
    }

    @Override
    public String toString() {
        return deque.toString();
    }

    /**
     * Returns the last non-priority element or priority element if the former does not exist.
     *
     * @return the last element or null.
     */
    @Nullable
    public T peekLast() {
        return deque.peekLast();
    }

    public Stream<T> stream() {
        return StreamSupport.stream(spliterator(), false);
    }
}
