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

package org.apache.flink.api.common.operators.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.TraversableOnceException;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * The KeyValueIterator returns a key and all values that belong to the key (share the same key).
 */
@Internal
public final class ListKeyGroupedIterator<E> {

    private final List<E> input;

    private final TypeSerializer<E> serializer; // != null if the elements should be copied

    private final TypeComparator<E> comparator;

    private ValuesIterator valuesIterator;

    private int currentPosition = 0;

    private E lookahead;

    private boolean done;

    /**
     * Initializes the ListKeyGroupedIterator..
     *
     * @param input The list with the input elements.
     * @param comparator The comparator for the data type iterated over.
     */
    public ListKeyGroupedIterator(
            List<E> input, TypeSerializer<E> serializer, TypeComparator<E> comparator) {
        if (input == null || comparator == null) {
            throw new NullPointerException();
        }

        this.input = input;
        this.serializer = serializer;
        this.comparator = comparator;

        this.done = input.isEmpty() ? true : false;
    }

    /**
     * Moves the iterator to the next key. This method may skip any values that have not yet been
     * returned by the iterator created by the {@link #getValues()} method. Hence, if called
     * multiple times it "removes" key groups.
     *
     * @return true, if the input iterator has an other group of records with the same key.
     */
    public boolean nextKey() throws IOException {

        if (lookahead != null) {
            // common case: whole value-iterator was consumed and a new key group is available.
            this.comparator.setReference(this.lookahead);
            this.valuesIterator.next = this.lookahead;
            this.lookahead = null;
            this.valuesIterator.iteratorAvailable = true;
            return true;
        }

        // first element, empty/done, or the values iterator was not entirely consumed
        if (this.done) {
            return false;
        }

        if (this.valuesIterator != null) {
            // values was not entirely consumed. move to the next key
            // Required if user code / reduce() method did not read the whole value iterator.
            E next;
            while (true) {
                if (currentPosition < input.size()
                        && (next = this.input.get(currentPosition++)) != null) {
                    if (!this.comparator.equalToReference(next)) {
                        // the keys do not match, so we have a new group. store the current key
                        this.comparator.setReference(next);
                        this.valuesIterator.next = next;
                        this.valuesIterator.iteratorAvailable = true;
                        return true;
                    }
                } else {
                    // input exhausted
                    this.valuesIterator.next = null;
                    this.valuesIterator = null;
                    this.done = true;
                    return false;
                }
            }
        } else {
            // first element
            // get the next element
            E first = input.get(currentPosition++);
            if (first != null) {
                this.comparator.setReference(first);
                this.valuesIterator = new ValuesIterator(first, serializer);
                return true;
            } else {
                // empty input, set everything null
                this.done = true;
                return false;
            }
        }
    }

    private E advanceToNext() {
        if (currentPosition < input.size()) {
            E next = input.get(currentPosition++);
            if (comparator.equalToReference(next)) {
                // same key
                return next;
            } else {
                // moved to the next key, no more values here
                lookahead = next;
                return null;
            }
        } else {
            // backing iterator is consumed
            this.done = true;
            return null;
        }
    }

    /**
     * Returns an iterator over all values that belong to the current key. The iterator is initially
     * <code>null</code> (before the first call to {@link #nextKey()} and after all keys are
     * consumed. In general, this method returns always a non-null value, if a previous call to
     * {@link #nextKey()} return <code>true</code>.
     *
     * @return Iterator over all values that belong to the current key.
     */
    public ValuesIterator getValues() {
        return this.valuesIterator;
    }

    // --------------------------------------------------------------------------------------------

    public final class ValuesIterator implements Iterator<E>, Iterable<E> {

        private E next;

        private boolean iteratorAvailable = true;

        private final TypeSerializer<E> serializer;

        private ValuesIterator(E first, TypeSerializer<E> serializer) {
            this.next = first;
            this.serializer = serializer;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public E next() {
            if (this.next != null) {
                E current = this.next;
                this.next = ListKeyGroupedIterator.this.advanceToNext();
                return serializer.copy(current);
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
            if (iteratorAvailable) {
                iteratorAvailable = false;
                return this;
            } else {
                throw new TraversableOnceException();
            }
        }

        public E getCurrent() {
            return next;
        }
    }
}
