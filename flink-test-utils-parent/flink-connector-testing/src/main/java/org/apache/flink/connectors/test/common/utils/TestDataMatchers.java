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

package org.apache.flink.connectors.test.common.utils;

import org.apache.flink.annotation.Internal;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/** Matchers for validating test data. */
@Internal
public class TestDataMatchers {

    // ----------------------------  Matcher Builders ----------------------------------
    public static <T> MultipleSplitDataMatcher<T> matchesMultipleSplitTestData(
            Collection<Collection<T>> testDataCollections) {
        return new MultipleSplitDataMatcher<>(testDataCollections);
    }

    public static <T> SplitDataMatcher<T> matchesSplitTestData(Collection<T> testData) {
        return new SplitDataMatcher<>(testData);
    }

    public static <T> SplitDataMatcher<T> matchesSplitTestData(Collection<T> testData, int limit) {
        return new SplitDataMatcher<>(testData, limit);
    }

    // ---------------------------- Matcher Definitions --------------------------------

    /**
     * Matcher for validating test data in a single split.
     *
     * @param <T> Type of validating record
     */
    public static class SplitDataMatcher<T> extends TypeSafeDiagnosingMatcher<Iterator<T>> {
        private static final int UNSET = -1;

        private final Collection<T> testData;
        private final int limit;

        private String mismatchDescription = null;

        public SplitDataMatcher(Collection<T> testData) {
            this.testData = testData;
            this.limit = UNSET;
        }

        public SplitDataMatcher(Collection<T> testData, int limit) {
            if (limit > testData.size()) {
                throw new IllegalArgumentException(
                        "Limit validation size should be less than number of test records");
            }
            this.testData = testData;
            this.limit = limit;
        }

        @Override
        protected boolean matchesSafely(Iterator<T> resultIterator, Description description) {
            if (mismatchDescription != null) {
                description.appendText(mismatchDescription);
                return false;
            }

            int recordCounter = 0;
            for (T testRecord : testData) {
                if (!resultIterator.hasNext()) {
                    mismatchDescription = "Result data is less than test data";
                    return false;
                }
                T resultRecord = resultIterator.next();
                if (!testRecord.equals(resultRecord)) {
                    mismatchDescription =
                            String.format(
                                    "Mismatched record at position %d: Expected '%s' but was '%s'",
                                    recordCounter, testRecord, resultRecord);
                    return false;
                }
                recordCounter++;
                if (limit != UNSET && recordCounter >= limit) {
                    break;
                }
            }
            if (limit == UNSET && resultIterator.hasNext()) {
                mismatchDescription = "Result data is more than test data";
                return false;
            } else {
                return true;
            }
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(
                    "Records consumed by Flink should be identical to test data "
                            + "and preserve the order in split");
        }
    }

    /**
     * Matcher for validating test data from multiple splits.
     *
     * <p>Each collection has a pointer (iterator) pointing to current checking record. When a
     * record is received in the stream, it will be compared to all current pointing records in
     * collections, and the pointer to the identical record will move forward.
     *
     * <p>If the stream preserves the correctness and order of records in all splits, all pointers
     * should reach the end of the collection finally.
     *
     * @param <T> Type of validating record
     */
    public static class MultipleSplitDataMatcher<T> extends TypeSafeDiagnosingMatcher<Iterator<T>> {

        private final List<IteratorWithCurrent<T>> testDataIterators = new ArrayList<>();

        private String mismatchDescription = null;

        public MultipleSplitDataMatcher(Collection<Collection<T>> testDataCollections) {
            for (Collection<T> testDataCollection : testDataCollections) {
                testDataIterators.add(new IteratorWithCurrent<>(testDataCollection.iterator()));
            }
        }

        @Override
        protected boolean matchesSafely(Iterator<T> resultIterator, Description description) {
            if (mismatchDescription != null) {
                description.appendText(mismatchDescription);
                return false;
            }
            while (resultIterator.hasNext()) {
                final T record = resultIterator.next();
                if (!matchThenNext(record)) {
                    mismatchDescription = String.format("Unexpected record '%s'", record);
                    return false;
                }
            }
            if (!hasReachedEnd()) {
                mismatchDescription = "Result data is less than test data";
                return false;
            } else {
                return true;
            }
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(
                    "Records consumed by Flink should be identical to test data "
                            + "and preserve the order in multiple splits");
        }

        /**
         * Check if any pointing data is identical to the record from the stream, and move the
         * pointer to next record if matched. Otherwise throws an exception.
         *
         * @param record Record from stream
         */
        private boolean matchThenNext(T record) {
            for (IteratorWithCurrent<T> testDataIterator : testDataIterators) {
                if (record.equals(testDataIterator.current())) {
                    testDataIterator.next();
                    return true;
                }
            }
            return false;
        }

        /**
         * Whether all pointers have reached the end of collections.
         *
         * @return True if all pointers have reached the end.
         */
        private boolean hasReachedEnd() {
            for (IteratorWithCurrent<T> testDataIterator : testDataIterators) {
                if (testDataIterator.hasNext()) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * An iterator wrapper which can access the element that the iterator is currently pointing to.
     *
     * @param <E> The type of elements returned by this iterator.
     */
    static class IteratorWithCurrent<E> implements Iterator<E> {

        private final Iterator<E> originalIterator;
        private E current;

        public IteratorWithCurrent(Iterator<E> originalIterator) {
            this.originalIterator = originalIterator;
            try {
                current = originalIterator.next();
            } catch (NoSuchElementException e) {
                current = null;
            }
        }

        @Override
        public boolean hasNext() {
            return current != null;
        }

        @Override
        public E next() {
            if (current == null) {
                throw new NoSuchElementException();
            }
            E previous = current;
            if (originalIterator.hasNext()) {
                current = originalIterator.next();
            } else {
                current = null;
            }
            return previous;
        }

        public E current() {
            return current;
        }
    }
}
