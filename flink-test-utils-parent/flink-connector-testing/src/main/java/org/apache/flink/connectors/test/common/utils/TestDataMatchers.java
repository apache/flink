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
import java.util.Iterator;
import java.util.List;

/** Matchers for validating test data. */
@Internal
public class TestDataMatchers {

    // ----------------------------  Matcher Builders ----------------------------------
    public static <T> MultipleSplitDataMatcher<T> matchesMultipleSplitTestData(
            List<List<T>> testRecordsLists) {
        return new MultipleSplitDataMatcher<>(testRecordsLists);
    }

    public static <T> SingleSplitDataMatcher<T> matchesSplitTestData(List<T> testData) {
        return new SingleSplitDataMatcher<>(testData);
    }

    public static <T> SingleSplitDataMatcher<T> matchesSplitTestData(List<T> testData, int limit) {
        return new SingleSplitDataMatcher<>(testData, limit);
    }

    // ---------------------------- Matcher Definitions --------------------------------

    /**
     * Matcher for validating test data in a single split.
     *
     * @param <T> Type of validating record
     */
    public static class SingleSplitDataMatcher<T> extends TypeSafeDiagnosingMatcher<Iterator<T>> {
        private static final int UNSET = -1;

        private final List<T> testData;
        private final int limit;

        private String mismatchDescription = null;

        public SingleSplitDataMatcher(List<T> testData) {
            this.testData = testData;
            this.limit = UNSET;
        }

        public SingleSplitDataMatcher(List<T> testData, int limit) {
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
                    mismatchDescription =
                            String.format(
                                    "Expected to have %d records in result, but only received %d records",
                                    limit == UNSET ? testData.size() : limit, recordCounter);
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
                mismatchDescription =
                        String.format(
                                "Expected to have exactly %d records in result, "
                                        + "but result iterator hasn't reached the end",
                                testData.size());
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
     * <p>Each list has a pointer (iterator) pointing to current checking record. When a record is
     * received in the stream, it will be compared to all current pointing records in lists, and the
     * pointer to the identical record will move forward.
     *
     * <p>If the stream preserves the correctness and order of records in all splits, all pointers
     * should reach the end of the list finally.
     *
     * @param <T> Type of validating record
     */
    public static class MultipleSplitDataMatcher<T> extends TypeSafeDiagnosingMatcher<Iterator<T>> {

        List<TestRecords<T>> testRecordsLists = new ArrayList<>();

        private String mismatchDescription = null;

        public MultipleSplitDataMatcher(List<List<T>> testData) {
            for (List<T> testRecordsList : testData) {
                this.testRecordsLists.add(new TestRecords<>(testRecordsList));
            }
        }

        @Override
        protected boolean matchesSafely(Iterator<T> resultIterator, Description description) {
            if (mismatchDescription != null) {
                description.appendText(mismatchDescription);
                return false;
            }

            int recordCounter = 0;
            while (resultIterator.hasNext()) {
                final T record = resultIterator.next();
                if (!matchThenNext(record)) {
                    this.mismatchDescription =
                            generateMismatchDescription(
                                    String.format(
                                            "Unexpected record '%s' at position %d",
                                            record, recordCounter),
                                    resultIterator);
                    return false;
                }
                recordCounter++;
            }

            if (!hasReachedEnd()) {
                this.mismatchDescription =
                        generateMismatchDescription(
                                String.format(
                                        "Expected to have exactly %d records in result, but only received %d records",
                                        testRecordsLists.stream()
                                                .mapToInt(list -> list.records.size())
                                                .sum(),
                                        recordCounter),
                                resultIterator);
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
            for (TestRecords<T> testRecordsList : testRecordsLists) {
                if (!testRecordsList.hasNext()) {
                    continue;
                }
                if (record.equals(testRecordsList.current())) {
                    testRecordsList.forward();
                    return true;
                }
            }
            return false;
        }

        /**
         * Whether all pointers have reached the end of lists.
         *
         * @return True if all pointers have reached the end.
         */
        private boolean hasReachedEnd() {
            for (TestRecords<T> testRecordsList : testRecordsLists) {
                if (testRecordsList.hasNext()) {
                    return false;
                }
            }
            return true;
        }

        String generateMismatchDescription(String reason, Iterator<T> resultIterator) {
            final StringBuilder sb = new StringBuilder();
            sb.append(reason).append("\n");
            sb.append("Current progress of multiple split test data validation:\n");
            int splitCounter = 0;
            for (TestRecords<T> testRecordsList : testRecordsLists) {
                sb.append(
                        String.format(
                                "Split %d (%d/%d): \n",
                                splitCounter++,
                                testRecordsList.offset,
                                testRecordsList.records.size()));
                for (int recordIndex = 0;
                        recordIndex < testRecordsList.records.size();
                        recordIndex++) {
                    sb.append(testRecordsList.records.get(recordIndex));
                    if (recordIndex == testRecordsList.offset) {
                        sb.append("\t<----");
                    }
                    sb.append("\n");
                }
            }
            if (resultIterator.hasNext()) {
                sb.append("Remaining received elements after the unexpected one: \n");
                while (resultIterator.hasNext()) {
                    sb.append(resultIterator.next()).append("\n");
                }
            }
            return sb.toString();
        }

        private static class TestRecords<T> {
            private int offset = 0;
            private final List<T> records;

            public TestRecords(List<T> records) {
                this.records = records;
            }

            public T current() {
                if (!hasNext()) {
                    return null;
                }
                return records.get(offset);
            }

            public void forward() {
                ++offset;
            }

            public boolean hasNext() {
                return offset < records.size();
            }
        }
    }
}
