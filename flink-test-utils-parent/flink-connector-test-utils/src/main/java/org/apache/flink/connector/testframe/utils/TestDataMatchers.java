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

package org.apache.flink.connector.testframe.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.CheckpointingMode;

import org.assertj.core.api.Condition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/** Matchers for validating test data. */
@Internal
public class TestDataMatchers {

    // ----------------------------  Matcher Builders ----------------------------------
    public static <T> MultipleSplitDataMatcher<T> matchesMultipleSplitTestData(
            List<List<T>> testRecordsLists, CheckpointingMode semantic) {
        return new MultipleSplitDataMatcher<>(testRecordsLists, semantic);
    }

    public static <T> MultipleSplitDataMatcher<T> matchesMultipleSplitTestData(
            List<List<T>> testRecordsLists,
            CheckpointingMode semantic,
            boolean testDataAllInResult) {
        return new MultipleSplitDataMatcher<>(
                testRecordsLists, MultipleSplitDataMatcher.UNSET, semantic, testDataAllInResult);
    }

    public static <T> MultipleSplitDataMatcher<T> matchesMultipleSplitTestData(
            List<List<T>> testRecordsLists, Integer limit, CheckpointingMode semantic) {
        if (limit == null) {
            return new MultipleSplitDataMatcher<>(testRecordsLists, semantic);
        }
        return new MultipleSplitDataMatcher<>(testRecordsLists, limit, semantic);
    }

    // ---------------------------- Matcher Definitions --------------------------------
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
    public static class MultipleSplitDataMatcher<T> extends Condition<Iterator<T>> {
        private static final Logger LOG = LoggerFactory.getLogger(MultipleSplitDataMatcher.class);

        private static final int UNSET = -1;

        List<TestRecords<T>> testRecordsLists = new ArrayList<>();

        private List<List<T>> testData;
        private String mismatchDescription = null;
        private final int limit;
        private final int testDataSize;
        private final CheckpointingMode semantic;
        private final boolean testDataAllInResult;

        public MultipleSplitDataMatcher(List<List<T>> testData, CheckpointingMode semantic) {
            this(testData, UNSET, semantic);
        }

        public MultipleSplitDataMatcher(
                List<List<T>> testData, int limit, CheckpointingMode semantic) {
            this(testData, limit, semantic, true);
        }

        public MultipleSplitDataMatcher(
                List<List<T>> testData,
                int limit,
                CheckpointingMode semantic,
                boolean testDataAllInResult) {
            super();
            int allSize = 0;
            for (List<T> testRecordsList : testData) {
                this.testRecordsLists.add(new TestRecords<>(testRecordsList));
                allSize += testRecordsList.size();
            }

            if (limit > allSize) {
                throw new IllegalArgumentException(
                        "Limit validation size should be less than number of test records");
            }
            this.testDataAllInResult = testDataAllInResult;
            this.testData = testData;
            this.semantic = semantic;
            this.testDataSize = allSize;
            this.limit = limit;
        }

        @Override
        public boolean matches(Iterator<T> resultIterator) {
            if (CheckpointingMode.AT_LEAST_ONCE.equals(semantic)) {
                return matchAtLeastOnce(resultIterator);
            }
            return matchExactlyOnce(resultIterator);
        }

        protected boolean matchExactlyOnce(Iterator<T> resultIterator) {
            int recordCounter = 0;
            while (resultIterator.hasNext()) {
                final T record = resultIterator.next();
                if (!matchThenNext(record)) {
                    if (recordCounter >= testDataSize) {
                        this.mismatchDescription =
                                generateMismatchDescription(
                                        String.format(
                                                "Expected to have exactly %d records in result, but received more records",
                                                testRecordsLists.stream()
                                                        .mapToInt(list -> list.records.size())
                                                        .sum()),
                                        resultIterator);
                    } else {
                        this.mismatchDescription =
                                generateMismatchDescription(
                                        String.format(
                                                "Unexpected record '%s' at position %d",
                                                record, recordCounter),
                                        resultIterator);
                    }
                    logError();
                    return false;
                }
                recordCounter++;

                if (limit != UNSET && recordCounter >= limit) {
                    break;
                }
            }
            if (limit == UNSET && !hasReachedEnd()) {
                this.mismatchDescription =
                        generateMismatchDescription(
                                String.format(
                                        "Expected to have exactly %d records in result, but only received %d records",
                                        testRecordsLists.stream()
                                                .mapToInt(list -> list.records.size())
                                                .sum(),
                                        recordCounter),
                                resultIterator);
                logError();
                return false;
            } else {
                return true;
            }
        }

        protected boolean matchAtLeastOnce(Iterator<T> resultIterator) {
            List<T> duplicateRead = new LinkedList<>();

            int recordCounter = 0;
            while (resultIterator.hasNext()) {
                final T record = resultIterator.next();
                if (!matchThenNext(record)) {
                    duplicateRead.add(record);
                } else {
                    recordCounter++;
                }

                if (limit != UNSET && recordCounter >= limit) {
                    break;
                }
            }
            if (limit == UNSET && !hasReachedEnd()) {
                this.mismatchDescription =
                        generateMismatchDescription(
                                String.format(
                                        "Expected to have at least %d records in result, but only received %d records",
                                        testRecordsLists.stream()
                                                .mapToInt(list -> list.records.size())
                                                .sum(),
                                        recordCounter),
                                resultIterator);
                logError();
                return false;
            } else {
                if (testDataAllInResult) {
                    return confirmDuplicateRead(duplicateRead);
                }
                return true;
            }
        }

        private boolean confirmDuplicateRead(List<T> duplicateRead) {
            for (T record : duplicateRead) {
                boolean found = false;
                for (List<T> collection : testData) {
                    if (collection.contains(record)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    this.mismatchDescription =
                            String.format("Unexpected duplicate record '%s'", record);
                    logError();
                    return false;
                }
            }
            return true;
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

        private void logError() {
            LOG.error(
                    "Records consumed by Flink should be identical to test data "
                            + "and preserve the order in multiple splits");
            LOG.error(mismatchDescription);
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
