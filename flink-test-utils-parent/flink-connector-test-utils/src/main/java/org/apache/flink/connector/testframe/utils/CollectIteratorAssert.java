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

import org.apache.flink.streaming.api.CheckpointingMode;

import org.assertj.core.api.AbstractAssert;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * This assertion used to compare records in the collect iterator to the target test data with
 * different semantic(AT_LEAST_ONCE, EXACTLY_ONCE).
 *
 * @param <T> The type of records in the test data and collect iterator
 */
public class CollectIteratorAssert<T>
        extends AbstractAssert<CollectIteratorAssert<T>, Iterator<T>> {

    private final Iterator<T> collectorIterator;
    private final List<RecordsFromSplit<T>> recordsFromSplits = new ArrayList<>();
    private int totalNumRecords;
    private Integer limit = null;

    protected CollectIteratorAssert(Iterator<T> collectorIterator) {
        super(collectorIterator, CollectIteratorAssert.class);
        this.collectorIterator = collectorIterator;
    }

    public CollectIteratorAssert<T> withNumRecordsLimit(int limit) {
        this.limit = limit;
        return this;
    }

    public void matchesRecordsFromSource(
            List<List<T>> recordsBySplitsFromSource, CheckpointingMode semantic) {
        for (List<T> recordsFromSplit : recordsBySplitsFromSource) {
            recordsFromSplits.add(new RecordsFromSplit<>(recordsFromSplit));
            totalNumRecords += recordsFromSplit.size();
        }

        if (limit != null && limit > totalNumRecords) {
            throw new IllegalArgumentException(
                    "Limit validation size should be less than total number of records from source");
        }

        switch (semantic) {
            case AT_LEAST_ONCE:
                compareWithAtLeastOnceSemantic(collectorIterator, recordsFromSplits);
                break;
            case EXACTLY_ONCE:
                compareWithExactlyOnceSemantic(collectorIterator, recordsFromSplits);
                break;
            default:
                throw new IllegalArgumentException(
                        String.format("Unrecognized semantic \"%s\"", semantic));
        }
    }

    private void compareWithAtLeastOnceSemantic(
            Iterator<T> resultIterator, List<RecordsFromSplit<T>> recordsFromSplits) {
        List<T> duplicateRead = new LinkedList<>();

        int recordCounter = 0;
        while (resultIterator.hasNext()) {
            final T record = resultIterator.next();
            if (!matchThenNext(record)) {
                duplicateRead.add(record);
            } else {
                recordCounter++;
            }

            if (limit != null && recordCounter >= limit) {
                break;
            }
        }
        if (limit == null && !hasReachedEnd()) {
            failWithMessage(
                    generateMismatchDescription(
                            String.format(
                                    "Expected to have at least %d records in result, but only received %d records",
                                    recordsFromSplits.stream()
                                            .mapToInt(
                                                    recordsFromSplit ->
                                                            recordsFromSplit.records.size())
                                            .sum(),
                                    recordCounter),
                            resultIterator));
        } else {
            confirmDuplicateRead(duplicateRead);
        }
    }

    private void compareWithExactlyOnceSemantic(
            Iterator<T> resultIterator, List<RecordsFromSplit<T>> recordsFromSplits) {
        int recordCounter = 0;
        while (resultIterator.hasNext()) {
            final T record = resultIterator.next();
            if (!matchThenNext(record)) {
                if (recordCounter >= totalNumRecords) {
                    failWithMessage(
                            generateMismatchDescription(
                                    String.format(
                                            "Expected to have exactly %d records in result, but received more records",
                                            recordsFromSplits.stream()
                                                    .mapToInt(
                                                            recordsFromSplit ->
                                                                    recordsFromSplit.records.size())
                                                    .sum()),
                                    resultIterator));
                } else {
                    failWithMessage(
                            generateMismatchDescription(
                                    String.format(
                                            "Unexpected record '%s' at position %d",
                                            record, recordCounter),
                                    resultIterator));
                }
            }
            recordCounter++;
            if (limit != null && recordCounter >= limit) {
                break;
            }
        }
        if (limit == null && !hasReachedEnd()) {
            failWithMessage(
                    generateMismatchDescription(
                            String.format(
                                    "Expected to have exactly %d records in result, but only received %d records",
                                    recordsFromSplits.stream()
                                            .mapToInt(
                                                    recordsFromSplit ->
                                                            recordsFromSplit.records.size())
                                            .sum(),
                                    recordCounter),
                            resultIterator));
        }
    }

    private void confirmDuplicateRead(List<T> duplicateRead) {
        for (T record : duplicateRead) {
            boolean found = false;
            for (RecordsFromSplit<T> recordsFromSplit : recordsFromSplits) {
                if (recordsFromSplit.records.contains(record)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                failWithMessage(String.format("Unexpected duplicate record '%s'", record));
            }
        }
    }

    /**
     * Check if any pointing data is identical to the record from the stream, and move the pointer
     * to next record if matched.
     *
     * @param record Record from stream
     */
    private boolean matchThenNext(T record) {
        for (RecordsFromSplit<T> recordsFromSplit : recordsFromSplits) {
            if (!recordsFromSplit.hasNext()) {
                continue;
            }
            if (record.equals(recordsFromSplit.current())) {
                recordsFromSplit.forward();
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
        for (RecordsFromSplit<T> recordsFromSplit : recordsFromSplits) {
            if (recordsFromSplit.hasNext()) {
                return false;
            }
        }
        return true;
    }

    private String generateMismatchDescription(String reason, Iterator<T> resultIterator) {
        final StringBuilder sb = new StringBuilder();
        sb.append(reason).append("\n");
        sb.append("Current progress of multiple split test data validation:\n");
        int splitCounter = 0;
        for (RecordsFromSplit<T> recordsFromSplit : recordsFromSplits) {
            sb.append(
                    String.format(
                            "Split %d (%d/%d): \n",
                            splitCounter++,
                            recordsFromSplit.offset,
                            recordsFromSplit.records.size()));
            for (int recordIndex = 0;
                    recordIndex < recordsFromSplit.records.size();
                    recordIndex++) {
                sb.append(recordsFromSplit.records.get(recordIndex));
                if (recordIndex == recordsFromSplit.offset) {
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

    private static class RecordsFromSplit<T> {
        private int offset = 0;
        private final List<T> records;

        public RecordsFromSplit(List<T> records) {
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
