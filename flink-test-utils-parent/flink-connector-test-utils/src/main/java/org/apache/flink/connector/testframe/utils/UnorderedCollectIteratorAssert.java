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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.apache.flink.shaded.guava31.com.google.common.base.Predicates.not;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This assertion used to compare records in the collect iterator to the target test data with
 * different semantics (AT_LEAST_ONCE, EXACTLY_ONCE) for unordered messages.
 *
 * @param <T> The type of records in the test data and collect iterator
 */
public class UnorderedCollectIteratorAssert<T>
        extends AbstractAssert<UnorderedCollectIteratorAssert<T>, Iterator<T>> {

    private final Iterator<T> collectorIterator;
    private final Set<T> allRecords;
    private final Set<T> matchedRecords;

    private Integer limit = null;

    protected UnorderedCollectIteratorAssert(Iterator<T> collectorIterator) {
        super(collectorIterator, UnorderedCollectIteratorAssert.class);
        this.collectorIterator = collectorIterator;
        this.allRecords = new HashSet<>();
        this.matchedRecords = new HashSet<>();
    }

    public UnorderedCollectIteratorAssert<T> withNumRecordsLimit(int limit) {
        this.limit = limit;
        return this;
    }

    public void matchesRecordsFromSource(
            List<List<T>> recordsBySplitsFromSource, CheckpointingMode semantic) {
        for (List<T> list : recordsBySplitsFromSource) {
            for (T t : list) {
                assertTrue(allRecords.add(t), "All the records should be unique.");
            }
        }

        if (limit != null && limit > allRecords.size()) {
            throw new IllegalArgumentException(
                    "Limit validation size should be less than or equal to total number of records from source");
        }

        switch (semantic) {
            case AT_LEAST_ONCE:
                compareWithAtLeastOnceSemantic();
                break;
            case EXACTLY_ONCE:
                compareWithExactlyOnceSemantic();
                break;
            default:
                throw new IllegalArgumentException("Unrecognized semantic \"" + semantic + "\"");
        }
    }

    private void compareWithAtLeastOnceSemantic() {
        int recordCounter = 0;
        while (collectorIterator.hasNext()) {
            final T record = collectorIterator.next();
            if (allRecords.contains(record)) {
                if (matchedRecords.add(record)) {
                    recordCounter++;
                }
            } else {
                throw new IllegalArgumentException("Record " + record + " is not expected.");
            }

            if (limit != null && recordCounter >= limit) {
                break;
            }
        }

        verifyMatchedRecords();
    }

    private void compareWithExactlyOnceSemantic() {
        int recordCounter = 0;
        while (collectorIterator.hasNext()) {
            final T record = collectorIterator.next();
            if (allRecords.contains(record)) {
                assertTrue(
                        matchedRecords.add(record),
                        "Record " + record + " is duplicated in exactly-once.");
                recordCounter++;
            } else {
                throw new IllegalArgumentException("Record " + record + " is not expected.");
            }

            if (limit != null && recordCounter >= limit) {
                break;
            }
        }

        verifyMatchedRecords();
    }

    private void verifyMatchedRecords() {
        if (limit == null && allRecords.size() > matchedRecords.size()) {
            Set<T> missingResults =
                    allRecords.stream().filter(not(matchedRecords::contains)).collect(toSet());
            if (!missingResults.isEmpty()) {
                throw new IllegalArgumentException(
                        "Expected to have "
                                + allRecords.size()
                                + " elements. But we missing: "
                                + missingResults);
            }
        }
    }
}
