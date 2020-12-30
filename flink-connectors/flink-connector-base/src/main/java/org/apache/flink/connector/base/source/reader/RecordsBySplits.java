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

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.api.connector.source.SourceSplit;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** An implementation of RecordsWithSplitIds to host all the records by splits. */
public class RecordsBySplits<E> implements RecordsWithSplitIds<E> {

    private final Set<String> finishedSplits;

    private final Iterator<Map.Entry<String, Collection<E>>> splitsIterator;

    @Nullable private Iterator<E> recordsInCurrentSplit;

    public RecordsBySplits(
            final Map<String, Collection<E>> recordsBySplit, final Set<String> finishedSplits) {

        this.splitsIterator = checkNotNull(recordsBySplit, "recordsBySplit").entrySet().iterator();
        this.finishedSplits = checkNotNull(finishedSplits, "finishedSplits");
    }

    @Nullable
    @Override
    public String nextSplit() {
        if (splitsIterator.hasNext()) {
            final Map.Entry<String, Collection<E>> next = splitsIterator.next();
            recordsInCurrentSplit = next.getValue().iterator();
            return next.getKey();
        } else {
            return null;
        }
    }

    @Nullable
    @Override
    public E nextRecordFromSplit() {
        if (recordsInCurrentSplit == null) {
            throw new IllegalStateException();
        }

        return recordsInCurrentSplit.hasNext() ? recordsInCurrentSplit.next() : null;
    }

    @Override
    public Set<String> finishedSplits() {
        return finishedSplits;
    }

    // ------------------------------------------------------------------------

    /**
     * A utility builder to collect records in individual calls, rather than put a finished
     * collection in the {@link RecordsBySplits#RecordsBySplits(Map, Set)} constructor.
     */
    public static class Builder<E> {

        private final Map<String, Collection<E>> recordsBySplits = new LinkedHashMap<>();
        private final Set<String> finishedSplits = new HashSet<>(2);

        /**
         * Add the record from the given split ID.
         *
         * @param splitId the split ID the record was from.
         * @param record the record to add.
         */
        public void add(String splitId, E record) {
            recordsBySplits.computeIfAbsent(splitId, sid -> new ArrayList<>()).add(record);
        }

        /**
         * Add the record from the given source split.
         *
         * @param split the source split the record was from.
         * @param record the record to add.
         */
        public void add(SourceSplit split, E record) {
            add(split.splitId(), record);
        }

        /**
         * Add multiple records from the given split ID.
         *
         * @param splitId the split ID given the records were from.
         * @param records the records to add.
         */
        public void addAll(String splitId, Collection<E> records) {
            this.recordsBySplits.compute(
                    splitId,
                    (id, r) -> {
                        if (r == null) {
                            r = records;
                        } else {
                            r.addAll(records);
                        }
                        return r;
                    });
        }

        /**
         * Add multiple records from the given source split.
         *
         * @param split the source split the records were from.
         * @param records the records to add.
         */
        public void addAll(SourceSplit split, Collection<E> records) {
            addAll(split.splitId(), records);
        }

        /**
         * Mark the split with the given ID as finished.
         *
         * @param splitId the ID of the finished split.
         */
        public void addFinishedSplit(String splitId) {
            finishedSplits.add(splitId);
        }

        /**
         * Mark multiple splits with the given IDs as finished.
         *
         * @param splitIds the IDs of the finished splits.
         */
        public void addFinishedSplits(Collection<String> splitIds) {
            finishedSplits.addAll(splitIds);
        }

        public RecordsBySplits<E> build() {
            return new RecordsBySplits<>(
                    recordsBySplits.isEmpty() ? Collections.emptyMap() : recordsBySplits,
                    finishedSplits.isEmpty() ? Collections.emptySet() : finishedSplits);
        }
    }
}
