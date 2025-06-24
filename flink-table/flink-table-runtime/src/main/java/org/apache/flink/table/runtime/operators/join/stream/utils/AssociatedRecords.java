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

package org.apache.flink.table.runtime.operators.join.stream.utils;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.asyncprocessing.state.JoinRecordAsyncStateView;
import org.apache.flink.table.runtime.operators.join.stream.asyncprocessing.state.OuterJoinRecordAsyncStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.util.IterableIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link AssociatedRecords} is the records associated to the input row. It is a wrapper of
 * {@code List<OuterRecord>} which provides two helpful methods {@link #getRecords()} and {@link
 * #getOuterRecords()}. See the method Javadoc for more details.
 */
public class AssociatedRecords {
    private final List<OuterRecord> records;

    private AssociatedRecords(List<OuterRecord> records) {
        checkNotNull(records);
        this.records = records;
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    public int size() {
        return records.size();
    }

    /**
     * Gets the iterable of records. This is usually be called when the {@link AssociatedRecords} is
     * from inner side.
     */
    public Iterable<RowData> getRecords() {
        return new RecordsIterable(records);
    }

    /**
     * Gets the iterable of {@link OuterRecord} which composites record and numOfAssociations. This
     * is usually be called when the {@link AssociatedRecords} is from outer side.
     */
    public Iterable<OuterRecord> getOuterRecords() {
        return records;
    }

    /**
     * Creates an {@link AssociatedRecords} which represents the records associated to the input
     * row.
     *
     * <p>This method is used in the sync state join operator.
     */
    public static AssociatedRecords fromSyncStateView(
            RowData input,
            boolean inputIsLeft,
            JoinRecordStateView otherSideStateView,
            JoinCondition condition)
            throws Exception {
        List<OuterRecord> associations = new ArrayList<>();
        if (otherSideStateView instanceof OuterJoinRecordStateView) {
            OuterJoinRecordStateView outerStateView = (OuterJoinRecordStateView) otherSideStateView;
            Iterable<Tuple2<RowData, Integer>> records =
                    outerStateView.getRecordsAndNumOfAssociations();
            for (Tuple2<RowData, Integer> record : records) {
                boolean matched =
                        inputIsLeft
                                ? condition.apply(input, record.f0)
                                : condition.apply(record.f0, input);
                if (matched) {
                    associations.add(new OuterRecord(record.f0, record.f1));
                }
            }
        } else {
            Iterable<RowData> records = otherSideStateView.getRecords();
            for (RowData record : records) {
                boolean matched =
                        inputIsLeft
                                ? condition.apply(input, record)
                                : condition.apply(record, input);
                if (matched) {
                    // use -1 as the default number of associations
                    associations.add(new OuterRecord(record, -1));
                }
            }
        }
        return new AssociatedRecords(associations);
    }

    /**
     * Creates an {@link AssociatedRecords} which represents the records associated to the input
     * row.
     *
     * <p>This method is used in the async state join operator.
     */
    public static StateFuture<AssociatedRecords> fromAsyncStateView(
            RowData input,
            boolean inputIsLeft,
            JoinRecordAsyncStateView otherSideAsyncStateView,
            JoinCondition joinCondition)
            throws Exception {
        Function<RowData, Boolean> conditionFunction =
                recordInState ->
                        inputIsLeft
                                ? joinCondition.apply(input, recordInState)
                                : joinCondition.apply(recordInState, input);

        if (otherSideAsyncStateView instanceof OuterJoinRecordAsyncStateView) {
            OuterJoinRecordAsyncStateView outerAsyncStateView =
                    (OuterJoinRecordAsyncStateView) otherSideAsyncStateView;
            return outerAsyncStateView
                    .findMatchedRecordsAndNumOfAssociations(conditionFunction)
                    .thenApply(AssociatedRecords::new);

        } else {
            return otherSideAsyncStateView
                    .findMatchedRecords(conditionFunction)
                    .thenApply(AssociatedRecords::new);
        }
    }

    /** A lazy Iterable which transform {@code List<OuterRecord>} to {@code Iterable<RowData>}. */
    private static final class RecordsIterable implements IterableIterator<RowData> {
        private final List<OuterRecord> records;
        private int index = 0;

        private RecordsIterable(List<OuterRecord> records) {
            this.records = records;
        }

        @Override
        public Iterator<RowData> iterator() {
            index = 0;
            return this;
        }

        @Override
        public boolean hasNext() {
            return index < records.size();
        }

        @Override
        public RowData next() {
            RowData row = records.get(index).record;
            index++;
            return row;
        }
    }
}
