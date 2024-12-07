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

package org.apache.flink.table.runtime.operators.join.stream.asyncprocessing.state;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.stream.utils.OuterRecord;

import java.util.List;
import java.util.function.Function;

/**
 * A {@link OuterJoinRecordAsyncStateView} is an extension to {@link JoinRecordAsyncStateView}. The
 * {@link OuterJoinRecordAsyncStateView} is used to store records for the outer input side of the
 * Join, e.g. the left side of left join, the both side of full join.
 *
 * <p>The additional information we should store with the record is the number of associations which
 * is the number of records associated this record with other side. This is an important information
 * when to send/retract a null padding row, to avoid recompute the associated numbers every time.
 *
 * @see JoinRecordAsyncStateView
 */
public interface OuterJoinRecordAsyncStateView extends JoinRecordAsyncStateView {

    /**
     * Adds a new record with the number of associations to the state view.
     *
     * @param record the added record
     * @param numOfAssociations the number of records associated with other side
     */
    StateFuture<Void> addRecord(RowData record, int numOfAssociations);

    /**
     * Updates the number of associations belongs to the record.
     *
     * @param record the record to update
     * @param numOfAssociations the new number of records associated with other side
     */
    StateFuture<Void> updateNumOfAssociations(RowData record, int numOfAssociations);

    /**
     * Find all the records matched the condition and the corresponding number of associations under
     * the current context (i.e. join key).
     */
    StateFuture<List<OuterRecord>> findMatchedRecordsAndNumOfAssociations(
            Function<RowData, Boolean> condition);

    // ----------------------------------------------------------------------------------------

    @Override
    default StateFuture<Void> addRecord(RowData record) {
        return addRecord(record, -1);
    }

    @Override
    default StateFuture<List<OuterRecord>> findMatchedRecords(
            Function<RowData, Boolean> condition) {
        return findMatchedRecordsAndNumOfAssociations(condition);
    }
}
