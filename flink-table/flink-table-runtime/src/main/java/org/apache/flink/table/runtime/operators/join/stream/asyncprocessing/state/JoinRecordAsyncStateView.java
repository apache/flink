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
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.utils.OuterRecord;

import java.util.List;
import java.util.function.Function;

/**
 * A {@link JoinRecordAsyncStateView} is a view to the join state. It encapsulates the join state
 * and provides some APIs facing the input records. The join state is used to store input records.
 * The structure of the join state is vary depending on the {@link JoinInputSideSpec}.
 *
 * <p>For example: when the {@link JoinInputSideSpec} is JoinKeyContainsUniqueKey, we will use
 * {@link org.apache.flink.api.common.state.v2.ValueState} to store records which has better
 * performance.
 *
 * <p>Different with {@link JoinRecordStateView}, this interface is based on async state api.
 */
public interface JoinRecordAsyncStateView {

    /** Add a new record to the state view. */
    StateFuture<Void> addRecord(RowData record);

    /** Retract the record from the state view. */
    StateFuture<Void> retractRecord(RowData record);

    /** Find all the records matched the condition under the current context (i.e. join key). */
    StateFuture<List<OuterRecord>> findMatchedRecords(Function<RowData, Boolean> condition);
}
