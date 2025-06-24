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

package org.apache.flink.table.runtime.operators.window.tvf.operator;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.GroupWindowAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowAggOperator;

import java.time.ZoneId;
import java.util.Collection;

import static org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills;

/**
 * The operator for aligned window table function.
 *
 * <p>See more details about aligned window and unaligned window in {@link WindowAggOperator}.
 *
 * <p>Note: The operator only applies for Window TVF with row semantics (e.g TUMBLE/HOP/CUMULATE)
 * instead of set semantics (e.g SESSION).
 *
 * <p>The operator emits result per record instead of at the end of window.
 */
public class AlignedWindowTableFunctionOperator extends WindowTableFunctionOperatorBase {

    private static final long serialVersionUID = 1L;

    public AlignedWindowTableFunctionOperator(
            GroupWindowAssigner<TimeWindow> windowAssigner,
            int rowtimeIndex,
            ZoneId shiftTimeZone) {
        super(windowAssigner, rowtimeIndex, shiftTimeZone);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData inputRow = element.getValue();
        long timestamp;
        if (windowAssigner.isEventTime()) {
            if (inputRow.isNullAt(rowtimeIndex)) {
                // null timestamp would be dropped
                numNullRowTimeRecordsDropped.inc();
                return;
            }
            timestamp = inputRow.getTimestamp(rowtimeIndex, 3).getMillisecond();
        } else {
            timestamp = getProcessingTimeService().getCurrentProcessingTime();
        }
        timestamp = toUtcTimestampMills(timestamp, shiftTimeZone);
        Collection<TimeWindow> elementWindows = windowAssigner.assignWindows(inputRow, timestamp);
        collect(inputRow, elementWindows);
    }
}
