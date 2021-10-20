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

package org.apache.flink.table.planner.factories.sink;

import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.factories.TestValuesRuntimeHelper;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/** A sink function to collect appended data. */
public class AppendingSinkFunction extends AbstractExactlyOnceSinkFunction {

    private static final long serialVersionUID = 1L;
    private final DynamicTableSink.DataStructureConverter converter;
    private final int rowtimeIndex;

    public AppendingSinkFunction(
            String tableName, DynamicTableSink.DataStructureConverter converter, int rowtimeIndex) {
        super(tableName);
        this.converter = converter;
        this.rowtimeIndex = rowtimeIndex;
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        RowKind kind = value.getRowKind();
        if (value.getRowKind() == RowKind.INSERT) {
            Row row = (Row) converter.toExternal(value);
            assert row != null;
            if (rowtimeIndex >= 0) {
                // currently, rowtime attribute always uses 3 precision
                TimestampData rowtime = value.getTimestamp(rowtimeIndex, 3);
                long mark = context.currentWatermark();
                if (mark > rowtime.getMillisecond()) {
                    // discard the late data
                    return;
                }
            }
            synchronized (TestValuesRuntimeHelper.LOCK) {
                localRawResult.add(kind.shortString() + "(" + row.toString() + ")");
            }
        } else {
            throw new RuntimeException(
                    "AppendingSinkFunction received " + value.getRowKind() + " messages.");
        }
    }
}
