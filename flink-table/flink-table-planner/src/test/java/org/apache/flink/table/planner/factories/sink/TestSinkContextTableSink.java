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

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;

/** A TableSink used for testing the implementation of {@link SinkFunction.Context}. */
public class TestSinkContextTableSink implements DynamicTableSink {

    public static final List<Long> ROWTIMES = new ArrayList<>();

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // clear ROWTIMES first
        synchronized (ROWTIMES) {
            ROWTIMES.clear();
        }
        SinkFunction<RowData> sinkFunction =
                new SinkFunction<RowData>() {
                    private static final long serialVersionUID = -4871941979714977824L;

                    @Override
                    public void invoke(RowData value, Context context) throws Exception {
                        synchronized (ROWTIMES) {
                            ROWTIMES.add(context.timestamp());
                        }
                    }
                };
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new TestSinkContextTableSink();
    }

    @Override
    public String asSummaryString() {
        return "TestSinkContextTableSink";
    }
}
