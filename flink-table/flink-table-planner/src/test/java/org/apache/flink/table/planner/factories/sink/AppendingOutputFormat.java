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

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TestValuesRuntimeHelper;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/** An outputFormat to collect appended data. */
public class AppendingOutputFormat extends RichOutputFormat<RowData> {

    private static final long serialVersionUID = 1L;
    private final String tableName;
    private final DynamicTableSink.DataStructureConverter converter;

    protected transient List<String> localRawResult;

    public AppendingOutputFormat(
            String tableName, DynamicTableSink.DataStructureConverter converter) {
        this.tableName = tableName;
        this.converter = converter;
    }

    @Override
    public void configure(Configuration parameters) {
        // nothing to do
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.localRawResult = new ArrayList<>();
        synchronized (TestValuesRuntimeHelper.LOCK) {
            TestValuesRuntimeHelper.getGlobalRawResult()
                    .computeIfAbsent(tableName, k -> new HashMap<>())
                    .put(taskNumber, localRawResult);
        }
    }

    @Override
    public void writeRecord(RowData value) throws IOException {
        RowKind kind = value.getRowKind();
        if (value.getRowKind() == RowKind.INSERT) {
            Row row = (Row) converter.toExternal(value);
            assert row != null;
            synchronized (TestValuesRuntimeHelper.LOCK) {
                localRawResult.add(kind.shortString() + "(" + row.toString() + ")");
            }
        } else {
            throw new RuntimeException(
                    "AppendingOutputFormat received " + value.getRowKind() + " messages.");
        }
    }

    @Override
    public void close() throws IOException {
        // nothing to do
    }
}
