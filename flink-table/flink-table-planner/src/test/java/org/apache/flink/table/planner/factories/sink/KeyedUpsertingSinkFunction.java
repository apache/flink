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

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TestValuesRuntimeHelper;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.RowUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * NOTE: This class should use a global map to store upsert values. Just like other external
 * databases.
 */
public class KeyedUpsertingSinkFunction extends AbstractExactlyOnceSinkFunction {
    private static final long serialVersionUID = 1L;
    private final DynamicTableSink.DataStructureConverter converter;
    private final int[] keyIndices;
    private final int expectedSize;

    // [key, value] map result
    private transient Map<String, String> localUpsertResult;
    private transient int receivedNum;

    public KeyedUpsertingSinkFunction(
            String tableName,
            DynamicTableSink.DataStructureConverter converter,
            int[] keyIndices,
            int expectedSize) {
        super(tableName);
        this.converter = converter;
        this.keyIndices = keyIndices;
        this.expectedSize = expectedSize;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        super.initializeState(context);

        synchronized (TestValuesRuntimeHelper.LOCK) {
            // always store in a single map, global upsert
            this.localUpsertResult =
                    TestValuesRuntimeHelper.getGlobalUpsertResult()
                            .computeIfAbsent(tableName, k -> new HashMap<>())
                            .computeIfAbsent(0, k -> new HashMap<>());
        }
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        RowKind kind = value.getRowKind();

        Row row = (Row) converter.toExternal(value);
        assert row != null;

        synchronized (TestValuesRuntimeHelper.LOCK) {
            if (RowUtils.USE_LEGACY_TO_STRING) {
                localRawResult.add(kind.shortString() + "(" + row + ")");
            } else {
                localRawResult.add(row.toString());
            }

            row.setKind(RowKind.INSERT);
            Row key = Row.project(row, keyIndices);

            if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
                localUpsertResult.put(key.toString(), row.toString());
            } else {
                String oldValue = localUpsertResult.remove(key.toString());
                if (oldValue == null) {
                    throw new RuntimeException(
                            "Tried to delete a value that wasn't inserted first. "
                                    + "This is probably an incorrectly implemented test.");
                }
            }
            receivedNum++;
            if (expectedSize != -1 && receivedNum == expectedSize) {
                // some sources are infinite (e.g. kafka),
                // we throw a SuccessException to indicate job is finished.
                throw new SuccessException();
            }
        }
    }
}
