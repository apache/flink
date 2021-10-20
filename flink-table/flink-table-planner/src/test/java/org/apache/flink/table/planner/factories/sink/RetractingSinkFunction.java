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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TestValuesRuntimeHelper;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/** A sink function to collect retracted data. */
public class RetractingSinkFunction extends AbstractExactlyOnceSinkFunction {
    private static final long serialVersionUID = 1L;

    private final DynamicTableSink.DataStructureConverter converter;

    protected transient ListState<String> retractResultState;
    protected transient List<String> localRetractResult;

    public RetractingSinkFunction(
            String tableName, DynamicTableSink.DataStructureConverter converter) {
        super(tableName);
        this.converter = converter;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        super.initializeState(context);
        this.retractResultState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>("sink-retract-results", Types.STRING));
        this.localRetractResult = new ArrayList<>();

        if (context.isRestored()) {
            for (String value : retractResultState.get()) {
                localRetractResult.add(value);
            }
        }

        int taskId = getRuntimeContext().getIndexOfThisSubtask();
        synchronized (TestValuesRuntimeHelper.LOCK) {
            TestValuesRuntimeHelper.getGlobalRetractResult()
                    .computeIfAbsent(tableName, k -> new HashMap<>())
                    .put(taskId, localRetractResult);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        super.snapshotState(context);
        retractResultState.clear();
        synchronized (TestValuesRuntimeHelper.LOCK) {
            retractResultState.addAll(localRetractResult);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void invoke(RowData value, Context context) throws Exception {
        RowKind kind = value.getRowKind();
        Row row = (Row) converter.toExternal(value);
        assert row != null;
        synchronized (TestValuesRuntimeHelper.LOCK) {
            localRawResult.add(kind.shortString() + "(" + row.toString() + ")");
            if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
                row.setKind(RowKind.INSERT);
                localRetractResult.add(row.toString());
            } else {
                row.setKind(RowKind.INSERT);
                boolean contains = localRetractResult.remove(row.toString());
                if (!contains) {
                    throw new RuntimeException(
                            "Tried to retract a value that wasn't inserted first. "
                                    + "This is probably an incorrectly implemented test.");
                }
            }
        }
    }
}
