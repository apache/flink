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
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TestValuesRuntimeHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * The sink implementation is end-to-end exactly once, so that it can be used to check the state
 * restoring in streaming sql.
 */
public abstract class AbstractExactlyOnceSinkFunction extends RichSinkFunction<RowData>
        implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;

    protected final String tableName;

    protected transient ListState<String> rawResultState;
    protected transient List<String> localRawResult;

    public AbstractExactlyOnceSinkFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.rawResultState =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>("sink-results", Types.STRING));
        this.localRawResult = new ArrayList<>();
        if (context.isRestored()) {
            for (String value : rawResultState.get()) {
                localRawResult.add(value);
            }
        }
        int taskId = getRuntimeContext().getIndexOfThisSubtask();
        synchronized (TestValuesRuntimeHelper.LOCK) {
            TestValuesRuntimeHelper.getGlobalRawResult()
                    .computeIfAbsent(tableName, k -> new HashMap<>())
                    .put(taskId, localRawResult);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        rawResultState.clear();
        synchronized (TestValuesRuntimeHelper.LOCK) {
            rawResultState.addAll(localRawResult);
        }
    }
}
