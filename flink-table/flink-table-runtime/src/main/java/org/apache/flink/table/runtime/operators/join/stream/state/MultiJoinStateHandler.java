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

package org.apache.flink.table.runtime.operators.join.stream.state;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamOperatorStateHandler;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.Iterator;

/**
 * A simple implementation of {@link MultiJoinStateHandler} that uses a MapState to store records.
 */

// TODO Gustavo: this is working but not final state
// We need multiple state handlers/state views: with unique key and without - we're using a hard
// coded key position here
// Add state TTL
public class MultiJoinStateHandler {

    private final StreamingMultiJoinOperator operator;
    private final StreamOperatorStateHandler stateHandler;

    private final Configuration config;
    private final ClassLoader userCodeClassloader;
    private final JoinInputSideSpec inputSpec;
    private final long stateRetentionTime;

    private transient MapState<Integer, RowData> recordState;

    public MultiJoinStateHandler(
            int inputIndex,
            StreamingMultiJoinOperator operator,
            StreamOperatorStateHandler stateHandler,
            Configuration config,
            ClassLoader userCodeClassloader,
            JoinInputSideSpec inputSpec,
            long stateRetentionTime) {
        this.operator = operator;
        this.stateHandler = stateHandler;
        this.config = config;
        this.userCodeClassloader = userCodeClassloader;
        this.inputSpec = inputSpec;
        this.stateRetentionTime = stateRetentionTime;

        // Initialize state
        initializeState(inputIndex);
    }

    private void initializeState(int inputIndex) {
        MapStateDescriptor<Integer, RowData> recordStateDesc =
                new MapStateDescriptor<>(
                        "multi-join-record-state-" + inputIndex,
                        InternalTypeInfo.of(Integer.class),
                        InternalTypeInfo.of(RowData.class));

        this.operator
                .getRuntimeContext()
                .setKeyedStateStore(this.stateHandler.getKeyedStateStore().get());
        this.recordState = this.operator.getRuntimeContext().getMapState(recordStateDesc);
    }

    public Iterator<RowData> getRecords() throws Exception {
        return recordState.values().iterator();
    }

    public void addRecord(RowData record) throws Exception {
        int key = Integer.parseInt(record.getString(1).toString());
        recordState.put(key, record);
    }

    public void removeRecord(RowData record) throws Exception {
        int key = Integer.parseInt(record.getString(1).toString());
        recordState.remove(key);
    }
}
