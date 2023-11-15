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

package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.collector.ListenableCollector;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The lookup join handler which holds a {@link LookupJoinRunner} to process lookup for insert or
 * update_after record and directly process delete and update_before record via local state.
 */
public class KeyedLookupJoinWrapper extends KeyedProcessFunction<RowData, RowData, RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(KeyedLookupJoinWrapper.class);
    private static final String STATE_CLEARED_WARN_MSG =
            "The state is cleared because of state ttl. "
                    + "This will result in incorrect result. You can increase the state ttl to avoid this.";

    private final LookupJoinRunner lookupJoinRunner;
    private final StateTtlConfig ttlConfig;
    private final TypeSerializer<RowData> serializer;
    private final boolean lookupKeyContainsPrimaryKey;

    // TODO to be unified by FLINK-24666
    private final boolean lenient = true;
    private transient BinaryRowData emptyRow;
    // for which !lookupKeyContainsPrimaryKey
    private transient ValueState<List<RowData>> state;

    // for which lookupKeyContainsPrimaryKey
    private transient ValueState<RowData> uniqueState;

    private transient FetchedRecordListener collectListener;

    public KeyedLookupJoinWrapper(
            LookupJoinRunner lookupJoinRunner,
            StateTtlConfig ttlConfig,
            TypeSerializer<RowData> serializer,
            boolean lookupKeyContainsPrimaryKey) {
        this.lookupJoinRunner = lookupJoinRunner;
        this.ttlConfig = ttlConfig;
        this.serializer = serializer;
        this.lookupKeyContainsPrimaryKey = lookupKeyContainsPrimaryKey;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        lookupJoinRunner.setRuntimeContext(getRuntimeContext());
        lookupJoinRunner.open(DefaultOpenContext.INSTANCE);

        if (lookupKeyContainsPrimaryKey) {
            ValueStateDescriptor<RowData> valueStateDescriptor =
                    new ValueStateDescriptor<>("unique-value", serializer);
            if (ttlConfig.isEnabled()) {
                valueStateDescriptor.enableTimeToLive(ttlConfig);
            }
            uniqueState = getRuntimeContext().getState(valueStateDescriptor);
        } else {
            ValueStateDescriptor<List<RowData>> valueStateDescriptor =
                    new ValueStateDescriptor<>("values", new ListSerializer<>(serializer));
            state = getRuntimeContext().getState(valueStateDescriptor);
            if (ttlConfig.isEnabled()) {
                valueStateDescriptor.enableTimeToLive(ttlConfig);
            }
        }
        emptyRow = initEmptyRow(lookupJoinRunner.tableFieldsCount);
        collectListener = new FetchedRecordListener();
        lookupJoinRunner.collector.setCollectListener(collectListener);
    }

    private BinaryRowData initEmptyRow(int arity) {
        BinaryRowData emptyRow = new BinaryRowData(arity);
        int size = emptyRow.getFixedLengthPartSize();
        byte[] bytes = new byte[size];
        emptyRow.pointTo(MemorySegmentFactory.wrap(bytes), 0, size);
        for (int index = 0; index < arity; index++) {
            emptyRow.setNullAt(index);
        }
        return emptyRow;
    }

    @Override
    public void processElement(
            RowData in,
            KeyedProcessFunction<RowData, RowData, RowData>.Context ctx,
            Collector<RowData> out)
            throws Exception {

        lookupJoinRunner.prepareCollector(in, out);
        collectListener.reset();

        // do lookup for acc msg
        if (RowDataUtil.isAccumulateMsg(in)) {
            // clear local state first
            deleteState();

            // fetcher has copied the input field when object reuse is enabled
            lookupJoinRunner.doFetch(in);

            // update state with empty row if lookup miss or pre-filtered
            if (!collectListener.collected) {
                updateState(emptyRow);
            }

            lookupJoinRunner.padNullForLeftJoin(in, out);
        } else {
            // do state access for non-acc msg
            if (lookupKeyContainsPrimaryKey) {
                RowData rightRow = uniqueState.value();
                // should distinguish null from empty(lookup miss)
                if (null == rightRow) {
                    stateStaledErrorHandle(in, out);
                } else {
                    collectDeleteRow(in, rightRow, out);
                }
            } else {
                List<RowData> rightRows = state.value();
                if (null == rightRows) {
                    stateStaledErrorHandle(in, out);
                } else {
                    for (RowData row : rightRows) {
                        collectDeleteRow(in, row, out);
                    }
                }
            }
            // clear state at last
            deleteState();
        }
    }

    private void collectDeleteRow(RowData in, RowData right, Collector<RowData> out) {
        lookupJoinRunner.outRow.replace(in, right);
        lookupJoinRunner.outRow.setRowKind(RowKind.DELETE);
        out.collect(lookupJoinRunner.outRow);
    }

    @Override
    public void close() throws Exception {
        lookupJoinRunner.close();
        super.close();
    }

    void deleteState() {
        if (lookupKeyContainsPrimaryKey) {
            uniqueState.clear();
        } else {
            state.clear();
        }
    }

    void updateState(RowData row) {
        try {
            if (lookupKeyContainsPrimaryKey) {
                uniqueState.update(row);
            } else {
                // This can be optimized if lookupFunction can be unified collecting a collection of
                // rows instead of collecting each row
                List<RowData> rows = state.value();
                if (null == rows) {
                    rows = new ArrayList<>();
                }
                rows.add(row);
                state.update(rows);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to update state!", e);
        }
    }

    class FetchedRecordListener implements ListenableCollector.CollectListener<RowData> {
        boolean collected;

        void reset() {
            collected = false;
        }

        @Override
        public void onCollect(RowData record) {
            collected = true;
            if (null == record) {
                updateState(emptyRow);
            } else {
                updateState(record);
            }
        }
    }

    private void stateStaledErrorHandle(RowData in, Collector out) {
        if (lenient) {
            LOG.warn(STATE_CLEARED_WARN_MSG);
            if (lookupJoinRunner.isLeftOuterJoin) {
                lookupJoinRunner.padNullForLeftJoin(in, out);
            }
        } else {
            throw new RuntimeException(STATE_CLEARED_WARN_MSG);
        }
    }
}
