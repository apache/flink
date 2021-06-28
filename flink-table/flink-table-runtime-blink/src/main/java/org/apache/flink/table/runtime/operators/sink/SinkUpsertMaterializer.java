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

package org.apache.flink.table.runtime.operators.sink;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.TableStreamOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;

/**
 * A operator that maintains the records corresponding to the upsert keys in the state, it receives
 * the upstream changelog records and generate an upsert view for the downstream.
 *
 * <ul>
 *   <li>For insert record, append the state and collect current record.
 *   <li>For delete record, delete in the state, collect delete record when the state is empty.
 *   <li>For delete record, delete in the state, collect the last one when the state is not empty.
 * </ul>
 */
public class SinkUpsertMaterializer extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SinkUpsertMaterializer.class);

    private static final String STATE_CLEARED_WARN_MSG =
            "The state is cleared because of state ttl. This will result in incorrect result. "
                    + "You can increase the state ttl to avoid this.";

    private final StateTtlConfig ttlConfig;
    private final TypeSerializer<RowData> serializer;
    private final GeneratedRecordEqualiser generatedEqualiser;

    private transient RecordEqualiser equaliser;
    private transient ValueState<List<RowData>> state;
    private transient TimestampedCollector<RowData> collector;

    public SinkUpsertMaterializer(
            StateTtlConfig ttlConfig,
            TypeSerializer<RowData> serializer,
            GeneratedRecordEqualiser generatedEqualiser) {
        this.ttlConfig = ttlConfig;
        this.serializer = serializer;
        this.generatedEqualiser = generatedEqualiser;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.equaliser =
                generatedEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());
        ValueStateDescriptor<List<RowData>> descriptor =
                new ValueStateDescriptor<>("values", new ListSerializer<>(serializer));
        if (ttlConfig.isEnabled()) {
            descriptor.enableTimeToLive(ttlConfig);
        }
        this.state = getRuntimeContext().getState(descriptor);
        this.collector = new TimestampedCollector<>(output);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData row = element.getValue();
        boolean isInsertOp = row.getRowKind() == INSERT || row.getRowKind() == UPDATE_AFTER;
        // Always set the RowKind to INSERT, so that we can compare rows correctly (RowKind will
        // be ignored)
        row.setRowKind(INSERT);
        List<RowData> values = state.value();
        if (values == null) {
            values = new ArrayList<>(2);
        }

        if (isInsertOp) {
            values.add(row);
            // Update to this new one
            collector.collect(row);
        } else {
            int lastIndex = values.size() - 1;
            int index = removeFirst(values, row);
            if (index == -1) {
                LOG.info(STATE_CLEARED_WARN_MSG);
                return;
            }
            if (values.isEmpty()) {
                // Delete this row
                row.setRowKind(DELETE);
                collector.collect(row);
            } else if (index == lastIndex) {
                // Last one removed
                // Update to newer
                collector.collect(values.get(values.size() - 1));
            }
        }

        if (values.isEmpty()) {
            state.clear();
        } else {
            state.update(values);
        }
    }

    private int removeFirst(List<RowData> values, RowData remove) {
        Iterator<RowData> iterator = values.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            RowData row = iterator.next();
            if (equaliser.equals(row, remove)) {
                iterator.remove();
                return i;
            }
            i++;
        }
        return -1;
    }
}
