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

package org.apache.flink.table.runtime.operators.deduplicate;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.processLastRowOnChangelog;
import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.processLastRowOnProcTime;

/** This function is used to get the last row for every key partition in miniBatch mode. */
public class ProcTimeMiniBatchDeduplicateKeepLastRowFunction
        extends MiniBatchDeduplicateFunctionBase<RowData, RowData, RowData, RowData, RowData> {

    private static final long serialVersionUID = -8981813609115029119L;
    private final TypeSerializer<RowData> serializer;
    private final boolean generateUpdateBefore;
    private final boolean generateInsert;
    private final boolean inputInsertOnly;
    private final boolean isStateTtlEnabled;
    // The code generated equaliser used to equal RowData.
    private final GeneratedRecordEqualiser genRecordEqualiser;

    // The record equaliser used to equal RowData.
    private transient RecordEqualiser equaliser;

    public ProcTimeMiniBatchDeduplicateKeepLastRowFunction(
            InternalTypeInfo<RowData> typeInfo,
            TypeSerializer<RowData> serializer,
            long stateRetentionTime,
            boolean generateUpdateBefore,
            boolean generateInsert,
            boolean inputInsertOnly,
            GeneratedRecordEqualiser genRecordEqualiser) {
        super(typeInfo, stateRetentionTime);
        this.serializer = serializer;
        this.generateUpdateBefore = generateUpdateBefore;
        this.generateInsert = generateInsert;
        this.inputInsertOnly = inputInsertOnly;
        this.genRecordEqualiser = genRecordEqualiser;
        this.isStateTtlEnabled = stateRetentionTime > 0;
    }

    @Override
    public void open(ExecutionContext ctx) throws Exception {
        super.open(ctx);
        equaliser =
                genRecordEqualiser.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
    }

    @Override
    public RowData addInput(@Nullable RowData value, RowData input) {
        // always put the input into buffer
        return serializer.copy(input);
    }

    @Override
    public void finishBundle(Map<RowData, RowData> buffer, Collector<RowData> out)
            throws Exception {
        for (Map.Entry<RowData, RowData> entry : buffer.entrySet()) {
            RowData currentKey = entry.getKey();
            RowData currentRow = entry.getValue();
            ctx.setCurrentKey(currentKey);
            if (inputInsertOnly) {
                processLastRowOnProcTime(
                        currentRow,
                        generateUpdateBefore,
                        generateInsert,
                        state,
                        out,
                        isStateTtlEnabled,
                        equaliser);
            } else {
                processLastRowOnChangelog(
                        currentRow, generateUpdateBefore, state, out, isStateTtlEnabled, equaliser);
            }
        }
    }
}
