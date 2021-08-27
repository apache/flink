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
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.checkInsertOnly;
import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.isDuplicate;
import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.updateDeduplicateResult;

/**
 * This function is used to get the first or last row for every key partition in miniBatch mode. But
 * only send latest change log to downstream.
 */
public class RowTimeMiniBatchLatestChangeDeduplicateFunction
        extends MiniBatchDeduplicateFunctionBase<RowData, RowData, RowData, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private final TypeSerializer<RowData> serializer;
    private final boolean generateUpdateBefore;
    private final boolean generateInsert;
    private final int rowtimeIndex;
    private final boolean keepLastRow;

    public RowTimeMiniBatchLatestChangeDeduplicateFunction(
            InternalTypeInfo<RowData> typeInfo,
            TypeSerializer<RowData> serializer,
            long minRetentionTime,
            int rowtimeIndex,
            boolean generateUpdateBefore,
            boolean generateInsert,
            boolean keepLastRow) {
        super(typeInfo, minRetentionTime);
        this.serializer = serializer;
        this.generateUpdateBefore = generateUpdateBefore;
        this.generateInsert = generateInsert;
        this.rowtimeIndex = rowtimeIndex;
        this.keepLastRow = keepLastRow;
    }

    @Override
    public RowData addInput(@Nullable RowData value, RowData input) throws Exception {
        if (isDuplicate(value, input, rowtimeIndex, keepLastRow)) {
            return serializer.copy(input);
        }
        return value;
    }

    @Override
    public void finishBundle(Map<RowData, RowData> buffer, Collector<RowData> out)
            throws Exception {
        for (Map.Entry<RowData, RowData> entry : buffer.entrySet()) {
            RowData currentKey = entry.getKey();
            RowData bufferedRow = entry.getValue();
            ctx.setCurrentKey(currentKey);
            RowData preRow = state.value();
            checkInsertOnly(bufferedRow);
            if (isDuplicate(preRow, bufferedRow, rowtimeIndex, keepLastRow)) {
                updateDeduplicateResult(
                        generateUpdateBefore, generateInsert, preRow, bufferedRow, out);
                state.update(bufferedRow);
            }
        }
    }
}
