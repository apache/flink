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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.deduplicate.utils.RowTimeDeduplicateFunctionHelper;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

/** This function is used to deduplicate on keys and keeps only first or last row on row time. */
public class RowTimeDeduplicateFunction
        extends SyncStateDeduplicateFunctionBase<RowData, RowData, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private final boolean generateUpdateBefore;
    private final boolean generateInsert;
    private final int rowtimeIndex;
    private final boolean keepLastRow;

    private transient SyncStateRowTimeDeduplicateFunctionHelper helper;

    public RowTimeDeduplicateFunction(
            InternalTypeInfo<RowData> typeInfo,
            long minRetentionTime,
            int rowtimeIndex,
            boolean generateUpdateBefore,
            boolean generateInsert,
            boolean keepLastRow) {
        super(typeInfo, null, minRetentionTime);
        this.generateUpdateBefore = generateUpdateBefore;
        this.generateInsert = generateInsert;
        this.rowtimeIndex = rowtimeIndex;
        this.keepLastRow = keepLastRow;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        helper = new SyncStateRowTimeDeduplicateFunctionHelper();
    }

    @Override
    public void processElement(RowData input, Context ctx, Collector<RowData> out)
            throws Exception {
        RowData prevRow = state.value();
        helper.deduplicateOnRowTime(input, prevRow, out);
    }

    private class SyncStateRowTimeDeduplicateFunctionHelper
            extends RowTimeDeduplicateFunctionHelper {

        public SyncStateRowTimeDeduplicateFunctionHelper() {
            super(generateUpdateBefore, generateInsert, rowtimeIndex, keepLastRow);
        }

        @Override
        protected void updateState(RowData currentRow) throws Exception {
            state.update(currentRow);
        }
    }
}
