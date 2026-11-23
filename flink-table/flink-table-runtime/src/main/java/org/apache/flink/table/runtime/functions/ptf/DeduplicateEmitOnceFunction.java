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

package org.apache.flink.table.runtime.functions.ptf;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.ValueView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import static org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategy.ARG_STATE_TTL;

/**
 * {@link Deduplicate} with {@code keep => 'first'} and no {@code sequence_by}: no later record can
 * replace the first one, so only whether the key has emitted is kept in the {@code emitted} state.
 */
@Internal
public class DeduplicateEmitOnceFunction extends DeduplicateFunctionBase {

    private static final long serialVersionUID = 1L;

    private static final String EMITTED_STATE = "emitted";

    private transient ProjectedRowData projectedOutput;

    DeduplicateEmitOnceFunction(final SpecializedContext context) {
        super(
                context,
                valueState(context, EMITTED_STATE, DataTypes.BOOLEAN().notNull(), ARG_STATE_TTL));
    }

    @Override
    public void open(final FunctionContext context) throws Exception {
        super.open(context);
        projectedOutput = ProjectedRowData.from(outputIndices);
    }

    public void eval(
            final Context ctx,
            final ValueView<Boolean> emitted,
            final RowData input,
            @Nullable final StringData keep,
            @Nullable final ColumnList sequenceBy,
            @Nullable final Long stateTtl)
            throws Exception {
        if (emitted.getValue() == null) {
            projectedOutput.replaceRow(input);
            projectedOutput.setRowKind(RowKind.INSERT);
            collect(projectedOutput);
            emitted.setValue(true);
        } else if (isTtlEnabled) {
            emitted.setValue(true);
        }
    }
}
