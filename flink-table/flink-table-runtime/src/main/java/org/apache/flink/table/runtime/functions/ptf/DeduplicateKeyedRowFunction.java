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
import org.apache.flink.table.api.dataview.ValueView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import static org.apache.flink.table.functions.definitions.DeduplicateDefinition.ARG_STATE_TTL;

/**
 * {@link Deduplicate} on updating input whose PARTITION BY columns are an upsert key, when the
 * output needs a -U or a full -D that the input does not carry. A key has at most one live row,
 * which is held in the {@code current} state to produce them. The input carries no -U.
 */
@Internal
public class DeduplicateKeyedRowFunction extends DeduplicateUpdatingFunctionBase {

    private static final long serialVersionUID = 1L;

    private static final String CURRENT_STATE = "current";

    DeduplicateKeyedRowFunction(final SpecializedContext context) {
        super(context, valueState(context, CURRENT_STATE, inputRowType(context), ARG_STATE_TTL));
    }

    public void eval(
            final Context ctx,
            final ValueView<RowData> currentState,
            final RowData input,
            @Nullable final StringData keep,
            @Nullable final ColumnList sequenceBy,
            @Nullable final Long stateTtl)
            throws Exception {
        final RowData current = currentState.getValue();
        if (input.getRowKind() == RowKind.DELETE) {
            if (current != null) {
                currentState.clear();
                emit(RowKind.DELETE, current);
            }
            return;
        }
        final BinaryRowData row = toStateRow(input);
        currentState.setValue(row);
        if (current == null) {
            emit(RowKind.INSERT, row);
        } else if (!row.equals(current)) {
            if (isUpdateBeforeEmitted) {
                emit(RowKind.UPDATE_BEFORE, current);
            }
            emit(RowKind.UPDATE_AFTER, row);
        }
    }
}
