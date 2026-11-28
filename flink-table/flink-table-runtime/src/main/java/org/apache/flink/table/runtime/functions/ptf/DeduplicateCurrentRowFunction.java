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
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.functions.definitions.DeduplicateDefinition;
import org.apache.flink.table.functions.definitions.DeduplicateDefinition.Keep;
import org.apache.flink.table.runtime.functions.ptf.DeduplicateUtils.SequenceComparator;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import static org.apache.flink.table.functions.definitions.DeduplicateDefinition.ARG_STATE_TTL;

/**
 * {@link Deduplicate} with {@code keep => 'last'}, or {@code keep => 'first'} with {@code
 * sequence_by}: a later record can replace the current one, so the current input row is held in the
 * {@code kept} state.
 */
@Internal
public class DeduplicateCurrentRowFunction extends DeduplicateRowFunctionBase {

    private static final long serialVersionUID = 1L;

    private static final String CURRENT_STATE = "kept";

    private final Keep keepMode;

    private final @Nullable SequenceComparator sequenceComparator;

    DeduplicateCurrentRowFunction(final SpecializedContext context) {
        super(context, valueState(context, CURRENT_STATE, inputRowType(context), ARG_STATE_TTL));
        this.keepMode = DeduplicateDefinition.keep(context.getCallContext());
        this.sequenceComparator = DeduplicateUtils.sequenceComparator(context);
    }

    @Override
    public void open(final FunctionContext context) throws Exception {
        super.open(context);
        if (sequenceComparator != null) {
            sequenceComparator.open(context);
        }
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
        if (current == null) {
            final BinaryRowData candidate = toStateRow(input);
            currentState.setValue(candidate);
            emit(RowKind.INSERT, candidate);
            return;
        }
        if (replacesCurrent(input, current)) {
            final BinaryRowData candidate = toStateRow(input);
            if (!candidate.equals(current)) {
                currentState.setValue(candidate);
                if (isUpdateBeforeEmitted) {
                    emit(RowKind.UPDATE_BEFORE, current);
                }
                emit(RowKind.UPDATE_AFTER, candidate);
            } else if (isTtlEnabled) {
                currentState.setValue(current);
            }
        } else if (isTtlEnabled) {
            currentState.setValue(current);
        }
    }

    /**
     * Without sequence_by only keep => 'last' reaches this class, so the latest arrival replaces.
     */
    private boolean replacesCurrent(final RowData candidate, final RowData current) {
        return sequenceComparator == null
                || DeduplicateUtils.replaces(
                        keepMode, sequenceComparator.compare(candidate, current));
    }
}
