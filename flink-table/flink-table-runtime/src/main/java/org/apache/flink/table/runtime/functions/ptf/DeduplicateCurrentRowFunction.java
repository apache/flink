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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.functions.definitions.DeduplicateDefinition;
import org.apache.flink.table.functions.definitions.DeduplicateDefinition.Keep;
import org.apache.flink.table.runtime.functions.ptf.DeduplicateUtils.SequenceComparator;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.strategies.ChangelogTypeStrategyUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.functions.definitions.DeduplicateDefinition.ARG_STATE_TTL;

/**
 * {@link Deduplicate} with {@code keep => 'last'}, or {@code keep => 'first'} with {@code
 * sequence_by}: a later record can replace the current one, so the current input row is held in the
 * {@code kept} state.
 */
@Internal
public class DeduplicateCurrentRowFunction extends DeduplicateFunctionBase {

    private static final long serialVersionUID = 1L;

    private static final String CURRENT_STATE = "kept";

    private final Keep keepMode;

    private final LogicalType[] inputTypes;

    private final boolean isUpdateBeforeEmitted;

    private final @Nullable SequenceComparator sequenceComparator;

    private transient RowDataSerializer inputSerializer;

    private transient ProjectedRowData projectedCandidate;

    private transient ProjectedRowData projectedCurrent;

    private transient JoinedRowData retraction;

    private transient RowData emptyRow;

    DeduplicateCurrentRowFunction(final SpecializedContext context) {
        super(
                context,
                valueState(
                        context,
                        CURRENT_STATE,
                        DataTypes.ROW(
                                DataType.getFields(
                                        DeduplicateUtils.tableSemantics(context).dataType())),
                        ARG_STATE_TTL));
        this.keepMode = DeduplicateDefinition.keep(context.getCallContext());
        final List<DataTypes.Field> inputFields =
                DataType.getFields(DeduplicateUtils.tableSemantics(context).dataType());
        this.inputTypes =
                inputFields.stream()
                        .map(field -> field.getDataType().getLogicalType())
                        .toArray(LogicalType[]::new);
        this.isUpdateBeforeEmitted =
                context.getCallContext()
                        .getOutputChangelogMode()
                        .map(mode -> mode.contains(RowKind.UPDATE_BEFORE))
                        .orElse(false);
        this.sequenceComparator = sequenceComparator(context, inputFields);
    }

    @Override
    public void open(final FunctionContext context) throws Exception {
        super.open(context);
        if (sequenceComparator != null) {
            sequenceComparator.open(context);
        }
        inputSerializer = new RowDataSerializer(inputTypes);
        projectedCandidate = ProjectedRowData.from(outputIndices);
        projectedCurrent = ProjectedRowData.from(outputIndices);
        retraction = new JoinedRowData();
        emptyRow = GenericRowData.of();
    }

    public void eval(
            final Context ctx,
            final ValueView<RowData> currentState,
            final RowData input,
            @Nullable final StringData keep,
            @Nullable final ColumnList sequenceBy,
            @Nullable final Long stateTtl)
            throws Exception {

        final BinaryRowData current = (BinaryRowData) currentState.getValue();
        if (current == null) {
            final BinaryRowData candidate = inputSerializer.toBinaryRow(input);
            currentState.setValue(candidate.copy());
            emitCandidate(RowKind.INSERT, candidate);
            return;
        }
        if (replacesCurrent(input, current)) {
            final BinaryRowData candidate = inputSerializer.toBinaryRow(input);
            if (!candidate.equals(current)) {
                currentState.setValue(candidate.copy());
                if (isUpdateBeforeEmitted) {
                    emitRetraction(current);
                }
                emitCandidate(RowKind.UPDATE_AFTER, candidate);
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

    private void emitCandidate(final RowKind kind, final BinaryRowData candidate) {
        projectedCandidate.replaceRow(candidate);
        projectedCandidate.setRowKind(kind);
        collect(projectedCandidate);
    }

    /** Wraps the current row so that the row owned by state never changes its row kind. */
    private void emitRetraction(final BinaryRowData current) {
        projectedCurrent.replaceRow(current);
        retraction.replace(projectedCurrent, emptyRow);
        retraction.setRowKind(RowKind.UPDATE_BEFORE);
        collect(retraction);
    }

    private static @Nullable SequenceComparator sequenceComparator(
            final SpecializedContext context, final List<DataTypes.Field> inputFields) {
        final int[] outputIndices =
                ChangelogTypeStrategyUtils.computeOutputIndices(
                        DeduplicateUtils.tableSemantics(context));
        final int[] sequenceIndices =
                Arrays.stream(
                                DeduplicateDefinition.sequenceByOutputIndices(
                                        context.getCallContext()))
                        .map(i -> outputIndices[i])
                        .toArray();
        if (sequenceIndices.length == 0) {
            return null;
        }
        final DataType[] types =
                Arrays.stream(sequenceIndices)
                        .mapToObj(i -> inputFields.get(i).getDataType())
                        .toArray(DataType[]::new);
        return new SequenceComparator(context, sequenceIndices, types);
    }
}
