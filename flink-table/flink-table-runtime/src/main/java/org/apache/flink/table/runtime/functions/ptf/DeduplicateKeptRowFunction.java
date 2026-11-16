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
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.runtime.functions.ptf.DeduplicateUtils.SequenceComparator;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategy;
import org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategyUtils;
import org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategyUtils.Keep;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * {@link Deduplicate} with {@code keep => 'last'}, or {@code keep => 'first'} with {@code
 * sequence_by}: a later record can replace the kept one, so the kept record is held in the {@code
 * kept} state.
 */
@Internal
public class DeduplicateKeptRowFunction extends DeduplicateFunctionBase {

    private static final long serialVersionUID = 1L;

    private final Keep keep;

    private final LogicalType[] payloadTypes;

    private final boolean isUpdateBeforeEmitted;

    private final @Nullable SequenceComparator sequenceComparator;

    private transient RowData.FieldGetter[] payloadGetters;

    private transient RowDataSerializer candidateSerializer;

    private transient RowDataSerializer currentSerializer;

    private transient GenericRowData reusedCandidate;

    private transient JoinedRowData output;

    private transient RowData emptyRow;

    DeduplicateKeptRowFunction(final SpecializedContext context) {
        super(context, internalState(Map.entry("kept", keptState(context))));
        final CallContext callContext = context.getCallContext();
        this.keep = DeduplicateTypeStrategy.keep(callContext);
        this.payloadTypes =
                DeduplicateTypeStrategyUtils.outputFields(DeduplicateUtils.tableSemantics(context))
                        .stream()
                        .map(field -> field.getDataType().getLogicalType())
                        .toArray(LogicalType[]::new);
        this.isUpdateBeforeEmitted =
                callContext
                        .getOutputChangelogMode()
                        .map(mode -> mode.contains(RowKind.UPDATE_BEFORE))
                        .orElse(false);
        this.sequenceComparator = sequenceComparator(context, outputIndices);
    }

    @Override
    public void open(final FunctionContext context) throws Exception {
        super.open(context);
        payloadGetters = new RowData.FieldGetter[outputIndices.length];
        for (int i = 0; i < outputIndices.length; i++) {
            payloadGetters[i] = RowData.createFieldGetter(payloadTypes[i], outputIndices[i]);
        }
        if (sequenceComparator != null) {
            sequenceComparator.open(context);
        }
        // toBinaryRow reuses its result row, so each side of a comparison needs its own
        // serializer
        candidateSerializer = new RowDataSerializer(payloadTypes);
        currentSerializer = new RowDataSerializer(payloadTypes);
        reusedCandidate = new GenericRowData(outputIndices.length);
        output = new JoinedRowData();
        emptyRow = GenericRowData.of();
    }

    @Override
    public void close() throws Exception {
        if (sequenceComparator != null) {
            sequenceComparator.close();
        }
        super.close();
    }

    public void eval(
            final Context ctx,
            final ValueView<RowData> keptState,
            final RowData input,
            @Nullable final StringData keep,
            @Nullable final ColumnList sequenceBy,
            @Nullable final Long stateTtl,
            @Nullable final Boolean resetTtlOnDuplicate)
            throws Exception {
        final RowData kept = keptState.getValue();
        if (kept == null) {
            final BinaryRowData candidate = toCandidate(input);
            emit(RowKind.INSERT, candidate);
            keptState.setValue(candidate.copy());
            return;
        }
        final int order = sequenceComparator == null ? 0 : sequenceComparator.compare(input, kept);
        if (DeduplicateUtils.replaces(this.keep, order)) {
            final BinaryRowData candidate = toCandidate(input);
            if (!candidate.equals(currentSerializer.toBinaryRow(kept))) {
                if (isUpdateBeforeEmitted) {
                    emit(RowKind.UPDATE_BEFORE, kept);
                }
                emit(RowKind.UPDATE_AFTER, candidate);
                keptState.setValue(candidate.copy());
                return;
            }
        }
        if (isResetTtlOnDuplicate) {
            keptState.setValue(kept);
        }
    }

    /** Returns the payload of the input as a reused binary row, valid until the next call. */
    private BinaryRowData toCandidate(final RowData input) {
        for (int i = 0; i < outputIndices.length; i++) {
            reusedCandidate.setField(i, payloadGetters[i].getFieldOrNull(input));
        }
        return candidateSerializer.toBinaryRow(reusedCandidate);
    }

    /** Wraps the payload so that rows owned by state never change their row kind. */
    private void emit(final RowKind kind, final RowData payload) {
        output.replace(payload, emptyRow);
        output.setRowKind(kind);
        collect(output);
    }

    private static StateTypeStrategy keptState(final SpecializedContext context) {
        return DeduplicateUtils.stateStrategy(
                context,
                DataTypes.ROW(
                        DeduplicateTypeStrategyUtils.outputFields(
                                DeduplicateUtils.tableSemantics(context))));
    }

    private static @Nullable SequenceComparator sequenceComparator(
            final SpecializedContext context, final int[] outputIndices) {
        final int[] payloadIndices =
                DeduplicateTypeStrategy.sequenceByOutputIndices(context.getCallContext());
        if (payloadIndices.length == 0) {
            return null;
        }
        final List<DataTypes.Field> payloadFields =
                DeduplicateTypeStrategyUtils.outputFields(DeduplicateUtils.tableSemantics(context));
        final int[] inputIndices =
                Arrays.stream(payloadIndices).map(i -> outputIndices[i]).toArray();
        final DataType[] types =
                Arrays.stream(payloadIndices)
                        .mapToObj(i -> payloadFields.get(i).getDataType())
                        .toArray(DataType[]::new);
        return new SequenceComparator(context, inputIndices, payloadIndices, types);
    }
}
