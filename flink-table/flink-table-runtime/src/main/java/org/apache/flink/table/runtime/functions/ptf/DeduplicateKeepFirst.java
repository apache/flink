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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.dataview.ValueView;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.strategies.ChangelogTypeStrategyUtils;
import org.apache.flink.table.types.inference.strategies.DeduplicateKeepFirstTypeStrategy;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.stream.IntStream;

/** Implementation of the {@code DEDUPLICATE_KEEP_FIRST} process table function. */
@Internal
public class DeduplicateKeepFirst extends BuiltInProcessTableFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private final int[] outputIndices;

    private final int eventTimeIndex;

    private final RowType inputRowType;

    private transient ProjectedRowData projectedOutput;

    private transient ProjectedRowData emitProjection;

    private transient RowData.FieldGetter[] payloadGetters;

    private transient TypeSerializer<RowData> candidateSerializer;

    public DeduplicateKeepFirst(final SpecializedContext context) {
        super(BuiltInFunctionDefinitions.DEDUPLICATE_KEEP_FIRST, context);
        final CallContext callContext = context.getCallContext();
        final TableSemantics tableSemantics =
                callContext
                        .getTableSemantics(DeduplicateKeepFirstTypeStrategy.ARG_INPUT)
                        .orElseThrow(() -> new IllegalStateException("Table argument expected."));
        this.outputIndices = ChangelogTypeStrategyUtils.computeOutputIndices(tableSemantics);
        this.eventTimeIndex = outputIndices.length;
        this.inputRowType = (RowType) tableSemantics.dataType().getLogicalType();
    }

    @Override
    public void open(final FunctionContext context) throws Exception {
        super.open(context);
        projectedOutput = ProjectedRowData.from(outputIndices);
        emitProjection = ProjectedRowData.from(IntStream.range(0, eventTimeIndex).toArray());
        payloadGetters = new RowData.FieldGetter[outputIndices.length];
        for (int i = 0; i < outputIndices.length; i++) {
            payloadGetters[i] =
                    RowData.createFieldGetter(
                            inputRowType.getTypeAt(outputIndices[i]), outputIndices[i]);
        }

        final LogicalType[] candidateTypes = new LogicalType[eventTimeIndex + 1];
        for (int i = 0; i < eventTimeIndex; i++) {
            candidateTypes[i] = inputRowType.getTypeAt(outputIndices[i]);
        }
        candidateTypes[eventTimeIndex] = new BigIntType();
        candidateSerializer = InternalSerializers.create(RowType.of(candidateTypes));
    }

    public void eval(
            final Context ctx,
            final ValueView<Boolean> seen,
            final ValueView<RowData> candidate,
            final RowData input,
            @Nullable final Long stateTtl,
            @Nullable final Boolean resetTtlOnDuplicate)
            throws Exception {
        // state_ttl is applied at plan time via getTimeToLive(); the PTF codegen still requires
        // a parameter for every declared argument, so it is unused here.
        final RowKind kind = input.getRowKind();
        if (kind == RowKind.UPDATE_BEFORE || kind == RowKind.DELETE) {
            return;
        }

        final TimeContext<Long> context = ctx.timeContext(Long.class);
        final Long rowtime = context.time();

        if (rowtime != null) { // watermark mode
            final Long watermark = context.tableWatermark();
            if (watermark != null && rowtime < watermark) {
                return;
            }

            if (seen.getValue() == null) { // not yet emitted → buffer the earliest candidate
                final RowData currLowest = candidate.getValue();
                final Long currentLowestEventTime =
                        currLowest == null ? null : currLowest.getLong(eventTimeIndex);
                if (currentLowestEventTime == null || rowtime < currentLowestEventTime) {
                    if (currentLowestEventTime != null) {
                        context.clearTimer(currentLowestEventTime);
                    }
                    // object reuse may overwrite the input's memory before the timer fires
                    candidate.setValue(
                            candidateSerializer.copy(materializeCandidate(input, rowtime)));
                    context.registerOnTime(rowtime);
                }
            } else { // already emitted → drop, optionally refresh the TTL
                if (resetTtlOnDuplicate == null || resetTtlOnDuplicate) {
                    seen.setValue(true);
                }
            }

        } else { // watermarkless mode
            if (seen.getValue() == null) {
                projectedOutput.replaceRow(input);
                projectedOutput.setRowKind(RowKind.INSERT);
                collect(projectedOutput);
                seen.setValue(true);
            } else {
                if (resetTtlOnDuplicate == null || resetTtlOnDuplicate) {
                    seen.setValue(true);
                }
            }
        }
    }

    public void onTimer(
            final OnTimerContext ctx,
            final ValueView<Boolean> seen,
            final ValueView<RowData> candidate)
            throws Exception {
        final RowData currLowest = candidate.getValue();
        if (currLowest == null) {
            return;
        }
        emitProjection.replaceRow(currLowest);
        collect(emitProjection);
        candidate.clear();
        seen.setValue(true);
    }

    private GenericRowData materializeCandidate(final RowData input, final long rowtime) {
        final GenericRowData row = new GenericRowData(eventTimeIndex + 1);
        for (int i = 0; i < eventTimeIndex; i++) {
            row.setField(i, payloadGetters[i].getFieldOrNull(input));
        }
        row.setField(eventTimeIndex, rowtime);
        return row;
    }
}
