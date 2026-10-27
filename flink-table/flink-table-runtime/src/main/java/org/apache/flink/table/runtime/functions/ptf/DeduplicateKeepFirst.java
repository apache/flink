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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.ValueView;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.runtime.functions.BuiltInSpecializedFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.strategies.ChangelogTypeStrategyUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.types.inference.strategies.DeduplicateKeepFirstTypeStrategy.ARG_INPUT;
import static org.apache.flink.table.types.inference.strategies.DeduplicateKeepFirstTypeStrategy.ARG_STATE_TTL;

/**
 * Implementation of the {@code DEDUPLICATE_KEEP_FIRST} process table function.
 *
 * <p>Specializes into {@link ProcessingTime} or {@link EventTime} depending on whether the input
 * declares a time column, because only event time needs to buffer a candidate row.
 */
@Internal
public class DeduplicateKeepFirst extends BuiltInSpecializedFunction {

    public DeduplicateKeepFirst() {
        super(BuiltInFunctionDefinitions.DEDUPLICATE_KEEP_FIRST);
    }

    @Override
    public UserDefinedFunction specialize(final SpecializedContext context) {
        return tableSemantics(context).timeColumn() == -1
                ? new ProcessingTime(context)
                : new EventTime(context);
    }

    private static TableSemantics tableSemantics(final SpecializedContext context) {
        return context.getCallContext()
                .getTableSemantics(ARG_INPUT)
                .orElseThrow(() -> new IllegalStateException("Table argument expected."));
    }

    private static StateTypeStrategy seenState(final SpecializedContext context) {
        return StateTypeStrategy.of(
                callContext ->
                        Optional.of(ValueView.newValueViewDataType(DataTypes.BOOLEAN().notNull())),
                context.getCallContext()
                        .getArgumentValue(ARG_STATE_TTL, Duration.class)
                        .orElse(null));
    }

    private static StateTypeStrategy candidateState(final SpecializedContext context) {
        final TableSemantics tableSemantics = tableSemantics(context);
        final List<DataTypes.Field> inputFields = DataType.getFields(tableSemantics.dataType());
        final DataType payload =
                DataTypes.ROW(
                        Arrays.stream(
                                        ChangelogTypeStrategyUtils.computeOutputIndices(
                                                tableSemantics))
                                .mapToObj(inputFields::get)
                                .toArray(DataTypes.Field[]::new));
        final DataType candidate =
                DataTypes.ROW(
                        DataTypes.FIELD("payload", payload),
                        DataTypes.FIELD("event_time", DataTypes.BIGINT()));
        // The candidate buffer is cleared by its timer, not by retention; disable its TTL so a
        // global table.exec.state.ttl cannot expire it before the timer fires.
        return StateTypeStrategy.of(
                callContext -> Optional.of(ValueView.newValueViewDataType(candidate)),
                Duration.ZERO);
    }

    private abstract static class Base extends BuiltInProcessTableFunction<RowData> {

        private static final long serialVersionUID = 1L;

        protected final int[] outputIndices;

        protected final RowType inputRowType;

        Base(
                final SpecializedContext context,
                final LinkedHashMap<String, StateTypeStrategy> stateStrategies) {
            super(BuiltInFunctionDefinitions.DEDUPLICATE_KEEP_FIRST, context, stateStrategies);
            final TableSemantics tableSemantics = tableSemantics(context);
            this.outputIndices = ChangelogTypeStrategyUtils.computeOutputIndices(tableSemantics);
            this.inputRowType = (RowType) tableSemantics.dataType().getLogicalType();
        }
    }

    /** Processing-time mode: emits the first arriving row per key. */
    @Internal
    public static class ProcessingTime extends Base {

        private static final long serialVersionUID = 1L;

        private transient ProjectedRowData projectedOutput;

        public ProcessingTime(final SpecializedContext context) {
            super(context, internalState(Map.entry("seen", seenState(context))));
        }

        @Override
        public void open(final FunctionContext context) throws Exception {
            super.open(context);
            projectedOutput = ProjectedRowData.from(outputIndices);
        }

        public void eval(
                final Context ctx,
                final ValueView<Boolean> seen,
                final RowData input,
                @Nullable final Long stateTtl,
                @Nullable final Boolean resetTtlOnDuplicate)
                throws Exception {
            // stateTtl is applied as the state's TTL at planning time.
            final RowKind kind = input.getRowKind();
            if (kind == RowKind.UPDATE_BEFORE || kind == RowKind.DELETE) {
                return;
            }

            if (seen.getValue() == null) {
                projectedOutput.replaceRow(input);
                projectedOutput.setRowKind(RowKind.INSERT);
                collect(projectedOutput);
                seen.setValue(true);
            } else if (resetTtlOnDuplicate == null || resetTtlOnDuplicate) {
                seen.setValue(true);
            }
        }
    }

    /** Event-time mode: emits the row with the earliest event time per key. */
    @Internal
    public static class EventTime extends Base {

        private static final long serialVersionUID = 1L;

        private static final int CANDIDATE_PAYLOAD = 0;

        private static final int CANDIDATE_EVENT_TIME = 1;

        private transient RowData.FieldGetter[] payloadGetters;

        private transient TypeSerializer<Object>[] payloadSerializers;

        public EventTime(final SpecializedContext context) {
            super(
                    context,
                    internalState(
                            Map.entry("seen", seenState(context)),
                            Map.entry("candidate", candidateState(context))));
        }

        @Override
        public void open(final FunctionContext context) throws Exception {
            super.open(context);
            payloadGetters = new RowData.FieldGetter[outputIndices.length];
            payloadSerializers = new TypeSerializer[outputIndices.length];
            for (int i = 0; i < outputIndices.length; i++) {
                final LogicalType payloadType = inputRowType.getTypeAt(outputIndices[i]);
                payloadGetters[i] = RowData.createFieldGetter(payloadType, outputIndices[i]);
                payloadSerializers[i] = InternalSerializers.create(payloadType);
            }
        }

        public void eval(
                final Context ctx,
                final ValueView<Boolean> seen,
                final ValueView<RowData> candidate,
                final RowData input,
                @Nullable final Long stateTtl,
                @Nullable final Boolean resetTtlOnDuplicate)
                throws Exception {
            final TimeContext<Long> context = ctx.timeContext(Long.class);
            final Long rowtime = context.time();
            final Long watermark = context.tableWatermark();
            if (watermark != null && rowtime <= watermark) {
                return;
            }

            if (seen.getValue() == null) {
                final RowData currLowest = candidate.getValue();
                final Long currentLowestEventTime =
                        currLowest == null ? null : currLowest.getLong(CANDIDATE_EVENT_TIME);
                if (currentLowestEventTime == null || rowtime < currentLowestEventTime) {
                    if (currentLowestEventTime != null) {
                        context.clearTimer(currentLowestEventTime);
                    }
                    // object reuse may overwrite the input's memory before the timer fires
                    candidate.setValue(materializeCandidate(input, rowtime));
                    context.registerOnTime(rowtime);
                }
            } else if (resetTtlOnDuplicate == null || resetTtlOnDuplicate) {
                seen.setValue(true);
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
            collect(currLowest.getRow(CANDIDATE_PAYLOAD, outputIndices.length));
            candidate.clear();
            seen.setValue(true);
        }

        private GenericRowData materializeCandidate(final RowData input, final long rowtime) {
            final GenericRowData payload = new GenericRowData(outputIndices.length);
            for (int i = 0; i < outputIndices.length; i++) {
                final Object field = payloadGetters[i].getFieldOrNull(input);
                payload.setField(i, field == null ? null : payloadSerializers[i].copy(field));
            }
            return GenericRowData.of(payload, rowtime);
        }
    }
}
