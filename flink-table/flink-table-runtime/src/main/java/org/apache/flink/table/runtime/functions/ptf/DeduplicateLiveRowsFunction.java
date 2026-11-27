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
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.api.dataview.ValueView;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.functions.definitions.DeduplicateDefinition;
import org.apache.flink.table.functions.definitions.DeduplicateDefinition.Keep;
import org.apache.flink.table.runtime.functions.ptf.DeduplicateUtils.SequenceComparator;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.utils.CallContextUtils;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.functions.definitions.DeduplicateDefinition.ARG_STATE_TTL;

/**
 * {@link Deduplicate} on updating input whose PARTITION BY columns contain no upsert key: a key can
 * have several live rows. The {@code rows} state counts each live row and stamps it with its
 * arrival, the {@code current} state holds the kept row, and the {@code arrival} state numbers the
 * rows of a key. When the kept row is retracted, the next one is found among the live rows.
 */
@Internal
public class DeduplicateLiveRowsFunction extends DeduplicateUpdatingFunctionBase {

    private static final long serialVersionUID = 1L;

    private static final String ROWS_STATE = "rows";

    private static final String CURRENT_STATE = "current";

    private static final String ARRIVAL_STATE = "arrival";

    private static final int COUNT = 0;

    private static final int ARRIVAL = 1;

    private final Keep keepMode;

    private final @Nullable SequenceComparator sequenceComparator;

    DeduplicateLiveRowsFunction(final SpecializedContext context) {
        super(context, liveRowsState(context));
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
            final MapView<RowData, RowData> rows,
            final ValueView<RowData> currentState,
            final ValueView<Long> arrivalState,
            final RowData input,
            @Nullable final StringData keep,
            @Nullable final ColumnList sequenceBy,
            @Nullable final Long stateTtl)
            throws Exception {
        final BinaryRowData row = toStateRow(input);
        final RowData current = currentState.getValue();
        final RowKind kind = input.getRowKind();
        if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
            add(rows, currentState, arrivalState, row, current);
        } else {
            retract(rows, currentState, arrivalState, row, current);
        }
    }

    private void add(
            final MapView<RowData, RowData> rows,
            final ValueView<RowData> currentState,
            final ValueView<Long> arrivalState,
            final BinaryRowData row,
            @Nullable final RowData current)
            throws Exception {
        final long arrival = nextArrival(rows, arrivalState);
        arrivalState.setValue(arrival);

        final RowData entry = rows.get(row);
        final long count = entry == null ? 1L : entry.getLong(COUNT) + 1;
        final long rowArrival =
                entry == null || keepMode == Keep.LAST ? arrival : entry.getLong(ARRIVAL);
        rows.put(row, GenericRowData.of(count, rowArrival));

        final RowData currentEntry = current == null ? null : rows.get(current);
        if (currentEntry == null) {
            // No kept row, or its entry expired: choose among the remembered rows.
            selectCurrent(rows, currentState, arrivalState, current);
            return;
        }
        if (!row.equals(current) && wins(row, rowArrival, current, currentEntry.getLong(ARRIVAL))) {
            currentState.setValue(row);
            if (isUpdateBeforeEmitted) {
                emit(RowKind.UPDATE_BEFORE, current);
            }
            emit(RowKind.UPDATE_AFTER, row);
        } else if (isTtlEnabled) {
            currentState.setValue(current);
        }
    }

    private void retract(
            final MapView<RowData, RowData> rows,
            final ValueView<RowData> currentState,
            final ValueView<Long> arrivalState,
            final BinaryRowData row,
            @Nullable final RowData current)
            throws Exception {
        final boolean isCurrent = current != null && row.equals(current);
        final RowData entry = rows.get(row);
        if (entry == null) {
            // The entry of the kept row can expire before the kept row itself.
            if (isCurrent) {
                selectCurrent(rows, currentState, arrivalState, current);
            }
            return;
        }
        final long count = entry.getLong(COUNT);
        if (count > 1) {
            rows.put(row, GenericRowData.of(count - 1, entry.getLong(ARRIVAL)));
            if (isTtlEnabled && current != null) {
                currentState.setValue(current);
            }
            return;
        }
        rows.remove(row);
        if (isCurrent) {
            selectCurrent(rows, currentState, arrivalState, current);
        }
    }

    /**
     * Makes the best remembered row the kept row, in place of {@code previous}, which is no longer
     * among the remembered rows.
     */
    private void selectCurrent(
            final MapView<RowData, RowData> rows,
            final ValueView<RowData> currentState,
            final ValueView<Long> arrivalState,
            @Nullable final RowData previous)
            throws Exception {
        RowData next = null;
        long nextArrival = 0L;
        for (final Map.Entry<RowData, RowData> candidate : rows.entries()) {
            final long candidateArrival = candidate.getValue().getLong(ARRIVAL);
            if (next == null || wins(candidate.getKey(), candidateArrival, next, nextArrival)) {
                next = candidate.getKey();
                nextArrival = candidateArrival;
            }
        }
        // OPEN POINT
        //
        // note : this may be able to be optimised from O(N) -> O(log N) amourtised when we do not
        // have the stateTTL arguement, by employing the use of a heap

        // state ttl cant present in this optimisation because of issues that will arise when
        // ttl expires

        if (next == null) {
            currentState.clear();
            arrivalState.clear();
            if (previous != null) {
                emit(RowKind.DELETE, previous);
            }
            return;
        }
        final BinaryRowData nextRow = ((BinaryRowData) next).copy();
        currentState.setValue(nextRow);
        if (previous == null) {
            emit(RowKind.INSERT, nextRow);
            return;
        }
        if (isUpdateBeforeEmitted) {
            emit(RowKind.UPDATE_BEFORE, previous);
        }
        emit(RowKind.UPDATE_AFTER, nextRow);
    }

    /**
     * Returns the arrival of the next record of the key. Without a counter, for a new key or after
     * the counter expired, numbering continues after the remembered rows.
     */
    private static long nextArrival(
            final MapView<RowData, RowData> rows, final ValueView<Long> arrivalState)
            throws Exception {
        final Long lastArrival = arrivalState.getValue();
        if (lastArrival != null) {
            return lastArrival + 1;
        }
        long maxArrival = -1L;
        for (final RowData entry : rows.values()) {
            maxArrival = Math.max(maxArrival, entry.getLong(ARRIVAL));
        }
        return maxArrival + 1;
    }

    /** Returns whether row {@code a} is kept over row {@code b}, with arrival breaking ties. */
    private boolean wins(
            final RowData a, final long arrivalA, final RowData b, final long arrivalB) {
        int order = sequenceComparator == null ? 0 : sequenceComparator.compare(a, b);
        if (order == 0) {
            order = Long.compare(arrivalA, arrivalB);
        }
        return keepMode == Keep.LAST ? order > 0 : order < 0;
    }

    private static LinkedHashMap<String, StateTypeStrategy> liveRowsState(
            final SpecializedContext context) {
        final DataType rowType = inputRowType(context);
        final Duration ttl =
                CallContextUtils.getStateTtl(context.getCallContext(), ARG_STATE_TTL).orElse(null);
        final DataType entryType =
                DataTypes.ROW(
                        DataTypes.FIELD("count", DataTypes.BIGINT().notNull()),
                        DataTypes.FIELD("arrival", DataTypes.BIGINT().notNull()));
        return internalState(
                Map.entry(
                        ROWS_STATE,
                        StateTypeStrategy.of(
                                callContext ->
                                        Optional.of(MapView.newMapViewDataType(rowType, entryType)),
                                ttl)),
                Map.entry(
                        CURRENT_STATE,
                        StateTypeStrategy.of(
                                callContext -> Optional.of(ValueView.newValueViewDataType(rowType)),
                                ttl)),
                Map.entry(
                        ARRIVAL_STATE,
                        StateTypeStrategy.of(
                                callContext ->
                                        Optional.of(
                                                ValueView.newValueViewDataType(DataTypes.BIGINT())),
                                ttl)));
    }
}
