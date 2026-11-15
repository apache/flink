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

package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;
import org.apache.flink.table.runtime.dataview.StateListView;
import org.apache.flink.table.runtime.dataview.StateMapView;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Process function for several event-time bounded RANGE OVER windows that share the same partition
 * and order key but have different preceding offsets.
 *
 * <p>E.g.: SELECT SUM(c) OVER (PARTITION BY b ORDER BY rowtime RANGE BETWEEN INTERVAL '10' MINUTE
 * PRECEDING AND CURRENT ROW), SUM(c) OVER (PARTITION BY b ORDER BY rowtime RANGE BETWEEN INTERVAL
 * '30' MINUTE PRECEDING AND CURRENT ROW) FROM T.
 *
 * <p>The input rows are stored once and kept for the largest offset. Each window keeps its own
 * accumulators and retracts a row when the row leaves that window's range.
 */
public class RowTimeMultiRangeBoundedPrecedingFunction<K>
        extends KeyedProcessFunction<K, RowData, RowData> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(RowTimeMultiRangeBoundedPrecedingFunction.class);

    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";

    private final GeneratedAggsHandleFunction[] genAggsHandlers;
    private final LogicalType[][] accTypes;
    private final LogicalType[] inputFieldTypes;
    private final long[] precedingOffsets;
    private final long maxPrecedingOffset;
    private final int rowTimeIdx;

    private transient JoinedRowData output;
    private transient AggsHandleFunction[] functions;
    private transient RowData[] aggValues;
    private transient JoinedRowData[] aggValueJoins;

    private transient ValueState<Long> lastTriggeringTsState;
    // the last timestamp whose rows have been aggregated and emitted; every window has retracted
    // exactly the rows that were out of its range at that timestamp
    private transient ValueState<Long> lastProcessedTsState;
    private transient ValueState<Long> cleanupTsState;
    private transient List<ValueState<RowData>> accStates;
    private transient MapState<Long, List<RowData>> inputState;

    private transient Counter numLateRecordsDropped;

    @VisibleForTesting
    Counter getCounter() {
        return numLateRecordsDropped;
    }

    public RowTimeMultiRangeBoundedPrecedingFunction(
            GeneratedAggsHandleFunction[] genAggsHandlers,
            LogicalType[][] accTypes,
            LogicalType[] inputFieldTypes,
            long[] precedingOffsets,
            int rowTimeIdx) {
        Preconditions.checkArgument(genAggsHandlers.length > 0);
        Preconditions.checkArgument(genAggsHandlers.length == accTypes.length);
        Preconditions.checkArgument(genAggsHandlers.length == precedingOffsets.length);
        this.genAggsHandlers = genAggsHandlers;
        this.accTypes = accTypes;
        this.inputFieldTypes = inputFieldTypes;
        this.precedingOffsets = precedingOffsets;
        long max = Long.MIN_VALUE;
        for (long offset : precedingOffsets) {
            max = Math.max(max, offset);
        }
        this.maxPrecedingOffset = max;
        this.rowTimeIdx = rowTimeIdx;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        final int n = genAggsHandlers.length;
        final StateDataViewStore store = new PerKeyStateDataViewStore(getRuntimeContext());
        functions = new AggsHandleFunction[n];
        accStates = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            functions[i] =
                    genAggsHandlers[i].newInstance(getRuntimeContext().getUserCodeClassLoader());
            // the generated handlers name their data view states from 0 upwards, so the states of
            // different windows would clash without a per-window prefix
            functions[i].open(new PrefixedStateDataViewStore(store, "w" + i + "$"));
            accStates.add(
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "accState_" + i,
                                            InternalTypeInfo.ofFields(accTypes[i]))));
        }
        aggValues = new RowData[n];
        aggValueJoins = new JoinedRowData[n - 1];
        for (int i = 0; i < n - 1; i++) {
            aggValueJoins[i] = new JoinedRowData();
        }
        output = new JoinedRowData();

        lastTriggeringTsState =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("lastTriggeringTsState", Types.LONG));
        lastProcessedTsState =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("lastProcessedTsState", Types.LONG));
        cleanupTsState =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("cleanupTsState", Types.LONG));

        InternalTypeInfo<RowData> inputType = InternalTypeInfo.ofFields(inputFieldTypes);
        inputState =
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        "inputState", Types.LONG, new ListTypeInfo<>(inputType)));

        numLateRecordsDropped =
                getRuntimeContext().getMetricGroup().counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
    }

    @Override
    public void processElement(
            RowData input,
            KeyedProcessFunction<K, RowData, RowData>.Context ctx,
            Collector<RowData> out)
            throws Exception {
        long triggeringTs = input.getLong(rowTimeIdx);

        Long lastTriggeringTs = lastTriggeringTsState.value();
        if (lastTriggeringTs == null) {
            lastTriggeringTs = 0L;
        }

        if (triggeringTs > lastTriggeringTs) {
            List<RowData> data = inputState.get(triggeringTs);
            if (data == null) {
                data = new ArrayList<>();
                ctx.timerService().registerEventTimeTimer(triggeringTs);
            }
            data.add(input);
            inputState.put(triggeringTs, data);
            registerCleanupTimer(ctx, triggeringTs);
        } else {
            numLateRecordsDropped.inc();
        }
    }

    private void registerCleanupTimer(
            KeyedProcessFunction<K, RowData, RowData>.Context ctx, long timestamp)
            throws Exception {
        long minCleanupTimestamp = timestamp + maxPrecedingOffset + 1;
        long maxCleanupTimestamp = timestamp + (long) (maxPrecedingOffset * 1.5) + 1;
        Long curCleanupTimestamp = cleanupTsState.value();
        if (curCleanupTimestamp == null || curCleanupTimestamp < minCleanupTimestamp) {
            ctx.timerService().registerEventTimeTimer(maxCleanupTimestamp);
            cleanupTsState.update(maxCleanupTimestamp);
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<K, RowData, RowData>.OnTimerContext ctx,
            Collector<RowData> out)
            throws Exception {
        Long cleanupTimestamp = cleanupTsState.value();
        if (cleanupTimestamp != null && cleanupTimestamp <= timestamp) {
            inputState.clear();
            for (int i = 0; i < functions.length; i++) {
                accStates.get(i).clear();
                functions[i].cleanup();
            }
            lastTriggeringTsState.clear();
            lastProcessedTsState.clear();
            cleanupTsState.clear();
            return;
        }

        List<RowData> inputs = inputState.get(timestamp);
        if (inputs != null) {
            for (int i = 0; i < functions.length; i++) {
                RowData acc = accStates.get(i).value();
                functions[i].setAccumulators(acc == null ? functions[i].createAccumulators() : acc);
            }

            // null when no row has been aggregated yet: then no window has retracted anything
            final Long lastProcessedTs = lastProcessedTsState.value();
            final List<Long> expiredTsList = new ArrayList<>();

            Iterator<Map.Entry<Long, List<RowData>>> iter = inputState.entries().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, List<RowData>> entry = iter.next();
                long dataTs = entry.getKey();
                long offset = timestamp - dataTs;
                if (offset <= 0) {
                    continue;
                }
                List<RowData> rows = entry.getValue();
                if (rows == null) {
                    LOG.warn(
                            "The state is cleared because of state ttl. "
                                    + "This will result in incorrect result. "
                                    + "You can increase the state ttl to avoid this.");
                    continue;
                }
                for (int i = 0; i < functions.length; i++) {
                    boolean outOfRangeNow = offset > precedingOffsets[i];
                    boolean inRangeBefore =
                            lastProcessedTs == null
                                    || lastProcessedTs - dataTs <= precedingOffsets[i];
                    if (outOfRangeNow && inRangeBefore) {
                        for (RowData row : rows) {
                            functions[i].retract(row);
                        }
                    }
                }
                if (offset > maxPrecedingOffset) {
                    expiredTsList.add(dataTs);
                }
            }

            for (int i = 0; i < functions.length; i++) {
                for (RowData row : inputs) {
                    functions[i].accumulate(row);
                }
                aggValues[i] = functions[i].getValue();
            }

            RowData aggValue = concatAggValues();
            for (RowData row : inputs) {
                output.replace(row, aggValue);
                out.collect(output);
            }

            for (Long ts : expiredTsList) {
                inputState.remove(ts);
            }
            for (int i = 0; i < functions.length; i++) {
                accStates.get(i).update(functions[i].getAccumulators());
            }
            lastProcessedTsState.update(timestamp);
        }
        lastTriggeringTsState.update(timestamp);
    }

    @Override
    public void close() throws Exception {
        if (functions != null) {
            for (AggsHandleFunction function : functions) {
                if (function != null) {
                    function.close();
                }
            }
        }
    }

    /** Concatenates the aggregate values of all windows, in window order. */
    private RowData concatAggValues() {
        RowData result = aggValues[0];
        for (int i = 1; i < aggValues.length; i++) {
            result = aggValueJoins[i - 1].replace(result, aggValues[i]);
        }
        return result;
    }

    /** Prefixes the names of the data view states of one window. */
    private static final class PrefixedStateDataViewStore implements StateDataViewStore {
        private final StateDataViewStore delegate;
        private final String prefix;

        private PrefixedStateDataViewStore(StateDataViewStore delegate, String prefix) {
            this.delegate = delegate;
            this.prefix = prefix;
        }

        @Override
        public <N, EK, EV> StateMapView<N, EK, EV> getStateMapView(
                String stateName,
                boolean supportNullKey,
                TypeSerializer<EK> keySerializer,
                TypeSerializer<EV> valueSerializer)
                throws Exception {
            return delegate.getStateMapView(
                    prefix + stateName, supportNullKey, keySerializer, valueSerializer);
        }

        @Override
        public <N, EE> StateListView<N, EE> getStateListView(
                String stateName, TypeSerializer<EE> elementSerializer) throws Exception {
            return delegate.getStateListView(prefix + stateName, elementSerializer);
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return delegate.getRuntimeContext();
        }
    }
}
