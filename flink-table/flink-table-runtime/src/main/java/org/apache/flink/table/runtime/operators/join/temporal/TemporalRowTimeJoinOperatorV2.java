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

package org.apache.flink.table.runtime.operators.join.temporal;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.sink.SortedLongSerializer;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.MathUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The operator for temporal join (FOR SYSTEM_TIME AS OF o.rowtime) on row time, it has no
 * limitation about message types of the left input and right input, this means the operator deals
 * changelog well.
 *
 * <p>For Event-time temporal join, its probe side is a regular table, its build side is a versioned
 * table, the version of versioned table can extract from the build side state. This operator works
 * by keeping on the state collection of probe and build records to process on next watermark. The
 * idea is that between watermarks we are collecting those elements and once we are sure that there
 * will be no updates we emit the correct result and clean up the expired data in state.
 *
 * <p>Probe-side records that arrive late (their event time is less than or equal to the current
 * watermark) are dropped on arrival and counted via the {@code numLateRecordsDropped} metric; they
 * are not joined or emitted (not even as null-padded results for left outer joins), because the
 * matching build-side version may already have been cleaned up.
 *
 * <p>Cleaning up the state drops all the "old" values from the probe side, where "old" is defined
 * as older than the current watermark. Build side is also cleaned up in the similar fashion,
 * however we always keep at least one record - the latest one - even if it's past the last
 * watermark.
 *
 * <p>One more trick is how the emitting results and cleaning up is triggered. It is achieved by
 * registering timers for the keys. We could register a timer for every probe and build side
 * element's event time (when watermark exceeds this timer, that's when we are emitting and/or
 * cleaning up the state). However this would cause huge number of registered timers. For example
 * with following evenTimes of probe records accumulated: {1, 2, 5, 8, 9}, if we had received
 * Watermark(10), it would trigger 5 separate timers for the same key. To avoid that we always keep
 * only one single registered timer for any given key, registered for the minimal value. Upon
 * triggering it, we process all records with event times older then or equal to currentWatermark.
 *
 * <p>Compared to {@link TemporalRowTimeJoinOperator}, this version stores the probe side keyed by
 * {@link LeftTimeIndexKey} (row time first, then arrival index) and the build side with {@link
 * SortedLongSerializer} as key serializer. Both serializations preserve numeric order under
 * unsigned lexicographic byte comparison, so state backends that iterate map state in
 * serialized-key order (RocksDB, ForSt) return the entries in ascending time order. On such
 * backends each watermark firing only reads state entries up to the watermark and stops, instead of
 * scanning the whole probe side and materializing plus sorting the whole build side. On unordered
 * backends (heap) the behavior is equivalent to {@link TemporalRowTimeJoinOperator}. The two
 * operators have incompatible state layouts.
 */
public class TemporalRowTimeJoinOperatorV2 extends BaseTwoInputStreamOperatorWithStateRetention {

    private static final long serialVersionUID = 1L;

    private static final String NEXT_LEFT_INDEX_STATE_NAME = "next-index";
    private static final String LEFT_STATE_NAME = "left";
    private static final String RIGHT_STATE_NAME = "right";
    private static final String REGISTERED_TIMER_STATE_NAME = "timer";
    private static final String TIMERS_STATE_NAME = "timers";
    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";

    private final boolean isLeftOuterJoin;
    private final InternalTypeInfo<RowData> leftType;
    private final InternalTypeInfo<RowData> rightType;
    private final GeneratedJoinCondition generatedJoinCondition;
    private final int leftTimeAttribute;
    private final int rightTimeAttribute;

    private final RowtimeComparator rightRowtimeComparator;

    /** Incremental index generator for the arrival index part of {@link #leftState}'s keys. */
    private transient ValueState<Long> nextLeftIndex;

    /**
     * Mapping from (row time, arrival index) into the left side `Row`. On ordered state backends
     * the entries are iterated in ascending (row time, arrival index) order, which allows stopping
     * at the first entry newer than the current watermark.
     */
    private transient MapState<LeftTimeIndexKey, RowData> leftState;

    /**
     * Mapping from timestamp to right side `Row`. The key serializer preserves numeric order in
     * serialized form, so on ordered state backends the entries are iterated in ascending timestamp
     * order.
     */
    private transient MapState<Long, RowData> rightState;

    // Long for correct handling of default null
    private transient ValueState<Long> registeredTimer;
    private transient TimestampedCollector<RowData> collector;
    private transient InternalTimerService<VoidNamespace> timerService;

    private transient JoinCondition joinCondition;
    private transient JoinedRowData outRow;
    private transient GenericRowData rightNullRow;

    private transient Counter numLateRecordsDropped;

    /** Whether the state backend iterates map state in ascending serialized-key order. */
    private transient boolean isOrderedStateBackend;

    public TemporalRowTimeJoinOperatorV2(
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            int leftTimeAttribute,
            int rightTimeAttribute,
            long minRetentionTime,
            long maxRetentionTime,
            boolean isLeftOuterJoin) {
        super(minRetentionTime, maxRetentionTime);
        this.leftType = leftType;
        this.rightType = rightType;
        this.generatedJoinCondition = generatedJoinCondition;
        this.leftTimeAttribute = leftTimeAttribute;
        this.rightTimeAttribute = rightTimeAttribute;
        this.rightRowtimeComparator = new RowtimeComparator(rightTimeAttribute);
        this.isLeftOuterJoin = isLeftOuterJoin;
    }

    @Override
    public void open() throws Exception {
        super.open();
        joinCondition =
                generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        joinCondition.setRuntimeContext(getRuntimeContext());
        joinCondition.open(DefaultOpenContext.INSTANCE);

        nextLeftIndex =
                getRuntimeContext()
                        .getState(
                                new ValueStateDescriptor<>(NEXT_LEFT_INDEX_STATE_NAME, Types.LONG));
        leftState =
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        LEFT_STATE_NAME,
                                        LeftTimeIndexKeySerializer.INSTANCE,
                                        InternalSerializers.create(leftType.toRowType())));
        rightState =
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        RIGHT_STATE_NAME,
                                        SortedLongSerializer.INSTANCE,
                                        InternalSerializers.create(rightType.toRowType())));
        registeredTimer =
                getRuntimeContext()
                        .getState(
                                new ValueStateDescriptor<>(
                                        REGISTERED_TIMER_STATE_NAME, Types.LONG));

        timerService =
                getInternalTimerService(TIMERS_STATE_NAME, VoidNamespaceSerializer.INSTANCE, this);

        final String backendId = getKeyedStateBackend().getBackendTypeIdentifier();
        isOrderedStateBackend =
                StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME.equals(backendId)
                        || StateBackendLoader.FORST_STATE_BACKEND_NAME.equals(backendId);

        outRow = new JoinedRowData();
        rightNullRow = new GenericRowData(rightType.toRowType().getFieldCount());
        collector = new TimestampedCollector<>(output);

        numLateRecordsDropped =
                getRuntimeContext().getMetricGroup().counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        RowData row = element.getValue();
        long leftTime = getLeftTime(row);
        if (leftTime <= timerService.currentWatermark()) {
            // The probe-side record is late. Drop it, because the matching build-side version may
            // already have been cleaned up.
            numLateRecordsDropped.inc();
            return;
        }
        leftState.put(new LeftTimeIndexKey(leftTime, getNextLeftIndex()), row);
        registerSmallestTimer(leftTime); // Timer to emit and clean up the state

        registerProcessingCleanupTimer();
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        RowData row = element.getValue();

        long rowTime = getRightTime(row);
        rightState.put(rowTime, row);
        registerSmallestTimer(rowTime); // Timer to clean up the state

        registerProcessingCleanupTimer();
    }

    @Override
    public void onEventTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {
        registeredTimer.clear();
        long lastUnprocessedTime = emitResultAndCleanUpState(timerService.currentWatermark());
        if (lastUnprocessedTime < Long.MAX_VALUE) {
            registerTimer(lastUnprocessedTime);
        }

        // if we have more state at any side, then update the timer, else clean it up.
        if (stateCleaningEnabled) {
            if (lastUnprocessedTime < Long.MAX_VALUE || !rightState.isEmpty()) {
                registerProcessingCleanupTimer();
            } else {
                cleanupLastTimer();
                nextLeftIndex.clear();
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (joinCondition != null) {
            joinCondition.close();
        }
        super.close();
    }

    /**
     * @return a row time of the oldest unprocessed probe record or Long.MaxValue, if all records
     *     have been processed.
     */
    private long emitResultAndCleanUpState(long currentWatermark) throws Exception {
        List<RowData> rightRowsSorted = getRightRowsSorted(currentWatermark);
        long lastUnprocessedTime = Long.MAX_VALUE;

        Iterator<Map.Entry<LeftTimeIndexKey, RowData>> leftIterator =
                leftState.entries().iterator();
        // the output records' order should keep same with left input records arrival order
        final Map<Long, RowData> orderedLeftRecords = new TreeMap<>();

        while (leftIterator.hasNext()) {
            Map.Entry<LeftTimeIndexKey, RowData> entry = leftIterator.next();
            LeftTimeIndexKey leftKey = entry.getKey();
            if (leftKey.timestamp <= currentWatermark) {
                orderedLeftRecords.put(leftKey.index, entry.getValue());
                leftIterator.remove();
            } else if (isOrderedStateBackend) {
                // Entries are iterated in ascending (timestamp, index) order, so the first entry
                // newer than the watermark carries the minimal remaining timestamp.
                lastUnprocessedTime = leftKey.timestamp;
                break;
            } else {
                lastUnprocessedTime = Math.min(lastUnprocessedTime, leftKey.timestamp);
            }
        }

        // iterate the triggered left records in the ascending order of the arrival index, i.e. the
        // arrival order.
        orderedLeftRecords.forEach(
                (leftSeq, leftRow) -> {
                    long leftTime = getLeftTime(leftRow);
                    Optional<RowData> rightRow = latestRightRowToJoin(rightRowsSorted, leftTime);
                    if (rightRow.isPresent() && RowDataUtil.isAccumulateMsg(rightRow.get())) {
                        if (joinCondition.apply(leftRow, rightRow.get())) {
                            collectJoinedRow(leftRow, rightRow.get());
                        } else {
                            if (isLeftOuterJoin) {
                                collectJoinedRow(leftRow, rightNullRow);
                            }
                        }
                    } else {
                        if (isLeftOuterJoin) {
                            collectJoinedRow(leftRow, rightNullRow);
                        }
                    }
                });
        orderedLeftRecords.clear();

        cleanupExpiredVersionInState(currentWatermark, rightRowsSorted);
        return lastUnprocessedTime;
    }

    private void collectJoinedRow(RowData leftSideRow, RowData rightRow) {
        outRow.setRowKind(leftSideRow.getRowKind());
        outRow.replace(leftSideRow, rightRow);
        collector.collect(outRow);
    }

    /**
     * Removes all expired version in the versioned table's state according to current watermark.
     */
    private void cleanupExpiredVersionInState(long currentWatermark, List<RowData> rightRowsSorted)
            throws Exception {
        int i = 0;
        int indexToKeep = firstIndexToKeep(currentWatermark, rightRowsSorted);
        // clean old version data that behind current watermark
        while (i < indexToKeep) {
            long rightTime = getRightTime(rightRowsSorted.get(i));
            rightState.remove(rightTime);
            i += 1;
        }
    }

    /**
     * The method to be called when a cleanup timer fires.
     *
     * @param time The timestamp of the fired timer.
     */
    @Override
    public void cleanupState(long time) {
        leftState.clear();
        rightState.clear();
        nextLeftIndex.clear();
        registeredTimer.clear();
    }

    private int firstIndexToKeep(long timerTimestamp, List<RowData> rightRowsSorted) {
        int firstIndexNewerThenTimer =
                indexOfFirstElementNewerThanTimer(timerTimestamp, rightRowsSorted);

        if (firstIndexNewerThenTimer < 0) {
            return rightRowsSorted.size() - 1;
        } else {
            return firstIndexNewerThenTimer - 1;
        }
    }

    private int indexOfFirstElementNewerThanTimer(long timerTimestamp, List<RowData> list) {
        ListIterator<RowData> iter = list.listIterator();
        while (iter.hasNext()) {
            if (getRightTime(iter.next()) > timerTimestamp) {
                return iter.previousIndex();
            }
        }
        return -1;
    }

    /**
     * Binary search {@code rightRowsSorted} to find the latest right row to join with {@code
     * leftTime}. Latest means a right row with largest time that is still smaller or equal to
     * {@code leftTime}. For example with: rightState = [1(+I), 4(+U), 7(+U), 9(-D), 12(I)],
     *
     * <p>If left time is 6, the valid period should be [4, 7), data 4(+U) should be joined.
     *
     * <p>If left time is 10, the valid period should be [9, 12), but data 9(-D) is a DELETE message
     * which means the correspond version has no data in period [9, 12), data 9(-D) should not be
     * correlated.
     *
     * @return found element or {@code Optional.empty} If such row was not found (either {@code
     *     rightRowsSorted} is empty or all {@code rightRowsSorted} are are newer).
     */
    private Optional<RowData> latestRightRowToJoin(List<RowData> rightRowsSorted, long leftTime) {
        return latestRightRowToJoin(rightRowsSorted, 0, rightRowsSorted.size() - 1, leftTime);
    }

    private Optional<RowData> latestRightRowToJoin(
            List<RowData> rightRowsSorted, int low, int high, long leftTime) {
        if (low > high) {
            // exact value not found, we are returning largest from the values smaller then leftTime
            if (low - 1 < 0) {
                return Optional.empty();
            } else {
                return Optional.of(rightRowsSorted.get(low - 1));
            }
        } else {
            int mid = (low + high) >>> 1;
            RowData midRow = rightRowsSorted.get(mid);
            long midTime = getRightTime(midRow);
            int cmp = Long.compare(midTime, leftTime);
            if (cmp < 0) {
                return latestRightRowToJoin(rightRowsSorted, mid + 1, high, leftTime);
            } else if (cmp > 0) {
                return latestRightRowToJoin(rightRowsSorted, low, mid - 1, leftTime);
            } else {
                return Optional.of(midRow);
            }
        }
    }

    private void registerSmallestTimer(long timestamp) throws IOException {
        Long currentRegisteredTimer = registeredTimer.value();
        if (currentRegisteredTimer == null) {
            registerTimer(timestamp);
        } else if (currentRegisteredTimer > timestamp) {
            timerService.deleteEventTimeTimer(VoidNamespace.INSTANCE, currentRegisteredTimer);
            registerTimer(timestamp);
        }
    }

    private void registerTimer(long timestamp) throws IOException {
        registeredTimer.update(timestamp);
        timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, timestamp);
    }

    private List<RowData> getRightRowsSorted(long currentWatermark) throws Exception {
        List<RowData> rightRows = new ArrayList<>();
        if (isOrderedStateBackend) {
            for (Map.Entry<Long, RowData> entry : rightState.entries()) {
                if (entry.getKey() > currentWatermark) {
                    break;
                }
                rightRows.add(entry.getValue());
            }
        } else {
            for (RowData row : rightState.values()) {
                rightRows.add(row);
            }
            rightRows.sort(rightRowtimeComparator);
        }
        return rightRows;
    }

    private long getNextLeftIndex() throws IOException {
        Long index = nextLeftIndex.value();
        if (index == null) {
            index = 0L;
        }
        nextLeftIndex.update(index + 1);
        return index;
    }

    private long getLeftTime(RowData leftRow) {
        return leftRow.getLong(leftTimeAttribute);
    }

    private long getRightTime(RowData rightRow) {
        return rightRow.getLong(rightTimeAttribute);
    }

    // ------------------------------------------------------------------------------------------

    private static class RowtimeComparator implements Comparator<RowData>, Serializable {

        private static final long serialVersionUID = 1L;

        private final int timeAttribute;

        private RowtimeComparator(int timeAttribute) {
            this.timeAttribute = timeAttribute;
        }

        @Override
        public int compare(RowData o1, RowData o2) {
            long o1Time = o1.getLong(timeAttribute);
            long o2Time = o2.getLong(timeAttribute);
            return Long.compare(o1Time, o2Time);
        }
    }

    /**
     * Key of {@link #leftState}: the row time of the probe record first, then a per-key arrival
     * index to keep records with the same row time distinct and to restore arrival order at
     * emission time.
     */
    public static final class LeftTimeIndexKey {

        private final long timestamp;
        private final long index;

        public LeftTimeIndexKey(long timestamp, long index) {
            this.timestamp = timestamp;
            this.index = index;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public long getIndex() {
            return index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LeftTimeIndexKey that = (LeftTimeIndexKey) o;
            return timestamp == that.timestamp && index == that.index;
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(timestamp);
            return 31 * result + Long.hashCode(index);
        }

        @Override
        public String toString() {
            return "LeftTimeIndexKey{timestamp=" + timestamp + ", index=" + index + '}';
        }
    }

    /**
     * Serializer for {@link LeftTimeIndexKey} that produces a lexicographically sortable byte
     * representation.
     *
     * @see SortedLongSerializer
     */
    public static final class LeftTimeIndexKeySerializer
            extends TypeSerializerSingleton<LeftTimeIndexKey> {

        private static final long serialVersionUID = 1L;

        /** Sharable instance of the LeftTimeIndexKeySerializer. */
        public static final LeftTimeIndexKeySerializer INSTANCE = new LeftTimeIndexKeySerializer();

        private static final LeftTimeIndexKey ZERO = new LeftTimeIndexKey(0L, 0L);

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public LeftTimeIndexKey createInstance() {
            return ZERO;
        }

        @Override
        public LeftTimeIndexKey copy(LeftTimeIndexKey from) {
            return from;
        }

        @Override
        public LeftTimeIndexKey copy(LeftTimeIndexKey from, LeftTimeIndexKey reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return 2 * Long.BYTES;
        }

        @Override
        public void serialize(LeftTimeIndexKey record, DataOutputView target) throws IOException {
            target.writeLong(MathUtils.flipSignBit(record.timestamp));
            target.writeLong(MathUtils.flipSignBit(record.index));
        }

        @Override
        public LeftTimeIndexKey deserialize(DataInputView source) throws IOException {
            long timestamp = MathUtils.flipSignBit(source.readLong());
            long index = MathUtils.flipSignBit(source.readLong());
            return new LeftTimeIndexKey(timestamp, index);
        }

        @Override
        public LeftTimeIndexKey deserialize(LeftTimeIndexKey reuse, DataInputView source)
                throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
        }

        @Override
        public TypeSerializerSnapshot<LeftTimeIndexKey> snapshotConfiguration() {
            return new LeftTimeIndexKeySerializerSnapshot();
        }

        /** Serializer configuration snapshot for compatibility and format evolution. */
        @SuppressWarnings("WeakerAccess")
        public static final class LeftTimeIndexKeySerializerSnapshot
                extends SimpleTypeSerializerSnapshot<LeftTimeIndexKey> {

            public LeftTimeIndexKeySerializerSnapshot() {
                super(() -> INSTANCE);
            }
        }
    }

    @VisibleForTesting
    static String getNextLeftIndexStateName() {
        return NEXT_LEFT_INDEX_STATE_NAME;
    }

    @VisibleForTesting
    static String getRegisteredTimerStateName() {
        return REGISTERED_TIMER_STATE_NAME;
    }

    @VisibleForTesting
    Counter getNumLateRecordsDropped() {
        return numLateRecordsDropped;
    }

    @VisibleForTesting
    boolean isOrderedStateBackend() {
        return isOrderedStateBackend;
    }
}
