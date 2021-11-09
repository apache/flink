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

package org.apache.flink.table.runtime.operators.join.interval;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig;

/**
 * A CoProcessFunction to execute time interval (time-bounded) stream semi/anti-join. Two kinds of
 * time criteria: "L.time between R.time + X and R.time + Y" or "R.time between L.time - Y and
 * L.time - X" X and Y might be negative or positive and X <= Y.
 */
public abstract class SemiAntiTimeIntervalJoin
        extends KeyedCoProcessFunction<RowData, RowData, RowData, RowData> {

    protected static final String LEFT_RECORDS_STATE_NAME = "left-records";
    protected static final String RIGHT_RECORDS_STATE_NAME = "right-records";

    private final boolean isAntiJoin;

    protected final long leftRelativeSize;
    protected final long rightRelativeSize;

    protected final long allowedLateness;
    private final InternalTypeInfo<RowData> leftType;
    private final InternalTypeInfo<RowData> rightType;
    private final IntervalJoinFunction joinFunction;
    private final long stateRetentionTime;

    // state to store rows from the left stream
    private transient SemiAntiLeftRecordStateView leftRecordStateView;
    // state to store rows from the right stream
    private transient SemiAntiRightRecordStateView rightRecordStateView;

    // Current time on the respective input stream.
    protected long leftOperatorTime = 0L;
    protected long rightOperatorTime = 0L;

    public SemiAntiTimeIntervalJoin(
            boolean isAntiJoin,
            long leftLowerBound,
            long leftUpperBound,
            long allowedLateness,
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            IntervalJoinFunction joinFunction,
            long stateRetentionTime) {
        this.isAntiJoin = isAntiJoin;
        this.leftRelativeSize = -leftLowerBound;
        this.rightRelativeSize = leftUpperBound;
        if (allowedLateness < 0) {
            throw new IllegalArgumentException("The allowed lateness must be non-negative.");
        }
        this.allowedLateness = allowedLateness;
        this.leftType = leftType;
        this.rightType = rightType;
        this.joinFunction = joinFunction;
        this.stateRetentionTime = stateRetentionTime;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        joinFunction.setRuntimeContext(getRuntimeContext());
        joinFunction.open(parameters);

        // TODO: support joins with unique keys
        // now states only support no unique key
        this.leftRecordStateView =
                new SemiAntiLeftRecordStateView(
                        getRuntimeContext(),
                        LEFT_RECORDS_STATE_NAME,
                        leftType,
                        createTtlConfig(stateRetentionTime));

        this.rightRecordStateView =
                new SemiAntiRightRecordStateView(
                        getRuntimeContext(),
                        RIGHT_RECORDS_STATE_NAME,
                        rightType,
                        createTtlConfig(stateRetentionTime));
    }

    @Override
    public void processElement1(RowData leftRow, Context ctx, Collector<RowData> out)
            throws Exception {
        updateOperatorTime(ctx);

        long timeForLeftRow = getTimeForLeftStream(ctx, leftRow);
        long rightQualifiedLowerBound = timeForLeftRow - rightRelativeSize;
        long rightQualifiedUpperBound = timeForLeftRow + leftRelativeSize;

        List<RowDataInfo> associatedRecords =
                getRowDataAssociatedWithOtherInput(
                        leftRow,
                        true,
                        joinFunction.getJoinCondition(),
                        rightQualifiedLowerBound,
                        rightQualifiedUpperBound);

        if (associatedRecords.isEmpty()) {
            if (isAntiJoin) {
                out.collect(leftRow);
            }
        } else {
            if (!isAntiJoin) {
                out.collect(leftRow);
            }
        }

        leftRecordStateView.addRecord(timeForLeftRow, leftRow, associatedRecords.size());
    }

    @Override
    public void processElement2(RowData rightRow, Context ctx, Collector<RowData> out)
            throws Exception {
        updateOperatorTime(ctx);

        long timeForRightRow = getTimeForRightStream(ctx, rightRow);
        long leftQualifiedLowerBound = timeForRightRow - leftRelativeSize;
        long leftQualifiedUpperBound = timeForRightRow + rightRelativeSize;

        List<RowDataInfo> associatedRecords =
                getRowDataAssociatedWithOtherInput(
                        rightRow,
                        false,
                        joinFunction.getJoinCondition(),
                        leftQualifiedLowerBound,
                        leftQualifiedUpperBound);

        if (associatedRecords.size() != 0) {
            for (RowDataInfo leftDataInfo : associatedRecords) {
                RowData leftData = leftDataInfo.record;
                RowKind originRowKind = leftData.getRowKind();
                // now this is the first right row that can match this left row
                if (leftDataInfo.numOfAssociations == 0) {
                    // If this is AntiJoin, we should delete the left row that has been output,
                    // else it's SemiJoin, and just output this left row.
                    if (isAntiJoin) {
                        leftData.setRowKind(RowKind.DELETE);
                    }
                    out.collect(leftData);
                }
                // ignore when numOfAssociations > 0
                // because "numOfAssociations > 0" means the leftData has been output before

                // revert the rowKind to add the row to state
                leftData.setRowKind(originRowKind);
                leftRecordStateView.updateNumOfAssociations(
                        leftDataInfo.time, leftData, leftDataInfo.numOfAssociations + 1);
            }
        } // if associatedRecords.size() is 0, do nothing

        rightRecordStateView.addRecord(timeForRightRow, rightRow);
    }

    /**
     * Update the operator time of the two streams. Must be the first call in all processing methods
     * (i.e., processElement(), onTimer()).
     *
     * @param ctx the context to acquire watermarks
     */
    abstract void updateOperatorTime(Context ctx);

    /**
     * Return the time with the target row from the left stream.
     *
     * @param ctx the runtime context
     * @param row the target row
     * @return time for the target row
     */
    abstract long getTimeForLeftStream(Context ctx, RowData row);

    /**
     * Return the time with the target row from the left stream.
     *
     * @param ctx the runtime context
     * @param row the target row
     * @return time for the target row
     */
    abstract long getTimeForRightStream(Context ctx, RowData row);

    private List<RowDataInfo> getRowDataAssociatedWithOtherInput(
            RowData input,
            boolean inputIsLeft,
            JoinCondition joinCondition,
            long lowerBoundTime,
            long upperBoundTime)
            throws Exception {
        List<RowDataInfo> result = new ArrayList<>();

        if (inputIsLeft) {
            for (long rightTime : rightRecordStateView.getAllTimes()) {
                if (rightTime < lowerBoundTime || rightTime > upperBoundTime) {
                    continue;
                }
                List<RowData> rightRowDataList = rightRecordStateView.getRecords(rightTime);
                for (RowData rightRowData : rightRowDataList) {
                    boolean matched = joinCondition.apply(input, rightRowData);
                    if (matched) {
                        result.add(RowDataInfo.of(rightRowData));
                    }
                }
            }
        } else {
            for (long leftTime : leftRecordStateView.getAllTimes()) {
                if (leftTime < lowerBoundTime || leftTime > upperBoundTime) {
                    continue;
                }
                List<Tuple2<RowData, Integer>> leftRowDataList =
                        leftRecordStateView.getRecordsAndNumOfAssociations(leftTime);
                for (Tuple2<RowData, Integer> leftRowData : leftRowDataList) {
                    boolean matched = joinCondition.apply(leftRowData.f0, input);
                    if (matched) {
                        result.add(RowDataInfo.of(leftTime, leftRowData.f0, leftRowData.f1));
                    }
                }
            }
        }

        return result;
    }

    private static final class SemiAntiLeftRecordStateView {
        // store records in the mapping like:
        // <event-time / proc-time, <record, <frequency, associated-num>>>
        private final MapState<Long, Map<RowData, Tuple2<Integer, Integer>>> recordState;

        private SemiAntiLeftRecordStateView(
                RuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType,
                StateTtlConfig ttlConfig) {

            MapTypeInfo<RowData, Tuple2<Integer, Integer>> mapTypeInfo =
                    new MapTypeInfo<>(recordType, new TupleTypeInfo<>(Types.INT, Types.INT));
            MapStateDescriptor<Long, Map<RowData, Tuple2<Integer, Integer>>> recordStateDesc =
                    new MapStateDescriptor<>(stateName, Types.LONG, mapTypeInfo);

            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }

            this.recordState = ctx.getMapState(recordStateDesc);
        }

        public List<Long> getAllTimes() throws Exception {
            List<Long> allTimes = new ArrayList<>();
            for (Long time : recordState.keys()) {
                allTimes.add(time);
            }
            return allTimes;
        }

        public void addRecord(long time, RowData record, int numOfAssociations) throws Exception {
            Map<RowData, Tuple2<Integer, Integer>> rowDataMap = recordState.get(time);
            Tuple2<Integer, Integer> frequencyAndNumOfAssociations;
            if (rowDataMap == null) {
                frequencyAndNumOfAssociations = Tuple2.of(1, numOfAssociations);
                rowDataMap = new HashMap<>();

            } else {
                frequencyAndNumOfAssociations = rowDataMap.get(record);
                if (frequencyAndNumOfAssociations == null) {
                    frequencyAndNumOfAssociations = Tuple2.of(1, numOfAssociations);
                } else {
                    frequencyAndNumOfAssociations.f0 = frequencyAndNumOfAssociations.f0 + 1;
                    frequencyAndNumOfAssociations.f1 = numOfAssociations;
                }
            }

            rowDataMap.put(record, frequencyAndNumOfAssociations);
            recordState.put(time, rowDataMap);
        }

        public void updateNumOfAssociations(long time, RowData record, int numOfAssociations)
                throws Exception {
            Map<RowData, Tuple2<Integer, Integer>> rowDataMap = recordState.get(time);
            Tuple2<Integer, Integer> frequencyAndNumOfAssociations;
            if (rowDataMap == null) {
                frequencyAndNumOfAssociations = Tuple2.of(1, numOfAssociations);
                rowDataMap = new HashMap<>();

            } else {
                frequencyAndNumOfAssociations = rowDataMap.get(record);
                if (frequencyAndNumOfAssociations == null) {
                    frequencyAndNumOfAssociations = Tuple2.of(1, numOfAssociations);
                } else {
                    frequencyAndNumOfAssociations.f1 = numOfAssociations;
                }
            }

            rowDataMap.put(record, frequencyAndNumOfAssociations);
            recordState.put(time, rowDataMap);
        }

        public List<Tuple2<RowData, Integer>> getRecordsAndNumOfAssociations(long time)
                throws Exception {
            List<Tuple2<RowData, Integer>> resultList = new ArrayList<>();
            Map<RowData, Tuple2<Integer, Integer>> rowDataMap = recordState.get(time);
            for (RowData rowData : rowDataMap.keySet()) {
                int remainTimes = rowDataMap.get(rowData).f0;
                Tuple2<RowData, Integer> recordAndNumOfAssociations =
                        Tuple2.of(rowData, rowDataMap.get(rowData).f1);
                while (remainTimes > 0) {
                    resultList.add(recordAndNumOfAssociations);
                    remainTimes--;
                }
            }
            return resultList;
        }
    }

    private static final class SemiAntiRightRecordStateView {
        // store records in the mapping like:
        // <event-time / proc-time, <record, frequency>
        private final MapState<Long, Map<RowData, Integer>> recordState;

        private SemiAntiRightRecordStateView(
                RuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType,
                StateTtlConfig ttlConfig) {

            MapTypeInfo<RowData, Integer> mapTypeInfo = new MapTypeInfo<>(recordType, Types.INT);
            MapStateDescriptor<Long, Map<RowData, Integer>> recordStateDesc =
                    new MapStateDescriptor<>(stateName, Types.LONG, mapTypeInfo);

            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }

            this.recordState = ctx.getMapState(recordStateDesc);
        }

        public List<Long> getAllTimes() throws Exception {
            List<Long> allTimes = new ArrayList<>();
            for (Long time : recordState.keys()) {
                allTimes.add(time);
            }
            return allTimes;
        }

        public void addRecord(long time, RowData record) throws Exception {
            Map<RowData, Integer> rowDataMap = recordState.get(time);
            Integer frequency;

            if (rowDataMap == null) {
                rowDataMap = new HashMap<>();
                frequency = 1;
            } else {
                frequency = rowDataMap.get(record);
                frequency = frequency == null ? 1 : frequency + 1;
            }

            rowDataMap.put(record, frequency);
            recordState.put(time, rowDataMap);
        }

        public void retractRecord(long time, RowData record) throws Exception {
            Map<RowData, Integer> rowDataMap = recordState.get(time);
            if (rowDataMap == null) {
                return;
            }

            Integer frequency = rowDataMap.get(record);
            if (frequency == null) {
                return;
            }
            if (frequency > 1) {
                rowDataMap.put(record, frequency - 1);
            } else {
                rowDataMap.remove(record);
            }
        }

        public List<RowData> getRecords(long time) throws Exception {
            List<RowData> resultList = new ArrayList<>();
            Map<RowData, Integer> rowDataMap = recordState.get(time);
            for (RowData rowData : rowDataMap.keySet()) {
                int remainTimes = rowDataMap.get(rowData);
                while (remainTimes > 0) {
                    resultList.add(rowData);
                    remainTimes--;
                }
            }
            return resultList;
        }
    }

    private static final class RowDataInfo {
        public final long time;
        public final RowData record;
        public final int numOfAssociations;

        private RowDataInfo(long time, RowData record, int numOfAssociations) {
            this.time = time;
            this.record = record;
            this.numOfAssociations = numOfAssociations;
        }

        public static RowDataInfo of(RowData record) {
            return new RowDataInfo(-1, record, -1);
        }

        public static RowDataInfo of(RowData record, int numOfAssociations) {
            return new RowDataInfo(-1, record, numOfAssociations);
        }

        public static RowDataInfo of(long time, RowData record, int numOfAssociations) {
            return new RowDataInfo(time, record, numOfAssociations);
        }
    }
}
