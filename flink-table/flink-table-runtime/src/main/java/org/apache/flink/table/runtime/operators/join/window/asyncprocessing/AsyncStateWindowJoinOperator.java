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

package org.apache.flink.table.runtime.operators.join.window.asyncprocessing;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncStateStreamOperator;
import org.apache.flink.runtime.state.v2.internal.InternalListState;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.AsyncStateTableStreamOperator;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.JoinConditionWithNullFilters;
import org.apache.flink.table.runtime.operators.join.window.WindowJoinOperator;
import org.apache.flink.table.runtime.operators.join.window.utils.WindowJoinHelper;
import org.apache.flink.table.runtime.operators.window.tvf.asyncprocessing.state.WindowListAsyncState;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowTimerService;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SlicingWindowTimerServiceImpl;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;

/**
 * A {@link AsyncStateWindowJoinOperator} implemented by async state api.
 *
 * <p>This class is nearly identical with {@link WindowJoinOperator}, but extending from {@link
 * AbstractAsyncStateStreamOperator} to integrate with asynchronous state access.
 *
 * <p>Note: currently, {@link AsyncStateWindowJoinOperator} doesn't support early-fire and
 * late-arrival. Thus, late elements (elements belong to emitted windows) will be simply dropped.
 *
 * <p>Note: currently, {@link AsyncStateWindowJoinOperator} doesn't support DELETE or UPDATE_BEFORE
 * input row.
 */
public class AsyncStateWindowJoinOperator extends AsyncStateTableStreamOperator<RowData>
        implements TwoInputStreamOperator<RowData, RowData, RowData>,
                Triggerable<RowData, Long>,
                KeyContext {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AsyncStateWindowJoinOperator.class);

    private static final String LEFT_RECORDS_STATE_NAME = "left-records";
    private static final String RIGHT_RECORDS_STATE_NAME = "right-records";

    private final RowDataSerializer leftSerializer;
    private final RowDataSerializer rightSerializer;
    private final GeneratedJoinCondition generatedJoinCondition;

    private final int leftWindowEndIndex;
    private final int rightWindowEndIndex;

    private final boolean[] filterNullKeys;
    private final ZoneId shiftTimeZone;

    private final FlinkJoinType joinType;

    private transient WindowTimerService<Long> windowTimerService;

    private transient JoinConditionWithNullFilters joinCondition;

    /** This is used for emitting elements with a given timestamp. */
    private transient TimestampedCollector<RowData> collector;

    private transient WindowListAsyncState<Long> leftWindowState;
    private transient WindowListAsyncState<Long> rightWindowState;

    private transient WindowJoinHelper helper;

    public AsyncStateWindowJoinOperator(
            TypeSerializer<RowData> leftSerializer,
            TypeSerializer<RowData> rightSerializer,
            GeneratedJoinCondition generatedJoinCondition,
            int leftWindowEndIndex,
            int rightWindowEndIndex,
            boolean[] filterNullKeys,
            ZoneId shiftTimeZone,
            FlinkJoinType joinType) {
        this.leftSerializer = (RowDataSerializer) leftSerializer;
        this.rightSerializer = (RowDataSerializer) rightSerializer;
        this.generatedJoinCondition = generatedJoinCondition;
        this.leftWindowEndIndex = leftWindowEndIndex;
        this.rightWindowEndIndex = rightWindowEndIndex;
        this.filterNullKeys = filterNullKeys;
        this.shiftTimeZone = shiftTimeZone;
        this.joinType = joinType;
    }

    @Override
    public void open() throws Exception {
        super.open();

        LOG.info("Window join is using async state");

        this.collector = new TimestampedCollector<>(output);
        collector.eraseTimestamp();

        final LongSerializer windowSerializer = LongSerializer.INSTANCE;

        InternalTimerService<Long> internalTimerService =
                getInternalTimerService("window-timers", windowSerializer, this);
        this.windowTimerService =
                new SlicingWindowTimerServiceImpl(internalTimerService, shiftTimeZone);

        // init join condition
        JoinCondition condition =
                generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.joinCondition = new JoinConditionWithNullFilters(condition, filterNullKeys, this);
        this.joinCondition.setRuntimeContext(getRuntimeContext());
        this.joinCondition.open(DefaultOpenContext.INSTANCE);

        // init state
        ListStateDescriptor<RowData> leftRecordStateDesc =
                new ListStateDescriptor<>(LEFT_RECORDS_STATE_NAME, leftSerializer);
        ListState<RowData> leftListState =
                getOrCreateKeyedState(Long.MIN_VALUE, windowSerializer, leftRecordStateDesc);
        this.leftWindowState =
                new WindowListAsyncState<>(
                        (InternalListState<RowData, Long, RowData>) leftListState);

        ListStateDescriptor<RowData> rightRecordStateDesc =
                new ListStateDescriptor<>(RIGHT_RECORDS_STATE_NAME, rightSerializer);
        ListState<RowData> rightListState =
                getOrCreateKeyedState(Long.MIN_VALUE, windowSerializer, rightRecordStateDesc);
        this.rightWindowState =
                new WindowListAsyncState<>(
                        (InternalListState<RowData, Long, RowData>) rightListState);

        this.helper = new AsyncStateWindowJoinHelper();
        this.helper.registerMetric(getRuntimeContext().getMetricGroup());
    }

    @Override
    public void close() throws Exception {
        super.close();
        collector = null;
        if (joinCondition != null) {
            joinCondition.close();
        }
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        helper.processElement(
                element,
                leftWindowEndIndex,
                helper.getLeftLateRecordsDroppedRate(),
                (windowEnd, rowData) -> leftWindowState.asyncAdd(windowEnd, rowData));
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        helper.processElement(
                element,
                rightWindowEndIndex,
                helper.getRightLateRecordsDroppedRate(),
                (windowEnd, rowData) -> rightWindowState.asyncAdd(windowEnd, rowData));
    }

    @Override
    public void onProcessingTime(InternalTimer<RowData, Long> timer) throws Exception {
        // Window join only support event-time now
        throw new UnsupportedOperationException(
                "This is a bug and should not happen. Please file an issue.");
    }

    @Override
    public void onEventTime(InternalTimer<RowData, Long> timer) throws Exception {
        triggerJoin(timer.getNamespace());
    }

    /**
     * Currently, similar to the {@link WindowJoinOperator#onEventTime} that uses the sync state
     * api, we directly load the list data from the state into memory to perform join operations.
     *
     * <p>Note: The order of data in the left and right side lists must be preserved to ensure the
     * output data sequence is maintained.
     */
    private void triggerJoin(long window) {
        StateFuture<StateIterator<RowData>> leftDataFuture = leftWindowState.asyncGet(window);
        StateFuture<StateIterator<RowData>> rightDataFuture = rightWindowState.asyncGet(window);
        StateFutureUtils.toIterable(leftDataFuture)
                .thenCombine(
                        StateFutureUtils.toIterable(rightDataFuture),
                        (leftDataIterator, rightDataIterator) -> {
                            helper.joinAndClear(window, leftDataIterator, rightDataIterator);
                            return null;
                        });
    }

    private class AsyncStateWindowJoinHelper extends WindowJoinHelper {

        public AsyncStateWindowJoinHelper() {
            super(
                    AsyncStateWindowJoinOperator.this.leftSerializer,
                    AsyncStateWindowJoinOperator.this.rightSerializer,
                    AsyncStateWindowJoinOperator.this.shiftTimeZone,
                    AsyncStateWindowJoinOperator.this.windowTimerService,
                    AsyncStateWindowJoinOperator.this.joinCondition,
                    AsyncStateWindowJoinOperator.this.collector,
                    AsyncStateWindowJoinOperator.this.joinType);
        }

        @Override
        public void clearState(long windowEnd, boolean isLeft) {
            // no need to wait these async requests to end
            if (isLeft) {
                leftWindowState.asyncClear(windowEnd);
            } else {
                rightWindowState.asyncClear(windowEnd);
            }
        }
    }
}
