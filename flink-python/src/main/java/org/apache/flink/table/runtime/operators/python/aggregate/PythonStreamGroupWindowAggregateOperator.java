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

package org.apache.flink.table.runtime.operators.python.aggregate;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.UpdatableRowData;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo;
import org.apache.flink.table.planner.expressions.PlannerNamedWindowProperty;
import org.apache.flink.table.planner.expressions.PlannerProctimeAttribute;
import org.apache.flink.table.planner.expressions.PlannerRowtimeAttribute;
import org.apache.flink.table.planner.expressions.PlannerWindowEnd;
import org.apache.flink.table.planner.expressions.PlannerWindowProperty;
import org.apache.flink.table.planner.expressions.PlannerWindowStart;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.SessionGroupWindow;
import org.apache.flink.table.planner.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.planner.plan.logical.TumblingGroupWindow;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.typeutils.DataViewUtils;
import org.apache.flink.table.runtime.operators.window.CountWindow;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;

import java.util.ArrayList;
import java.util.List;

/** The Python Group Window AggregateFunction operator for the blink planner. */
@Internal
public class PythonStreamGroupWindowAggregateOperator<K, W extends Window>
        extends AbstractPythonStreamAggregateOperator implements Triggerable<K, W> {

    private static final long serialVersionUID = 1L;

    @VisibleForTesting
    static final String STREAM_GROUP_WINDOW_AGGREGATE_URN =
            "flink:transform:stream_group_window_aggregate:v1";

    @VisibleForTesting static final byte REGISTER_EVENT_TIMER = 0;

    @VisibleForTesting static final byte REGISTER_PROCESSING_TIMER = 1;

    @VisibleForTesting static final byte DELETE_EVENT_TIMER = 2;

    @VisibleForTesting static final byte DELETE_PROCESSING_TIMER = 3;

    /** True if the count(*) agg is inserted by the planner. */
    private final boolean countStarInserted;

    /** The row time index of the input data. */
    @VisibleForTesting final int inputTimeFieldIndex;

    /**
     * The allowed lateness for elements. This is used for:
     *
     * <ul>
     *   <li>Deciding if an element should be dropped from a window due to lateness.
     *   <li>Clearing the state of a window if the system time passes the {@code window.maxTimestamp
     *       + allowedLateness} landmark.
     * </ul>
     */
    @VisibleForTesting final long allowedLateness;

    /** The Infos of the Window. */
    private FlinkFnApi.GroupWindow.WindowProperty[] namedProperties;

    /** A {@link WindowAssigner} assigns zero or more {@link Window Windows} to an element. */
    @VisibleForTesting final WindowAssigner<W> windowAssigner;

    /** Window Type includes Tumble window, Sliding window and Session Window. */
    private FlinkFnApi.GroupWindow.WindowType windowType;

    /** Whether it is a row time window. */
    private boolean isRowTime;

    /** Whether it is a Time Window. */
    private boolean isTimeWindow;

    /** Window size. */
    private long size;

    /** Window slide. */
    private long slide;

    /** Session Window gap. */
    private long gap;

    /** For serializing the window in checkpoints. */
    @VisibleForTesting transient TypeSerializer<W> windowSerializer;

    /** Interface for working with time and timers. */
    private transient InternalTimerService<W> internalTimerService;

    private transient UpdatableRowData reuseTimerData;

    private transient int timerDataLength;

    private transient int keyLength;

    public PythonStreamGroupWindowAggregateOperator(
            Configuration config,
            RowType inputType,
            RowType outputType,
            PythonAggregateFunctionInfo[] aggregateFunctions,
            DataViewUtils.DataViewSpec[][] dataViewSpecs,
            int[] grouping,
            int indexOfCountStar,
            boolean generateUpdateBefore,
            boolean countStarInserted,
            int inputTimeFieldIndex,
            WindowAssigner<W> windowAssigner,
            LogicalWindow window,
            long allowedLateness,
            PlannerNamedWindowProperty[] namedProperties) {
        super(
                config,
                inputType,
                outputType,
                aggregateFunctions,
                dataViewSpecs,
                grouping,
                indexOfCountStar,
                generateUpdateBefore);
        this.countStarInserted = countStarInserted;
        this.inputTimeFieldIndex = inputTimeFieldIndex;
        this.windowAssigner = windowAssigner;
        this.allowedLateness = allowedLateness;
        buildWindow(window, namedProperties);
    }

    @Override
    public void open() throws Exception {
        windowSerializer = windowAssigner.getWindowSerializer(new ExecutionConfig());
        internalTimerService = getInternalTimerService("window-timers", windowSerializer, this);
        // The structure is:  [timer_type]|[row key]|[optional field]...
        // If the window is 'TimeWindow', store the TimeWindow start in the 2nd column and
        // TimeWindow end in the 3rd Column. e.g. the [optional field]s are
        // [Time Window start][Time Window end].
        // If the window is 'CountWindow', store the CountWindow id in the 2rd column. e.g.
        // the [optional field]s are [Count Window id].
        if (isTimeWindow) {
            reuseTimerData = new UpdatableRowData(GenericRowData.of(0, null, 0, 0), 4);
        } else {
            reuseTimerData = new UpdatableRowData(GenericRowData.of(0, null, 0), 3);
        }
        keyLength = getKeyType().getFieldCount();
        super.open();
        reuseTimerRowData.setField(3, reuseTimerData);
    }

    @Override
    public void processElementInternal(RowData value) throws Exception {
        reuseRowData.setField(1, value);
        reuseRowData.setLong(2, internalTimerService.currentWatermark());
        udfInputTypeSerializer.serialize(reuseRowData, baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
        baos.reset();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
        byte[] rawUdfResult = resultTuple.f0;
        int length = resultTuple.f1;
        bais.setBuffer(rawUdfResult, 0, length);
        RowData udfResult = udfOutputTypeSerializer.deserialize(baisWrapper);
        byte recordType = udfResult.getByte(0);
        if (recordType == NORMAL_RECORD) {
            GenericRowData aggResult =
                    (GenericRowData) udfResult.getRow(1, outputType.getFieldCount());
            int fieldCount = outputType.getFieldCount();
            for (int i = fieldCount - namedProperties.length; i < fieldCount; i++) {
                aggResult.setField(i, TimestampData.fromEpochMillis(aggResult.getLong(i)));
            }
            rowDataWrapper.collect(aggResult);
        } else {
            RowData timerData = udfResult.getRow(2, timerDataLength);
            byte timerOperandType = timerData.getByte(0);
            RowData key = timerData.getRow(1, keyLength);
            long timestamp = timerData.getLong(2);
            W window;
            if (isTimeWindow) {
                long start = timerData.getLong(3);
                long end = timerData.getLong(4);
                window = (W) TimeWindow.of(start, end);
            } else {
                long id = timerData.getLong(3);
                window = (W) new CountWindow(id);
            }
            synchronized (getKeyedStateBackend()) {
                setCurrentKey(key);

                if (timerOperandType == REGISTER_EVENT_TIMER) {
                    internalTimerService.registerEventTimeTimer(window, timestamp);
                } else if (timerOperandType == REGISTER_PROCESSING_TIMER) {
                    internalTimerService.registerProcessingTimeTimer(window, timestamp);
                } else if (timerOperandType == DELETE_EVENT_TIMER) {
                    internalTimerService.deleteEventTimeTimer(window, timestamp);
                } else if (timerOperandType == DELETE_PROCESSING_TIMER) {
                    internalTimerService.deleteProcessingTimeTimer(window, timestamp);
                } else {
                    throw new RuntimeException(
                            String.format("Unsupported timerOperandType %s.", timerOperandType));
                }
            }
        }
    }

    @Override
    public String getFunctionUrn() {
        return STREAM_GROUP_WINDOW_AGGREGATE_URN;
    }

    @Override
    public RowType getUserDefinedFunctionInputType() {
        List<RowType.RowField> inputFields = new ArrayList<>();
        inputFields.add(new RowType.RowField("record_type", new TinyIntType()));
        inputFields.add(new RowType.RowField("row_data", inputType));
        inputFields.add(new RowType.RowField("timestamp", new BigIntType()));
        List<RowType.RowField> timerDataFields = new ArrayList<>();
        timerDataFields.add(new RowType.RowField("timer_type", new TinyIntType()));
        timerDataFields.add(new RowType.RowField("key", getKeyType()));
        if (isTimeWindow) {
            timerDataFields.add(new RowType.RowField("start", new BigIntType()));
            timerDataFields.add(new RowType.RowField("end", new BigIntType()));
        } else {
            timerDataFields.add(new RowType.RowField("id", new BigIntType()));
        }
        inputFields.add(new RowType.RowField("timer", new RowType(timerDataFields)));
        return new RowType(inputFields);
    }

    @Override
    public RowType getUserDefinedFunctionOutputType() {
        List<RowType.RowField> outputFields = new ArrayList<>();
        outputFields.add(new RowType.RowField("record_type", new TinyIntType()));
        List<RowType.RowField> resultFields =
                new ArrayList<>(
                        outputType
                                .getFields()
                                .subList(0, outputType.getFieldCount() - namedProperties.length));
        for (int i = 0; i < namedProperties.length; i++) {
            resultFields.add(new RowType.RowField("w" + i, new BigIntType()));
        }
        outputFields.add(new RowType.RowField("row_data", new RowType(resultFields)));
        List<RowType.RowField> timerDataFields = new ArrayList<>();
        timerDataFields.add(new RowType.RowField("timer_operand_type", new TinyIntType()));
        timerDataFields.add(new RowType.RowField("key", getKeyType()));
        timerDataFields.add(new RowType.RowField("timestamp", new BigIntType()));
        if (isTimeWindow) {
            timerDataFields.add(new RowType.RowField("start", new BigIntType()));
            timerDataFields.add(new RowType.RowField("end", new BigIntType()));
        } else {
            timerDataFields.add(new RowType.RowField("id", new BigIntType()));
        }
        timerDataLength = timerDataFields.size();
        outputFields.add(new RowType.RowField("timer", new RowType(timerDataFields)));
        return new RowType(outputFields);
    }

    @Override
    protected FlinkFnApi.UserDefinedAggregateFunctions getUserDefinedFunctionsProto() {
        FlinkFnApi.UserDefinedAggregateFunctions.Builder builder =
                super.getUserDefinedFunctionsProto().toBuilder();
        builder.setCountStarInserted(countStarInserted);
        FlinkFnApi.GroupWindow.Builder windowBuilder = FlinkFnApi.GroupWindow.newBuilder();
        windowBuilder.setWindowType(windowType);
        windowBuilder.setIsTimeWindow(isTimeWindow);
        windowBuilder.setIsRowTime(isRowTime);
        windowBuilder.setTimeFieldIndex(inputTimeFieldIndex);
        windowBuilder.setWindowSize(size);
        windowBuilder.setWindowSlide(slide);
        windowBuilder.setWindowGap(gap);
        windowBuilder.setAllowedLateness(allowedLateness);
        for (FlinkFnApi.GroupWindow.WindowProperty namedProperty : namedProperties) {
            windowBuilder.addNamedProperties(namedProperty);
        }
        builder.setGroupWindow(windowBuilder);
        return builder.build();
    }

    @Override
    public void onEventTime(InternalTimer<K, W> timer) throws Exception {
        emitTriggerTimerData(timer, REGISTER_EVENT_TIMER);
    }

    @Override
    public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
        emitTriggerTimerData(timer, REGISTER_PROCESSING_TIMER);
    }

    private void buildWindow(LogicalWindow window, PlannerNamedWindowProperty[] namedProperties) {
        ValueLiteralExpression size = null;
        ValueLiteralExpression slide = null;
        ValueLiteralExpression gap = null;
        if (window instanceof TumblingGroupWindow) {
            this.windowType = FlinkFnApi.GroupWindow.WindowType.TUMBLING_GROUP_WINDOW;
            size = ((TumblingGroupWindow) window).size();
        } else if (window instanceof SlidingGroupWindow) {
            this.windowType = FlinkFnApi.GroupWindow.WindowType.SLIDING_GROUP_WINDOW;
            size = ((SlidingGroupWindow) window).size();
            slide = ((SlidingGroupWindow) window).slide();
        } else if (window instanceof SessionGroupWindow) {
            this.windowType = FlinkFnApi.GroupWindow.WindowType.SESSION_GROUP_WINDOW;
            gap = ((SessionGroupWindow) window).gap();
        } else {
            throw new RuntimeException(String.format("Unsupported LogicWindow Type %s", window));
        }
        this.isRowTime = AggregateUtil.isRowtimeAttribute(window.timeAttribute());
        this.isTimeWindow = gap != null || AggregateUtil.hasTimeIntervalType(size);
        if (size != null) {
            this.size = AggregateUtil.toDuration(size).toMillis();
        } else {
            this.size = 0L;
        }
        if (slide != null) {
            this.slide = AggregateUtil.toDuration(slide).toMillis();
        } else {
            this.slide = 0L;
        }
        if (gap != null) {
            this.gap = AggregateUtil.toDuration(gap).toMillis();
        } else {
            this.gap = 0L;
        }

        this.namedProperties = new FlinkFnApi.GroupWindow.WindowProperty[namedProperties.length];
        for (int i = 0; i < namedProperties.length; i++) {
            PlannerWindowProperty namedProperty = namedProperties[i].getProperty();
            if (namedProperty instanceof PlannerWindowStart) {
                this.namedProperties[i] = FlinkFnApi.GroupWindow.WindowProperty.WINDOW_START;
            } else if (namedProperty instanceof PlannerWindowEnd) {
                this.namedProperties[i] = FlinkFnApi.GroupWindow.WindowProperty.WINDOW_END;
            } else if (namedProperty instanceof PlannerRowtimeAttribute) {
                this.namedProperties[i] = FlinkFnApi.GroupWindow.WindowProperty.ROW_TIME_ATTRIBUTE;
            } else if (namedProperty instanceof PlannerProctimeAttribute) {
                this.namedProperties[i] = FlinkFnApi.GroupWindow.WindowProperty.PROC_TIME_ATTRIBUTE;

            } else {
                throw new RuntimeException("Unexpected property " + namedProperty);
            }
        }
    }

    private void emitTriggerTimerData(InternalTimer<K, W> timer, byte processingTimer)
            throws Exception {
        W window = timer.getNamespace();
        reuseTimerData.setByte(0, processingTimer);
        reuseTimerData.setField(1, timer.getKey());
        if (isTimeWindow) {
            reuseTimerData.setLong(2, ((TimeWindow) window).getStart());
            reuseTimerData.setLong(3, ((TimeWindow) window).getEnd());
        } else {
            reuseTimerData.setLong(2, ((CountWindow) window).getId());
        }
        reuseTimerRowData.setLong(2, timer.getTimestamp());
        udfInputTypeSerializer.serialize(reuseTimerRowData, baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
        baos.reset();
        elementCount++;
        checkInvokeFinishBundleByCount();
        emitResults();
    }
}
