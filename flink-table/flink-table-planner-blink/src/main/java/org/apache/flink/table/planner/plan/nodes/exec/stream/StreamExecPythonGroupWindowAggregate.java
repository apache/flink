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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.expressions.PlannerProctimeAttribute;
import org.apache.flink.table.planner.expressions.PlannerRowtimeAttribute;
import org.apache.flink.table.planner.expressions.PlannerWindowEnd;
import org.apache.flink.table.planner.expressions.PlannerWindowProperty;
import org.apache.flink.table.planner.expressions.PlannerWindowStart;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.planner.plan.logical.TumblingGroupWindow;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.utils.CommonPythonUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.WindowEmitStrategy;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.window.assigners.CountSlidingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.CountTumblingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.SlidingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.TumblingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.operators.window.triggers.ElementTriggers;
import org.apache.flink.table.runtime.operators.window.triggers.EventTimeTriggers;
import org.apache.flink.table.runtime.operators.window.triggers.ProcessingTimeTriggers;
import org.apache.flink.table.runtime.operators.window.triggers.Trigger;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.core.AggregateCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.planner.plan.utils.AggregateUtil.hasRowIntervalType;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.hasTimeIntervalType;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.isProctimeAttribute;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.isRowtimeAttribute;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.timeFieldIndex;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.toDuration;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.toLong;

/** Stream [[ExecNode]] for group widow aggregate (Python user defined aggregate function). */
public class StreamExecPythonGroupWindowAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData> {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(StreamExecPythonGroupWindowAggregate.class);

    private static final String ARROW_STREAM_PYTHON_GROUP_WINDOW_AGGREGATE_FUNCTION_OPERATOR_NAME =
            "org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream."
                    + "StreamArrowPythonGroupWindowAggregateFunctionOperator";

    private final int[] grouping;
    private final AggregateCall[] aggCalls;
    private final LogicalWindow window;
    private final FlinkRelBuilder.PlannerNamedWindowProperty[] namedWindowProperties;
    private final WindowEmitStrategy emitStrategy;

    public StreamExecPythonGroupWindowAggregate(
            int[] grouping,
            AggregateCall[] aggCalls,
            LogicalWindow window,
            FlinkRelBuilder.PlannerNamedWindowProperty[] namedWindowProperties,
            WindowEmitStrategy emitStrategy,
            ExecEdge inputEdge,
            RowType outputType,
            String description) {
        super(Collections.singletonList(inputEdge), outputType, description);
        this.grouping = grouping;
        this.aggCalls = aggCalls;
        this.window = window;
        this.namedWindowProperties = namedWindowProperties;
        this.emitStrategy = emitStrategy;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final boolean isCountWindow;
        if (window instanceof TumblingGroupWindow) {
            isCountWindow = hasRowIntervalType(((TumblingGroupWindow) window).size());
        } else if (window instanceof SlidingGroupWindow) {
            isCountWindow = hasRowIntervalType(((SlidingGroupWindow) window).size());
        } else {
            isCountWindow = false;
        }

        final TableConfig tableConfig = planner.getTableConfig();
        if (isCountWindow
                && grouping.length > 0
                && tableConfig.getMinIdleStateRetentionTime() < 0) {
            LOGGER.warn(
                    "No state retention interval configured for a query which accumulates state."
                            + " Please provide a query configuration with valid retention interval to"
                            + " prevent excessive state size. You may specify a retention time of 0 to"
                            + " not clean up the state.");
        }

        final ExecNode<RowData> inputNode = (ExecNode<RowData>) getInputNodes().get(0);
        final Transformation<RowData> inputTransform = inputNode.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputNode.getOutputType();
        final RowType outputRowType = InternalTypeInfo.of(getOutputType()).toRowType();

        final int inputTimeFieldIndex;
        if (isRowtimeAttribute(window.timeAttribute())) {
            inputTimeFieldIndex =
                    timeFieldIndex(
                            FlinkTypeFactory.INSTANCE().buildRelNodeRowType(inputRowType),
                            planner.getRelBuilder(),
                            window.timeAttribute());
            if (inputTimeFieldIndex < 0) {
                throw new TableException(
                        "Group window must defined on a time attribute, "
                                + "but the time attribute can't be found.\n"
                                + "This should never happen. Please file an issue.");
            }
        } else {
            inputTimeFieldIndex = -1;
        }
        Tuple2<WindowAssigner<?>, Trigger<?>> windowAssignerAndTrigger =
                generateWindowAssignerAndTrigger();
        WindowAssigner<?> windowAssigner = windowAssignerAndTrigger.f0;
        Trigger<?> trigger = windowAssignerAndTrigger.f1;
        Configuration config = CommonPythonUtil.getMergedConfig(planner.getExecEnv(), tableConfig);
        OneInputTransformation<RowData, RowData> transform =
                createPythonStreamWindowGroupOneInputTransformation(
                        inputTransform,
                        inputRowType,
                        outputRowType,
                        inputTimeFieldIndex,
                        windowAssigner,
                        trigger,
                        emitStrategy.getAllowLateness(),
                        config);
        if (inputsContainSingleton()) {
            transform.setParallelism(1);
            transform.setMaxParallelism(1);
        }

        if (CommonPythonUtil.isPythonWorkerUsingManagedMemory(config)) {
            transform.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
        }
        // set KeyType and Selector for state
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(grouping, InternalTypeInfo.of(inputRowType));
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());
        return transform;
    }

    private Tuple2<WindowAssigner<?>, Trigger<?>> generateWindowAssignerAndTrigger() {
        WindowAssigner<?> windowAssiger;
        Trigger<?> trigger;
        if (window instanceof TumblingGroupWindow) {
            TumblingGroupWindow tumblingWindow = (TumblingGroupWindow) window;
            FieldReferenceExpression timeField = tumblingWindow.timeField();
            ValueLiteralExpression size = tumblingWindow.size();
            if (isProctimeAttribute(timeField) && hasTimeIntervalType(size)) {
                windowAssiger = TumblingWindowAssigner.of(toDuration(size)).withProcessingTime();
                trigger = ProcessingTimeTriggers.afterEndOfWindow();
            } else if (isRowtimeAttribute(timeField) && hasTimeIntervalType(size)) {
                windowAssiger = TumblingWindowAssigner.of(toDuration(size)).withEventTime();
                trigger = EventTimeTriggers.afterEndOfWindow();
            } else if (isProctimeAttribute(timeField) && hasRowIntervalType(size)) {
                windowAssiger = CountTumblingWindowAssigner.of(toLong(size));
                trigger = ElementTriggers.count(toLong(size));
            } else {
                // TODO: EventTimeTumblingGroupWindow should sort the stream on event time
                // before applying the  windowing logic. Otherwise, this would be the same as a
                // ProcessingTimeTumblingGroupWindow
                throw new UnsupportedOperationException(
                        "Event-time grouping windows on row intervals are currently not supported.");
            }
        } else if (window instanceof SlidingGroupWindow) {
            SlidingGroupWindow slidingWindow = (SlidingGroupWindow) window;
            FieldReferenceExpression timeField = slidingWindow.timeField();
            ValueLiteralExpression size = slidingWindow.size();
            ValueLiteralExpression slide = slidingWindow.slide();
            if (isProctimeAttribute(timeField) && hasTimeIntervalType(size)) {
                windowAssiger = SlidingWindowAssigner.of(toDuration(size), toDuration(slide));
                trigger = ProcessingTimeTriggers.afterEndOfWindow();
            } else if (isRowtimeAttribute(timeField) && hasTimeIntervalType(size)) {
                windowAssiger = SlidingWindowAssigner.of(toDuration(size), toDuration(slide));
                trigger = EventTimeTriggers.afterEndOfWindow();
            } else if (isProctimeAttribute(timeField) && hasRowIntervalType(size)) {
                windowAssiger = CountSlidingWindowAssigner.of(toLong(size), toLong(slide));
                trigger = ElementTriggers.count(toLong(size));
            } else {
                // TODO: EventTimeTumblingGroupWindow should sort the stream on event time
                // before applying the  windowing logic. Otherwise, this would be the same as a
                // ProcessingTimeTumblingGroupWindow
                throw new UnsupportedOperationException(
                        "Event-time grouping windows on row intervals are currently not supported.");
            }
        } else {
            throw new TableException("Unsupported window: " + window.toString());
        }
        return Tuple2.of(windowAssiger, trigger);
    }

    private OneInputTransformation<RowData, RowData>
            createPythonStreamWindowGroupOneInputTransformation(
                    Transformation<RowData> inputTransform,
                    RowType inputRowType,
                    RowType outputRowType,
                    int inputTimeFieldIndex,
                    WindowAssigner<?> windowAssigner,
                    Trigger<?> trigger,
                    long allowance,
                    Configuration config) {
        int[] namePropertyTypeArray =
                Arrays.stream(namedWindowProperties)
                        .mapToInt(
                                p -> {
                                    PlannerWindowProperty property = p.property();
                                    if (property instanceof PlannerWindowStart) {
                                        return 0;
                                    }
                                    if (property instanceof PlannerWindowEnd) {
                                        return 1;
                                    }
                                    if (property instanceof PlannerRowtimeAttribute) {
                                        return 2;
                                    }
                                    if (property instanceof PlannerProctimeAttribute) {
                                        return 3;
                                    }
                                    throw new TableException("Unexpected property " + property);
                                })
                        .toArray();

        Tuple2<int[], PythonFunctionInfo[]> aggInfos =
                CommonPythonUtil.extractPythonAggregateFunctionInfosFromAggregateCall(aggCalls);
        int[] pythonUdafInputOffsets = aggInfos.f0;
        PythonFunctionInfo[] pythonFunctionInfos = aggInfos.f1;
        OneInputStreamOperator<RowData, RowData> pythonOperator =
                getPythonStreamGroupWindowAggregateFunctionOperator(
                        config,
                        inputRowType,
                        outputRowType,
                        windowAssigner,
                        trigger,
                        allowance,
                        inputTimeFieldIndex,
                        namePropertyTypeArray,
                        pythonUdafInputOffsets,
                        pythonFunctionInfos);
        return new OneInputTransformation<>(
                inputTransform,
                getDesc(),
                pythonOperator,
                InternalTypeInfo.of(outputRowType),
                inputTransform.getParallelism());
    }

    @SuppressWarnings("unchecked")
    private OneInputStreamOperator<RowData, RowData>
            getPythonStreamGroupWindowAggregateFunctionOperator(
                    Configuration config,
                    RowType inputRowType,
                    RowType outputRowType,
                    WindowAssigner<?> windowAssigner,
                    Trigger<?> trigger,
                    long allowance,
                    int inputTimeFieldIndex,
                    int[] namedProperties,
                    int[] udafInputOffsets,
                    PythonFunctionInfo[] pythonFunctionInfos) {
        Class clazz =
                CommonPythonUtil.loadClass(
                        ARROW_STREAM_PYTHON_GROUP_WINDOW_AGGREGATE_FUNCTION_OPERATOR_NAME);
        try {
            Constructor<OneInputStreamOperator<RowData, RowData>> ctor =
                    clazz.getConstructor(
                            Configuration.class,
                            PythonFunctionInfo[].class,
                            RowType.class,
                            RowType.class,
                            int.class,
                            WindowAssigner.class,
                            Trigger.class,
                            long.class,
                            int[].class,
                            int[].class,
                            int[].class);
            return ctor.newInstance(
                    config,
                    pythonFunctionInfos,
                    inputRowType,
                    outputRowType,
                    inputTimeFieldIndex,
                    windowAssigner,
                    trigger,
                    allowance,
                    namedProperties,
                    grouping,
                    udafInputOffsets);
        } catch (NoSuchMethodException
                | IllegalAccessException
                | InstantiationException
                | InvocationTargetException e) {
            throw new TableException(
                    "Python StreamArrowPythonGroupWindowAggregateFunctionOperator constructed failed.",
                    e);
        }
    }
}
