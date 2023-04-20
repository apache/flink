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

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.SessionGroupWindow;
import org.apache.flink.table.planner.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.planner.plan.logical.TumblingGroupWindow;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.WindowEmitStrategy;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.generated.GeneratedClass;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceTableAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.groupwindow.WindowProperty;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.window.CountWindow;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.WindowOperator;
import org.apache.flink.table.runtime.operators.window.WindowOperatorBuilder;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.planner.plan.utils.AggregateUtil.hasRowIntervalType;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.hasTimeIntervalType;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.isProctimeAttribute;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.isRowtimeAttribute;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.isTableAggregate;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.timeFieldIndex;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.toDuration;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.toLong;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.transformToStreamAggregateInfoList;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Stream {@link ExecNode} for either group window aggregate or group window table aggregate.
 *
 * <p>The differences between {@link StreamExecWindowAggregate} and {@link
 * StreamExecGroupWindowAggregate} is that, this node is translated from window TVF syntax, but the
 * * other is from the legacy GROUP WINDOW FUNCTION syntax. In the long future, {@link
 * StreamExecGroupWindowAggregate} will be dropped.
 */
@ExecNodeMetadata(
        name = "stream-exec-group-window-aggregate",
        version = 1,
        consumedOptions = {
            "table.local-time-zone",
            "table.exec.mini-batch.enabled",
            "table.exec.mini-batch.size"
        },
        producedTransformations =
                StreamExecGroupWindowAggregate.GROUP_WINDOW_AGGREGATE_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecGroupWindowAggregate extends StreamExecAggregateBase {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(StreamExecGroupWindowAggregate.class);

    public static final String GROUP_WINDOW_AGGREGATE_TRANSFORMATION = "group-window-aggregate";

    public static final String FIELD_NAME_WINDOW = "window";
    public static final String FIELD_NAME_NAMED_WINDOW_PROPERTIES = "namedWindowProperties";

    @JsonProperty(FIELD_NAME_GROUPING)
    private final int[] grouping;

    @JsonProperty(FIELD_NAME_AGG_CALLS)
    private final AggregateCall[] aggCalls;

    @JsonProperty(FIELD_NAME_WINDOW)
    private final LogicalWindow window;

    @JsonProperty(FIELD_NAME_NAMED_WINDOW_PROPERTIES)
    private final NamedWindowProperty[] namedWindowProperties;

    @JsonProperty(FIELD_NAME_NEED_RETRACTION)
    private final boolean needRetraction;

    public StreamExecGroupWindowAggregate(
            ReadableConfig tableConfig,
            int[] grouping,
            AggregateCall[] aggCalls,
            LogicalWindow window,
            NamedWindowProperty[] namedWindowProperties,
            boolean needRetraction,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecGroupWindowAggregate.class),
                ExecNodeContext.newPersistedConfig(
                        StreamExecGroupWindowAggregate.class, tableConfig),
                grouping,
                aggCalls,
                window,
                namedWindowProperties,
                needRetraction,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecGroupWindowAggregate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
            @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
            @JsonProperty(FIELD_NAME_WINDOW) LogicalWindow window,
            @JsonProperty(FIELD_NAME_NAMED_WINDOW_PROPERTIES)
                    NamedWindowProperty[] namedWindowProperties,
            @JsonProperty(FIELD_NAME_NEED_RETRACTION) boolean needRetraction,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.grouping = checkNotNull(grouping);
        this.aggCalls = checkNotNull(aggCalls);
        this.window = checkNotNull(window);
        this.namedWindowProperties = checkNotNull(namedWindowProperties);
        this.needRetraction = needRetraction;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final boolean isCountWindow;
        if (window instanceof TumblingGroupWindow) {
            isCountWindow = hasRowIntervalType(((TumblingGroupWindow) window).size());
        } else if (window instanceof SlidingGroupWindow) {
            isCountWindow = hasRowIntervalType(((SlidingGroupWindow) window).size());
        } else {
            isCountWindow = false;
        }

        if (isCountWindow && grouping.length > 0 && config.getStateRetentionTime() < 0) {
            LOGGER.warn(
                    "No state retention interval configured for a query which accumulates state. "
                            + "Please provide a query configuration with valid retention interval to prevent "
                            + "excessive state size. You may specify a retention time of 0 to not clean up the state.");
        }

        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();

        final int inputTimeFieldIndex;
        if (isRowtimeAttribute(window.timeAttribute())) {
            inputTimeFieldIndex =
                    timeFieldIndex(
                            planner.getTypeFactory().buildRelNodeRowType(inputRowType),
                            planner.createRelBuilder(),
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

        final ZoneId shiftTimeZone =
                TimeWindowUtil.getShiftTimeZone(
                        window.timeAttribute().getOutputDataType().getLogicalType(),
                        TableConfigUtils.getLocalTimeZone(config));

        final boolean[] aggCallNeedRetractions = new boolean[aggCalls.length];
        Arrays.fill(aggCallNeedRetractions, needRetraction);
        final AggregateInfoList aggInfoList =
                transformToStreamAggregateInfoList(
                        planner.getTypeFactory(),
                        inputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        aggCallNeedRetractions,
                        needRetraction,
                        true, // isStateBackendDataViews
                        true); // needDistinctInfo

        final GeneratedClass<?> aggCodeGenerator =
                createAggsHandler(
                        aggInfoList,
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        planner.createRelBuilder(),
                        inputRowType.getChildren(),
                        shiftTimeZone);

        final LogicalType[] aggResultTypes = extractLogicalTypes(aggInfoList.getActualValueTypes());
        final LogicalType[] windowPropertyTypes =
                Arrays.stream(namedWindowProperties)
                        .map(p -> p.getProperty().getResultType())
                        .toArray(LogicalType[]::new);

        final EqualiserCodeGenerator generator =
                new EqualiserCodeGenerator(
                        ArrayUtils.addAll(aggResultTypes, windowPropertyTypes),
                        planner.getFlinkContext().getClassLoader());
        final GeneratedRecordEqualiser equaliser =
                generator.generateRecordEqualiser("WindowValueEqualiser");

        final LogicalType[] aggValueTypes = extractLogicalTypes(aggInfoList.getActualValueTypes());
        final LogicalType[] accTypes = extractLogicalTypes(aggInfoList.getAccTypes());
        final int inputCountIndex = aggInfoList.getIndexOfCountStar();

        final WindowOperator<?, ?> operator =
                createWindowOperator(
                        config,
                        aggCodeGenerator,
                        equaliser,
                        accTypes,
                        windowPropertyTypes,
                        aggValueTypes,
                        inputRowType.getChildren().toArray(new LogicalType[0]),
                        inputTimeFieldIndex,
                        shiftTimeZone,
                        inputCountIndex);

        final OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(GROUP_WINDOW_AGGREGATE_TRANSFORMATION, config),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism(),
                        false);

        // set KeyType and Selector for state
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(),
                        grouping,
                        InternalTypeInfo.of(inputRowType));
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());
        return transform;
    }

    private LogicalType[] extractLogicalTypes(DataType[] dataTypes) {
        return Arrays.stream(dataTypes)
                .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                .toArray(LogicalType[]::new);
    }

    private GeneratedClass<?> createAggsHandler(
            AggregateInfoList aggInfoList,
            ExecNodeConfig config,
            ClassLoader classLoader,
            RelBuilder relBuilder,
            List<LogicalType> fieldTypes,
            ZoneId shiftTimeZone) {
        final boolean needMerge;
        final Class<?> windowClass;
        if (window instanceof SlidingGroupWindow) {
            ValueLiteralExpression size = ((SlidingGroupWindow) window).size();
            needMerge = hasTimeIntervalType(size);
            windowClass = hasRowIntervalType(size) ? CountWindow.class : TimeWindow.class;
        } else if (window instanceof TumblingGroupWindow) {
            needMerge = false;
            ValueLiteralExpression size = ((TumblingGroupWindow) window).size();
            windowClass = hasRowIntervalType(size) ? CountWindow.class : TimeWindow.class;
        } else if (window instanceof SessionGroupWindow) {
            needMerge = true;
            windowClass = TimeWindow.class;
        } else {
            throw new TableException("Unsupported window: " + window.toString());
        }

        final AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                                new CodeGeneratorContext(config, classLoader),
                                relBuilder,
                                JavaScalaConversionUtil.toScala(fieldTypes),
                                false) // copyInputField
                        .needAccumulate();

        if (needMerge) {
            generator.needMerge(0, false, null);
        }
        if (needRetraction) {
            generator.needRetract();
        }

        final List<WindowProperty> windowProperties =
                Arrays.asList(
                        Arrays.stream(namedWindowProperties)
                                .map(NamedWindowProperty::getProperty)
                                .toArray(WindowProperty[]::new));
        final boolean isTableAggregate =
                isTableAggregate(Arrays.asList(aggInfoList.getActualAggregateCalls()));
        if (isTableAggregate) {
            return generator.generateNamespaceTableAggsHandler(
                    "GroupingWindowTableAggsHandler",
                    aggInfoList,
                    JavaScalaConversionUtil.toScala(windowProperties),
                    windowClass,
                    shiftTimeZone);
        } else {
            return generator.generateNamespaceAggsHandler(
                    "GroupingWindowAggsHandler",
                    aggInfoList,
                    JavaScalaConversionUtil.toScala(windowProperties),
                    windowClass,
                    shiftTimeZone);
        }
    }

    private WindowOperator<?, ?> createWindowOperator(
            ReadableConfig config,
            GeneratedClass<?> aggsHandler,
            GeneratedRecordEqualiser recordEqualiser,
            LogicalType[] accTypes,
            LogicalType[] windowPropertyTypes,
            LogicalType[] aggValueTypes,
            LogicalType[] inputFields,
            int timeFieldIndex,
            ZoneId shiftTimeZone,
            int inputCountIndex) {
        WindowOperatorBuilder builder =
                WindowOperatorBuilder.builder()
                        .withInputFields(inputFields)
                        .withShiftTimezone(shiftTimeZone)
                        .withInputCountIndex(inputCountIndex);

        if (window instanceof TumblingGroupWindow) {
            TumblingGroupWindow tumblingWindow = (TumblingGroupWindow) window;
            FieldReferenceExpression timeField = tumblingWindow.timeField();
            ValueLiteralExpression size = tumblingWindow.size();
            if (isProctimeAttribute(timeField) && hasTimeIntervalType(size)) {
                builder = builder.tumble(toDuration(size)).withProcessingTime();
            } else if (isRowtimeAttribute(timeField) && hasTimeIntervalType(size)) {
                builder = builder.tumble(toDuration(size)).withEventTime(timeFieldIndex);
            } else if (isProctimeAttribute(timeField) && hasRowIntervalType(size)) {
                builder = builder.countWindow(toLong(size));
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
                builder = builder.sliding(toDuration(size), toDuration(slide)).withProcessingTime();
            } else if (isRowtimeAttribute(timeField) && hasTimeIntervalType(size)) {
                builder =
                        builder.sliding(toDuration(size), toDuration(slide))
                                .withEventTime(timeFieldIndex);
            } else if (isProctimeAttribute(timeField) && hasRowIntervalType(size)) {
                builder = builder.countWindow(toLong(size), toLong(slide));
            } else {
                // TODO: EventTimeTumblingGroupWindow should sort the stream on event time
                // before applying the  windowing logic. Otherwise, this would be the same as a
                // ProcessingTimeTumblingGroupWindow
                throw new UnsupportedOperationException(
                        "Event-time grouping windows on row intervals are currently not supported.");
            }
        } else if (window instanceof SessionGroupWindow) {
            SessionGroupWindow sessionWindow = (SessionGroupWindow) window;
            FieldReferenceExpression timeField = sessionWindow.timeField();
            ValueLiteralExpression gap = sessionWindow.gap();
            if (isProctimeAttribute(timeField)) {
                builder = builder.session(toDuration(gap)).withProcessingTime();
            } else if (isRowtimeAttribute(timeField)) {
                builder = builder.session(toDuration(gap)).withEventTime(timeFieldIndex);
            } else {
                throw new UnsupportedOperationException("This should not happen.");
            }
        } else {
            throw new TableException("Unsupported window: " + window.toString());
        }

        WindowEmitStrategy emitStrategy = WindowEmitStrategy.apply(config, window);
        if (emitStrategy.produceUpdates()) {
            // mark this operator will send retraction and set new trigger
            builder.produceUpdates()
                    .triggering(emitStrategy.getTrigger())
                    .withAllowedLateness(Duration.ofMillis(emitStrategy.getAllowLateness()));
        }

        if (aggsHandler instanceof GeneratedNamespaceAggsHandleFunction) {
            return builder.aggregate(
                            (GeneratedNamespaceAggsHandleFunction<?>) aggsHandler,
                            recordEqualiser,
                            accTypes,
                            aggValueTypes,
                            windowPropertyTypes)
                    .build();
        } else if (aggsHandler instanceof GeneratedNamespaceTableAggsHandleFunction) {
            return builder.aggregate(
                            (GeneratedNamespaceTableAggsHandleFunction<?>) aggsHandler,
                            accTypes,
                            aggValueTypes,
                            windowPropertyTypes)
                    .build();
        } else {
            throw new TableException(
                    "Unsupported agg handler class: " + aggsHandler.getClass().getSimpleName());
        }
    }
}
