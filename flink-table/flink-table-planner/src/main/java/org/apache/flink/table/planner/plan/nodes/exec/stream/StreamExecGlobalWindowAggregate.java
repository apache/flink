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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.expressions.PlannerNamedWindowProperty;
import org.apache.flink.table.planner.expressions.PlannerWindowProperty;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonDeserializer;
import org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.window.SlicingWindowAggOperatorBuilder;
import org.apache.flink.table.runtime.operators.window.slicing.SliceAssigner;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.tools.RelBuilder;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for window table-valued based global aggregate. */
public class StreamExecGlobalWindowAggregate extends StreamExecWindowAggregateBase {

    public static final String FIELD_NAME_LOCAL_AGG_INPUT_ROW_TYPE = "localAggInputRowType";

    @JsonProperty(FIELD_NAME_GROUPING)
    private final int[] grouping;

    @JsonProperty(FIELD_NAME_AGG_CALLS)
    private final AggregateCall[] aggCalls;

    @JsonProperty(FIELD_NAME_WINDOWING)
    private final WindowingStrategy windowing;

    @JsonProperty(FIELD_NAME_NAMED_WINDOW_PROPERTIES)
    private final PlannerNamedWindowProperty[] namedWindowProperties;

    /** The input row type of this node's local agg. */
    @JsonProperty(FIELD_NAME_LOCAL_AGG_INPUT_ROW_TYPE)
    @JsonSerialize(using = LogicalTypeJsonSerializer.class)
    @JsonDeserialize(using = LogicalTypeJsonDeserializer.class)
    private final RowType localAggInputRowType;

    public StreamExecGlobalWindowAggregate(
            int[] grouping,
            AggregateCall[] aggCalls,
            WindowingStrategy windowing,
            PlannerNamedWindowProperty[] namedWindowProperties,
            InputProperty inputProperty,
            RowType localAggInputRowType,
            RowType outputType,
            String description) {
        this(
                grouping,
                aggCalls,
                windowing,
                namedWindowProperties,
                getNewNodeId(),
                Collections.singletonList(inputProperty),
                localAggInputRowType,
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecGlobalWindowAggregate(
            @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
            @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
            @JsonProperty(FIELD_NAME_WINDOWING) WindowingStrategy windowing,
            @JsonProperty(FIELD_NAME_NAMED_WINDOW_PROPERTIES)
                    PlannerNamedWindowProperty[] namedWindowProperties,
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_LOCAL_AGG_INPUT_ROW_TYPE) RowType localAggInputRowType,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, inputProperties, outputType, description);
        this.grouping = checkNotNull(grouping);
        this.aggCalls = checkNotNull(aggCalls);
        this.windowing = checkNotNull(windowing);
        this.namedWindowProperties = checkNotNull(namedWindowProperties);
        this.localAggInputRowType = localAggInputRowType;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();

        final TableConfig config = planner.getTableConfig();
        final ZoneId shiftTimeZone =
                TimeWindowUtil.getShiftTimeZone(windowing.getTimeAttributeType(), config);
        final SliceAssigner sliceAssigner = createSliceAssigner(windowing, shiftTimeZone);

        final AggregateInfoList localAggInfoList =
                AggregateUtil.deriveStreamWindowAggregateInfoList(
                        localAggInputRowType, // should use original input here
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        windowing.getWindow(),
                        false); // isStateBackendDataViews

        final AggregateInfoList globalAggInfoList =
                AggregateUtil.deriveStreamWindowAggregateInfoList(
                        localAggInputRowType, // should use original input here
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        windowing.getWindow(),
                        true); // isStateBackendDataViews

        // handler used to merge multiple local accumulators into one accumulator,
        // where the accumulators are all on memory
        final GeneratedNamespaceAggsHandleFunction<Long> localAggsHandler =
                createAggsHandler(
                        "LocalWindowAggsHandler",
                        sliceAssigner,
                        localAggInfoList,
                        grouping.length,
                        true,
                        localAggInfoList.getAccTypes(),
                        config,
                        planner.getRelBuilder(),
                        shiftTimeZone);

        // handler used to merge the single local accumulator (on memory) into state accumulator
        final GeneratedNamespaceAggsHandleFunction<Long> globalAggsHandler =
                createAggsHandler(
                        "GlobalWindowAggsHandler",
                        sliceAssigner,
                        globalAggInfoList,
                        0,
                        true,
                        localAggInfoList.getAccTypes(),
                        config,
                        planner.getRelBuilder(),
                        shiftTimeZone);

        // handler used to merge state accumulators for merging slices into window,
        // e.g. Hop and Cumulate
        final GeneratedNamespaceAggsHandleFunction<Long> stateAggsHandler =
                createAggsHandler(
                        "StateWindowAggsHandler",
                        sliceAssigner,
                        globalAggInfoList,
                        0,
                        false,
                        globalAggInfoList.getAccTypes(),
                        config,
                        planner.getRelBuilder(),
                        shiftTimeZone);

        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(grouping, InternalTypeInfo.of(inputRowType));
        final LogicalType[] accTypes = convertToLogicalTypes(globalAggInfoList.getAccTypes());

        final OneInputStreamOperator<RowData, RowData> windowOperator =
                SlicingWindowAggOperatorBuilder.builder()
                        .inputSerializer(new RowDataSerializer(inputRowType))
                        .shiftTimeZone(shiftTimeZone)
                        .keySerializer(
                                (PagedTypeSerializer<RowData>)
                                        selector.getProducedType().toSerializer())
                        .assigner(sliceAssigner)
                        .countStarIndex(globalAggInfoList.getIndexOfCountStar())
                        .globalAggregate(
                                localAggsHandler,
                                globalAggsHandler,
                                stateAggsHandler,
                                new RowDataSerializer(accTypes))
                        .build();

        final OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        getDescription(),
                        SimpleOperatorFactory.of(windowOperator),
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism(),
                        WINDOW_AGG_MEMORY_RATIO);

        // set KeyType and Selector for state
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());
        return transform;
    }

    private GeneratedNamespaceAggsHandleFunction<Long> createAggsHandler(
            String name,
            SliceAssigner sliceAssigner,
            AggregateInfoList aggInfoList,
            int mergedAccOffset,
            boolean mergedAccIsOnHeap,
            DataType[] mergedAccExternalTypes,
            TableConfig config,
            RelBuilder relBuilder,
            ZoneId shifTimeZone) {
        final AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                                new CodeGeneratorContext(config),
                                relBuilder,
                                JavaScalaConversionUtil.toScala(localAggInputRowType.getChildren()),
                                true) // copyInputField
                        .needAccumulate()
                        .needMerge(mergedAccOffset, mergedAccIsOnHeap, mergedAccExternalTypes);

        final List<PlannerWindowProperty> windowProperties =
                Arrays.asList(
                        Arrays.stream(namedWindowProperties)
                                .map(PlannerNamedWindowProperty::getProperty)
                                .toArray(PlannerWindowProperty[]::new));

        return generator.generateNamespaceAggsHandler(
                name,
                aggInfoList,
                JavaScalaConversionUtil.toScala(windowProperties),
                sliceAssigner,
                shifTimeZone);
    }
}
