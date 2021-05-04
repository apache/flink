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
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.RecordsWindowBuffer;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.WindowBuffer;
import org.apache.flink.table.runtime.operators.aggregate.window.combines.LocalAggCombiner;
import org.apache.flink.table.runtime.operators.window.slicing.SliceAssigner;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.tools.RelBuilder;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for window table-valued based local aggregate. */
public class StreamExecLocalWindowAggregate extends StreamExecWindowAggregateBase {

    private static final long WINDOW_AGG_MEMORY_RATIO = 100;

    public static final String FIELD_NAME_WINDOWING = "windowing";

    @JsonProperty(FIELD_NAME_GROUPING)
    private final int[] grouping;

    @JsonProperty(FIELD_NAME_AGG_CALLS)
    private final AggregateCall[] aggCalls;

    @JsonProperty(FIELD_NAME_WINDOWING)
    private final WindowingStrategy windowing;

    public StreamExecLocalWindowAggregate(
            int[] grouping,
            AggregateCall[] aggCalls,
            WindowingStrategy windowing,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                grouping,
                aggCalls,
                windowing,
                getNewNodeId(),
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecLocalWindowAggregate(
            @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
            @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
            @JsonProperty(FIELD_NAME_WINDOWING) WindowingStrategy windowing,
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, inputProperties, outputType, description);
        this.grouping = checkNotNull(grouping);
        this.aggCalls = checkNotNull(aggCalls);
        this.windowing = checkNotNull(windowing);
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

        final AggregateInfoList aggInfoList =
                AggregateUtil.deriveStreamWindowAggregateInfoList(
                        inputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        windowing.getWindow(),
                        false); // isStateBackendDataViews

        final GeneratedNamespaceAggsHandleFunction<Long> generatedAggsHandler =
                createAggsHandler(
                        sliceAssigner,
                        aggInfoList,
                        config,
                        planner.getRelBuilder(),
                        inputRowType.getChildren(),
                        shiftTimeZone);
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(grouping, InternalTypeInfo.of(inputRowType));

        PagedTypeSerializer<RowData> keySer =
                (PagedTypeSerializer<RowData>) selector.getProducedType().toSerializer();
        AbstractRowDataSerializer<RowData> valueSer = new RowDataSerializer(inputRowType);

        WindowBuffer.LocalFactory bufferFactory =
                new RecordsWindowBuffer.LocalFactory(
                        keySer, valueSer, new LocalAggCombiner.Factory(generatedAggsHandler));

        final OneInputStreamOperator<RowData, RowData> localAggOperator =
                new LocalSlicingWindowAggOperator(
                        selector, sliceAssigner, bufferFactory, shiftTimeZone);

        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                getDescription(),
                SimpleOperatorFactory.of(localAggOperator),
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism(),
                // use less memory here to let the chained head operator can have more memory
                WINDOW_AGG_MEMORY_RATIO / 2);
    }

    private GeneratedNamespaceAggsHandleFunction<Long> createAggsHandler(
            SliceAssigner sliceAssigner,
            AggregateInfoList aggInfoList,
            TableConfig config,
            RelBuilder relBuilder,
            List<LogicalType> fieldTypes,
            ZoneId shiftTimeZone) {
        final AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                                new CodeGeneratorContext(config),
                                relBuilder,
                                JavaScalaConversionUtil.toScala(fieldTypes),
                                true) // copyInputField
                        .needAccumulate()
                        .needMerge(0, true, null);

        return generator.generateNamespaceAggsHandler(
                "LocalWindowAggsHandler",
                aggInfoList,
                JavaScalaConversionUtil.toScala(Collections.emptyList()),
                sliceAssigner,
                shiftTimeZone);
    }
}
