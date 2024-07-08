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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.batch.HashWindowCodeGenerator;
import org.apache.flink.table.planner.codegen.agg.batch.WindowCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedOperator;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecPythonGroupWindowAggregate.FIELD_NAME_INPUT_TIME_FIELD_INDEX;
import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortAggregate.FIELD_NAME_AGG_CALLS;
import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortAggregate.FIELD_NAME_AGG_INPUT_ROW_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortAggregate.FIELD_NAME_AUX_GROUPING;
import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortAggregate.FIELD_NAME_GROUPING;
import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortAggregate.FIELD_NAME_IS_FINAL;
import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortAggregate.FIELD_NAME_IS_MERGE;
import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortWindowAggregate.FIELD_NAME_ENABLE_ASSIGN_PANE;
import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortWindowAggregate.FIELD_NAME_INPUT_TIME_IS_DATE;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonGroupWindowAggregate.FIELD_NAME_NAMED_WINDOW_PROPERTIES;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonGroupWindowAggregate.FIELD_NAME_WINDOW;

/** Batch {@link ExecNode} for hash-based window aggregate operator. */
@ExecNodeMetadata(
        name = "batch-exec-hash-window-aggregate",
        version = 1,
        minPlanVersion = FlinkVersion.v1_20,
        minStateVersion = FlinkVersion.v1_20)
public class BatchExecHashWindowAggregate extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    @JsonProperty(FIELD_NAME_GROUPING)
    private final int[] grouping;

    @JsonProperty(FIELD_NAME_AUX_GROUPING)
    private final int[] auxGrouping;

    @JsonProperty(FIELD_NAME_AGG_CALLS)
    private final AggregateCall[] aggCalls;

    @JsonProperty(FIELD_NAME_WINDOW)
    private final LogicalWindow window;

    @JsonProperty(FIELD_NAME_INPUT_TIME_FIELD_INDEX)
    private final int inputTimeFieldIndex;

    @JsonProperty(FIELD_NAME_INPUT_TIME_IS_DATE)
    private final boolean inputTimeIsDate;

    @JsonProperty(FIELD_NAME_NAMED_WINDOW_PROPERTIES)
    private final NamedWindowProperty[] namedWindowProperties;

    @JsonProperty(FIELD_NAME_AGG_INPUT_ROW_TYPE)
    private final RowType aggInputRowType;

    @JsonProperty(FIELD_NAME_ENABLE_ASSIGN_PANE)
    private final boolean enableAssignPane;

    @JsonProperty(FIELD_NAME_IS_MERGE)
    private final boolean isMerge;

    @JsonProperty(FIELD_NAME_IS_FINAL)
    private final boolean isFinal;

    public BatchExecHashWindowAggregate(
            ReadableConfig tableConfig,
            int[] grouping,
            int[] auxGrouping,
            AggregateCall[] aggCalls,
            LogicalWindow window,
            int inputTimeFieldIndex,
            boolean inputTimeIsDate,
            NamedWindowProperty[] namedWindowProperties,
            RowType aggInputRowType,
            boolean enableAssignPane,
            boolean isMerge,
            boolean isFinal,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecHashWindowAggregate.class),
                ExecNodeContext.newPersistedConfig(BatchExecHashWindowAggregate.class, tableConfig),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.grouping = grouping;
        this.auxGrouping = auxGrouping;
        this.aggCalls = aggCalls;
        this.window = window;
        this.inputTimeFieldIndex = inputTimeFieldIndex;
        this.inputTimeIsDate = inputTimeIsDate;
        this.namedWindowProperties = namedWindowProperties;
        this.aggInputRowType = aggInputRowType;
        this.enableAssignPane = enableAssignPane;
        this.isMerge = isMerge;
        this.isFinal = isFinal;
    }

    @JsonCreator
    public BatchExecHashWindowAggregate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
            @JsonProperty(FIELD_NAME_AUX_GROUPING) int[] auxGrouping,
            @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
            @JsonProperty(FIELD_NAME_WINDOW) LogicalWindow window,
            @JsonProperty(FIELD_NAME_INPUT_TIME_FIELD_INDEX) int inputTimeFieldIndex,
            @JsonProperty(FIELD_NAME_INPUT_TIME_IS_DATE) boolean inputTimeIsDate,
            @JsonProperty(FIELD_NAME_NAMED_WINDOW_PROPERTIES)
                    NamedWindowProperty[] namedWindowProperties,
            @JsonProperty(FIELD_NAME_AGG_INPUT_ROW_TYPE) RowType aggInputRowType,
            @JsonProperty(FIELD_NAME_ENABLE_ASSIGN_PANE) boolean enableAssignPane,
            @JsonProperty(FIELD_NAME_IS_MERGE) boolean isMerge,
            @JsonProperty(FIELD_NAME_IS_FINAL) boolean isFinal,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTY) InputProperty inputProperty,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(
                id,
                context,
                persistedConfig,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.grouping = grouping;
        this.auxGrouping = auxGrouping;
        this.aggCalls = aggCalls;
        this.window = window;
        this.inputTimeFieldIndex = inputTimeFieldIndex;
        this.inputTimeIsDate = inputTimeIsDate;
        this.namedWindowProperties = namedWindowProperties;
        this.aggInputRowType = aggInputRowType;
        this.enableAssignPane = enableAssignPane;
        this.isMerge = isMerge;
        this.isFinal = isFinal;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        final AggregateInfoList aggInfos =
                AggregateUtil.transformToBatchAggregateInfoList(
                        planner.getTypeFactory(),
                        aggInputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        null, // aggCallNeedRetractions
                        null); // orderKeyIndexes
        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        final HashWindowCodeGenerator hashWindowCodeGenerator =
                new HashWindowCodeGenerator(
                        new CodeGeneratorContext(
                                config, planner.getFlinkContext().getClassLoader()),
                        planner.createRelBuilder(),
                        window,
                        inputTimeFieldIndex,
                        inputTimeIsDate,
                        JavaScalaConversionUtil.toScala(Arrays.asList(namedWindowProperties)),
                        aggInfos,
                        inputRowType,
                        grouping,
                        auxGrouping,
                        enableAssignPane,
                        isMerge,
                        isFinal);
        final int groupBufferLimitSize =
                config.get(ExecutionConfigOptions.TABLE_EXEC_WINDOW_AGG_BUFFER_SIZE_LIMIT);
        final Tuple2<Long, Long> windowSizeAndSlideSize = WindowCodeGenerator.getWindowDef(window);
        final GeneratedOperator<OneInputStreamOperator<RowData, RowData>> generatedOperator =
                hashWindowCodeGenerator.gen(
                        inputRowType,
                        (RowType) getOutputType(),
                        groupBufferLimitSize,
                        0, // windowStart
                        windowSizeAndSlideSize.f0,
                        windowSizeAndSlideSize.f1);

        final long managedMemory =
                config.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_HASH_AGG_MEMORY).getBytes();
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationName(config),
                createTransformationDescription(config),
                new CodeGenOperatorFactory<>(generatedOperator),
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism(),
                managedMemory,
                false);
    }
}
