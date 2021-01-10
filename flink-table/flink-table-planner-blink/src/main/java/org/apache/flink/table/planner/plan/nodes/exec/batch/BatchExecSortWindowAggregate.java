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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder.PlannerNamedWindowProperty;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.batch.SortWindowCodeGenerator;
import org.apache.flink.table.planner.codegen.agg.batch.WindowCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedOperator;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.core.AggregateCall;

import java.util.Arrays;
import java.util.Collections;

/** Batch {@link ExecNode} for sort-based window aggregate operator. */
public class BatchExecSortWindowAggregate extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData> {

    private final int[] grouping;
    private final int[] auxGrouping;
    private final AggregateCall[] aggCalls;
    private final LogicalWindow window;
    private final int inputTimeFieldIndex;
    private final boolean inputTimeIsDate;
    private final PlannerNamedWindowProperty[] namedWindowProperties;
    private final RowType aggInputRowType;
    private final boolean enableAssignPane;
    private final boolean isMerge;
    private final boolean isFinal;

    public BatchExecSortWindowAggregate(
            int[] grouping,
            int[] auxGrouping,
            AggregateCall[] aggCalls,
            LogicalWindow window,
            int inputTimeFieldIndex,
            boolean inputTimeIsDate,
            PlannerNamedWindowProperty[] namedWindowProperties,
            RowType aggInputRowType,
            boolean enableAssignPane,
            boolean isMerge,
            boolean isFinal,
            ExecEdge inputEdge,
            RowType outputType,
            String description) {
        super(Collections.singletonList(inputEdge), outputType, description);
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
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecNode<RowData> inputNode = (ExecNode<RowData>) getInputNodes().get(0);
        final Transformation<RowData> inputTransform = inputNode.translateToPlan(planner);

        final AggregateInfoList aggInfos =
                AggregateUtil.transformToBatchAggregateInfoList(
                        aggInputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        null,
                        null);

        final TableConfig tableConfig = planner.getTableConfig();
        final int groupBufferLimitSize =
                tableConfig
                        .getConfiguration()
                        .getInteger(ExecutionConfigOptions.TABLE_EXEC_WINDOW_AGG_BUFFER_SIZE_LIMIT);

        final Tuple2<Long, Long> windowSizeAndSlideSize = WindowCodeGenerator.getWindowDef(window);
        final SortWindowCodeGenerator windowCodeGenerator =
                new SortWindowCodeGenerator(
                        new CodeGeneratorContext(tableConfig),
                        planner.getRelBuilder(),
                        window,
                        inputTimeFieldIndex,
                        inputTimeIsDate,
                        JavaScalaConversionUtil.toScala(Arrays.asList(namedWindowProperties)),
                        aggInfos,
                        (RowType) inputNode.getOutputType(),
                        (RowType) getOutputType(),
                        groupBufferLimitSize,
                        0L,
                        windowSizeAndSlideSize.f0,
                        windowSizeAndSlideSize.f1,
                        grouping,
                        auxGrouping,
                        enableAssignPane,
                        isMerge,
                        isFinal);

        final GeneratedOperator<OneInputStreamOperator<RowData, RowData>> generatedOperator;
        if (grouping.length == 0) {
            generatedOperator = windowCodeGenerator.genWithoutKeys();
        } else {
            generatedOperator = windowCodeGenerator.genWithKeys();
        }

        return new OneInputTransformation<>(
                inputTransform,
                getDesc(),
                new CodeGenOperatorFactory<>(generatedOperator),
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism());
    }
}
