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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.batch.AggWithoutKeysCodeGenerator;
import org.apache.flink.table.planner.codegen.agg.batch.SortAggCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
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

/** Batch {@link ExecNode} for (global) sort-based aggregate operator. */
public class BatchExecSortAggregate extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData> {

    private final int[] grouping;
    private final int[] auxGrouping;
    private final AggregateCall[] aggCalls;
    private final RowType aggInputRowType;
    private final boolean isMerge;
    private final boolean isFinal;

    public BatchExecSortAggregate(
            int[] grouping,
            int[] auxGrouping,
            AggregateCall[] aggCalls,
            RowType aggInputRowType,
            boolean isMerge,
            boolean isFinal,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(Collections.singletonList(inputProperty), outputType, description);
        this.grouping = grouping;
        this.auxGrouping = auxGrouping;
        this.aggCalls = aggCalls;
        this.aggInputRowType = aggInputRowType;
        this.isMerge = isMerge;
        this.isFinal = isFinal;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        final RowType outputRowType = (RowType) getOutputType();

        final CodeGeneratorContext ctx = new CodeGeneratorContext(planner.getTableConfig());
        final AggregateInfoList aggInfos =
                AggregateUtil.transformToBatchAggregateInfoList(
                        aggInputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        null,
                        null);

        final GeneratedOperator<OneInputStreamOperator<RowData, RowData>> generatedOperator;
        if (grouping.length == 0) {
            generatedOperator =
                    AggWithoutKeysCodeGenerator.genWithoutKeys(
                            ctx,
                            planner.getRelBuilder(),
                            aggInfos,
                            inputRowType,
                            outputRowType,
                            isMerge,
                            isFinal,
                            "NoGrouping");
        } else {
            generatedOperator =
                    SortAggCodeGenerator.genWithKeys(
                            ctx,
                            planner.getRelBuilder(),
                            aggInfos,
                            inputRowType,
                            outputRowType,
                            grouping,
                            auxGrouping,
                            isMerge,
                            isFinal);
        }

        return new OneInputTransformation<>(
                inputTransform,
                getDescription(),
                new CodeGenOperatorFactory<>(generatedOperator),
                InternalTypeInfo.of(outputRowType),
                inputTransform.getParallelism());
    }
}
