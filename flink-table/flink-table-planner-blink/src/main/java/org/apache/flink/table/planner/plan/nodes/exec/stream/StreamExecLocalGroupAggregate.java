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
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.MiniBatchLocalGroupAggFunction;
import org.apache.flink.table.runtime.operators.bundle.MapBundleOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.core.AggregateCall;

import java.util.Arrays;
import java.util.Collections;

/** Stream {@link ExecNode} for unbounded local group aggregate. */
public class StreamExecLocalGroupAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData> {

    private final int[] grouping;
    private final AggregateCall[] aggCalls;
    /** Each element indicates whether the corresponding agg call needs `retract` method. */
    private final boolean[] aggCallNeedRetractions;
    /** Whether this node consumes retraction messages. */
    private final boolean needRetraction;

    public StreamExecLocalGroupAggregate(
            int[] grouping,
            AggregateCall[] aggCalls,
            boolean[] aggCallNeedRetractions,
            boolean needRetraction,
            ExecEdge inputEdge,
            RowType outputType,
            String description) {
        super(Collections.singletonList(inputEdge), outputType, description);
        this.grouping = grouping;
        this.aggCalls = aggCalls;
        this.aggCallNeedRetractions = aggCallNeedRetractions;
        this.needRetraction = needRetraction;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecNode<RowData> inputNode = (ExecNode<RowData>) getInputNodes().get(0);
        final Transformation<RowData> inputTransform = inputNode.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputNode.getOutputType();

        final AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                        new CodeGeneratorContext(planner.getTableConfig()),
                        planner.getRelBuilder(),
                        JavaScalaConversionUtil.toScala(inputRowType.getChildren()),
                        // the local aggregate result will be buffered, so need copy
                        true);
        generator.needAccumulate().needMerge(0, true, null);
        if (needRetraction) {
            generator.needRetract();
        }

        final AggregateInfoList aggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        inputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        aggCallNeedRetractions,
                        needRetraction,
                        false,
                        true);
        final GeneratedAggsHandleFunction aggsHandler =
                generator.generateAggsHandler("GroupAggsHandler", aggInfoList);
        final MiniBatchLocalGroupAggFunction aggFunction =
                new MiniBatchLocalGroupAggFunction(aggsHandler);

        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        grouping, (InternalTypeInfo<RowData>) inputTransform.getOutputType());

        final MapBundleOperator<RowData, RowData, RowData, RowData> operator =
                new MapBundleOperator<>(
                        aggFunction,
                        AggregateUtil.createMiniBatchTrigger(planner.getTableConfig()),
                        selector);

        final OneInputTransformation<RowData, RowData> transform =
                new OneInputTransformation<>(
                        inputTransform,
                        getDesc(),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism());

        if (inputsContainSingleton()) {
            transform.setParallelism(1);
            transform.setMaxParallelism(1);
        }

        return transform;
    }
}
