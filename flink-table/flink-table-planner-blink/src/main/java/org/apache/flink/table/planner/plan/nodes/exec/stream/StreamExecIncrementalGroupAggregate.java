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
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
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
import org.apache.flink.table.runtime.operators.aggregate.MiniBatchIncrementalGroupAggFunction;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.tools.RelBuilder;

import java.util.Arrays;
import java.util.Collections;

/** Stream {@link ExecNode} for unbounded incremental group aggregate. */
public class StreamExecIncrementalGroupAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData> {

    /** The partial agg's grouping. */
    private final int[] partialAggGrouping;
    /** The final agg's grouping. */
    private final int[] finalAggGrouping;
    /** The partial agg's original agg calls. */
    private final AggregateCall[] partialOriginalAggCalls;
    /** Each element indicates whether the corresponding agg call needs `retract` method. */
    private final boolean[] partialAggCallNeedRetractions;
    /** The input row type of this node's partial local agg. */
    private final RowType partialLocalAggInputRowType;
    /** Whether this node consumes retraction messages. */
    private final boolean partialAggNeedRetraction;

    public StreamExecIncrementalGroupAggregate(
            int[] partialAggGrouping,
            int[] finalAggGrouping,
            AggregateCall[] partialOriginalAggCalls,
            boolean[] partialAggCallNeedRetractions,
            RowType partialLocalAggInputRowType,
            boolean partialAggNeedRetraction,
            ExecEdge inputEdge,
            RowType outputType,
            String description) {
        super(Collections.singletonList(inputEdge), outputType, description);
        this.partialAggGrouping = partialAggGrouping;
        this.finalAggGrouping = finalAggGrouping;
        this.partialOriginalAggCalls = partialOriginalAggCalls;
        this.partialAggCallNeedRetractions = partialAggCallNeedRetractions;
        this.partialLocalAggInputRowType = partialLocalAggInputRowType;
        this.partialAggNeedRetraction = partialAggNeedRetraction;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final TableConfig config = planner.getTableConfig();
        final ExecNode<RowData> inputNode = (ExecNode<RowData>) getInputNodes().get(0);
        final Transformation<RowData> inputTransform = inputNode.translateToPlan(planner);

        final AggregateInfoList partialLocalAggInfoList =
                AggregateUtil.createPartialAggInfoList(
                        partialLocalAggInputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(partialOriginalAggCalls)),
                        partialAggCallNeedRetractions,
                        partialAggNeedRetraction,
                        false);

        final GeneratedAggsHandleFunction partialAggsHandler =
                generateAggsHandler(
                        "PartialGroupAggsHandler",
                        partialLocalAggInfoList,
                        partialAggGrouping.length,
                        partialLocalAggInfoList.getAccTypes(),
                        config,
                        planner.getRelBuilder(),
                        // the partial aggregate accumulators will be buffered, so need copy
                        true);

        final AggregateInfoList incrementalAggInfo =
                AggregateUtil.createIncrementalAggInfoList(
                        partialLocalAggInputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(partialOriginalAggCalls)),
                        partialAggCallNeedRetractions,
                        partialAggNeedRetraction);

        final GeneratedAggsHandleFunction finalAggsHandler =
                generateAggsHandler(
                        "FinalGroupAggsHandler",
                        incrementalAggInfo,
                        0,
                        partialLocalAggInfoList.getAccTypes(),
                        config,
                        planner.getRelBuilder(),
                        // the final aggregate accumulators is not buffered
                        false);

        final RowDataKeySelector partialKeySelector =
                KeySelectorUtil.getRowDataSelector(
                        partialAggGrouping, InternalTypeInfo.of(inputNode.getOutputType()));
        final RowDataKeySelector finalKeySelector =
                KeySelectorUtil.getRowDataSelector(
                        finalAggGrouping, partialKeySelector.getProducedType());

        final MiniBatchIncrementalGroupAggFunction aggFunction =
                new MiniBatchIncrementalGroupAggFunction(
                        partialAggsHandler,
                        finalAggsHandler,
                        finalKeySelector,
                        config.getIdleStateRetention().toMillis());

        final OneInputStreamOperator<RowData, RowData> operator =
                new KeyedMapBundleOperator<>(
                        aggFunction, AggregateUtil.createMiniBatchTrigger(config));

        // partitioned aggregation
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

        // set KeyType and Selector for state
        transform.setStateKeySelector(partialKeySelector);
        transform.setStateKeyType(partialKeySelector.getProducedType());
        return transform;
    }

    private GeneratedAggsHandleFunction generateAggsHandler(
            String name,
            AggregateInfoList aggInfoList,
            int mergedAccOffset,
            DataType[] mergedAccExternalTypes,
            TableConfig config,
            RelBuilder relBuilder,
            boolean inputFieldCopy) {

        AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                        new CodeGeneratorContext(config),
                        relBuilder,
                        JavaScalaConversionUtil.toScala(partialLocalAggInputRowType.getChildren()),
                        inputFieldCopy);

        return generator
                .needMerge(mergedAccOffset, true, mergedAccExternalTypes)
                .generateAggsHandler(name, aggInfoList);
    }
}
