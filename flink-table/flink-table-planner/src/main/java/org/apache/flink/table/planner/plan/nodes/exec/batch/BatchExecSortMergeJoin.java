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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.AdaptiveJoinExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.OperatorType;
import org.apache.flink.table.planner.plan.utils.SorMergeJoinOperatorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link BatchExecNode} for Sort Merge Join. */
public class BatchExecSortMergeJoin extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>,
                SingleTransformationTranslator<RowData>,
                AdaptiveJoinExecNode {

    private final FlinkJoinType joinType;
    private final int[] leftKeys;
    private final int[] rightKeys;
    private final boolean[] filterNulls;
    private final @Nullable RexNode nonEquiCondition;
    private final int estimatedLeftAvgRowSize;
    private final int estimatedRightAvgRowSize;
    private final long estimatedLeftRowCount;
    private final long estimatedRightRowCount;
    private final boolean leftIsSmaller;
    private final boolean withJobStrategyHint;

    public BatchExecSortMergeJoin(
            ReadableConfig tableConfig,
            FlinkJoinType joinType,
            int[] leftKeys,
            int[] rightKeys,
            boolean[] filterNulls,
            @Nullable RexNode nonEquiCondition,
            int estimatedLeftAvgRowSize,
            int estimatedRightAvgRowSize,
            long estimatedLeftRowCount,
            long estimatedRightRowCount,
            boolean leftIsSmaller,
            InputProperty leftInputProperty,
            InputProperty rightInputProperty,
            RowType outputType,
            boolean withJobStrategyHint,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecSortMergeJoin.class),
                ExecNodeContext.newPersistedConfig(BatchExecSortMergeJoin.class, tableConfig),
                Arrays.asList(leftInputProperty, rightInputProperty),
                outputType,
                description);
        this.joinType = checkNotNull(joinType);
        this.leftKeys = checkNotNull(leftKeys);
        this.rightKeys = checkNotNull(rightKeys);
        this.filterNulls = checkNotNull(filterNulls);
        checkArgument(leftKeys.length > 0 && leftKeys.length == rightKeys.length);
        checkArgument(leftKeys.length == filterNulls.length);

        this.nonEquiCondition = nonEquiCondition;
        this.estimatedLeftAvgRowSize = estimatedLeftAvgRowSize;
        this.estimatedRightAvgRowSize = estimatedRightAvgRowSize;
        this.estimatedLeftRowCount = estimatedLeftRowCount;
        this.estimatedRightRowCount = estimatedRightRowCount;
        this.leftIsSmaller = leftIsSmaller;
        this.withJobStrategyHint = withJobStrategyHint;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        ExecEdge leftInputEdge = getInputEdges().get(0);
        ExecEdge rightInputEdge = getInputEdges().get(1);

        // get input types
        RowType leftType = (RowType) leftInputEdge.getOutputType();
        RowType rightType = (RowType) rightInputEdge.getOutputType();

        long externalBufferMemory =
                config.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_EXTERNAL_BUFFER_MEMORY)
                        .getBytes();
        long sortMemory =
                config.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_SORT_MEMORY).getBytes();
        int externalBufferNum = 1;
        if (joinType == FlinkJoinType.FULL) {
            externalBufferNum = 2;
        }

        long managedMemory = externalBufferMemory * externalBufferNum + sortMemory * 2;

        GeneratedJoinCondition condFunc =
                JoinUtil.generateConditionFunction(
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        nonEquiCondition,
                        leftType,
                        rightType);

        StreamOperatorFactory<RowData> sortMergeJoinOperatorFactory =
                SorMergeJoinOperatorUtil.generateOperatorFactory(
                        condFunc,
                        leftType,
                        rightType,
                        leftKeys,
                        rightKeys,
                        joinType,
                        config,
                        leftIsSmaller,
                        filterNulls,
                        managedMemory,
                        planner.getFlinkContext().getClassLoader());

        Transformation<RowData> leftInputTransform =
                (Transformation<RowData>) leftInputEdge.translateToPlan(planner);
        Transformation<RowData> rightInputTransform =
                (Transformation<RowData>) rightInputEdge.translateToPlan(planner);
        return ExecNodeUtil.createTwoInputTransformation(
                leftInputTransform,
                rightInputTransform,
                createTransformationName(config),
                createTransformationDescription(config),
                sortMergeJoinOperatorFactory,
                InternalTypeInfo.of(getOutputType()),
                rightInputTransform.getParallelism(),
                managedMemory,
                false);
    }

    @Override
    public boolean canBeTransformedToAdaptiveJoin() {
        return !withJobStrategyHint && joinType != FlinkJoinType.FULL;
    }

    @Override
    public BatchExecAdaptiveJoin toAdaptiveJoinNode() {
        return new BatchExecAdaptiveJoin(
                getPersistedConfig(),
                new JoinSpec(joinType, leftKeys, rightKeys, filterNulls, nonEquiCondition),
                estimatedLeftAvgRowSize,
                estimatedRightAvgRowSize,
                estimatedLeftRowCount,
                estimatedRightRowCount,
                leftIsSmaller,
                false,
                getInputProperties(),
                (RowType) getOutputType(),
                getDescription(),
                OperatorType.SortMergeJoin);
    }
}
