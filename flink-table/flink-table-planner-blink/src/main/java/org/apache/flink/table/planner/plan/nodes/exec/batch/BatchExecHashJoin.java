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
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.LongHashJoinGenerator;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.HashJoinOperator;
import org.apache.flink.table.runtime.operators.join.HashJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link BatchExecNode} for Hash Join. */
public class BatchExecHashJoin extends ExecNodeBase<RowData> implements BatchExecNode<RowData> {

    private final FlinkJoinType joinType;
    private final int[] leftKeys;
    private final int[] rightKeys;
    private final boolean[] filterNulls;
    private final @Nullable RexNode nonEquiCondition;
    private final boolean leftIsBuild;
    private final int estimatedLeftAvgRowSize;
    private final int estimatedRightAvgRowSize;
    private final long estimatedLeftRowCount;
    private final long estimatedRightRowCount;
    private final boolean tryDistinctBuildRow;

    public BatchExecHashJoin(
            FlinkJoinType joinType,
            int[] leftKeys,
            int[] rightKeys,
            boolean[] filterNulls,
            @Nullable RexNode nonEquiCondition,
            int estimatedLeftAvgRowSize,
            int estimatedRightAvgRowSize,
            long estimatedLeftRowCount,
            long estimatedRightRowCount,
            boolean leftIsBuild,
            boolean tryDistinctBuildRow,
            List<ExecEdge> inputEdges,
            RowType outputType,
            String description) {
        super(inputEdges, outputType, description);
        this.joinType = checkNotNull(joinType);
        this.leftKeys = checkNotNull(leftKeys);
        this.rightKeys = checkNotNull(rightKeys);
        this.filterNulls = checkNotNull(filterNulls);
        checkArgument(leftKeys.length > 0 && leftKeys.length == rightKeys.length);
        checkArgument(leftKeys.length == filterNulls.length);

        this.nonEquiCondition = nonEquiCondition;
        this.leftIsBuild = leftIsBuild;
        this.estimatedLeftAvgRowSize = estimatedLeftAvgRowSize;
        this.estimatedRightAvgRowSize = estimatedRightAvgRowSize;
        this.estimatedLeftRowCount = estimatedLeftRowCount;
        this.estimatedRightRowCount = estimatedRightRowCount;
        this.tryDistinctBuildRow = tryDistinctBuildRow;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        ExecNode<RowData> leftInputNode = (ExecNode<RowData>) getInputNodes().get(0);
        ExecNode<RowData> rightInputNode = (ExecNode<RowData>) getInputNodes().get(1);

        Transformation<RowData> leftInputTransform = leftInputNode.translateToPlan(planner);
        Transformation<RowData> rightInputTransform = rightInputNode.translateToPlan(planner);
        // get type
        RowType leftType = (RowType) leftInputNode.getOutputType();
        RowType rightType = (RowType) rightInputNode.getOutputType();

        LogicalType[] keyFieldTypes =
                IntStream.of(leftKeys).mapToObj(leftType::getTypeAt).toArray(LogicalType[]::new);
        RowType keyType = RowType.of(keyFieldTypes);

        TableConfig config = planner.getTableConfig();
        GeneratedJoinCondition condFunc =
                JoinUtil.generateConditionFunction(config, nonEquiCondition, leftType, rightType);

        // projection for equals
        GeneratedProjection leftProj =
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(config),
                        "HashJoinLeftProjection",
                        leftType,
                        keyType,
                        leftKeys);
        GeneratedProjection rightProj =
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(config),
                        "HashJoinRightProjection",
                        rightType,
                        keyType,
                        rightKeys);

        Transformation<RowData> buildTransform;
        Transformation<RowData> probeTransform;
        GeneratedProjection buildProj;
        GeneratedProjection probeProj;
        int[] buildKeys;
        int[] probeKeys;
        RowType buildType;
        RowType probeType;
        int buildRowSize;
        long buildRowCount;
        long probeRowCount;
        boolean reverseJoin = !leftIsBuild;
        if (leftIsBuild) {
            buildTransform = leftInputTransform;
            buildProj = leftProj;
            buildType = leftType;
            buildRowSize = estimatedLeftAvgRowSize;
            buildRowCount = estimatedLeftRowCount;
            buildKeys = leftKeys;

            probeTransform = rightInputTransform;
            probeProj = rightProj;
            probeType = rightType;
            probeRowCount = estimatedLeftRowCount;
            probeKeys = rightKeys;
        } else {
            buildTransform = rightInputTransform;
            buildProj = rightProj;
            buildType = rightType;
            buildRowSize = estimatedRightAvgRowSize;
            buildRowCount = estimatedRightRowCount;
            buildKeys = rightKeys;

            probeTransform = leftInputTransform;
            probeProj = leftProj;
            probeType = leftType;
            probeRowCount = estimatedLeftRowCount;
            probeKeys = leftKeys;
        }

        // operator
        StreamOperatorFactory<RowData> operator;
        HashJoinType hashJoinType =
                HashJoinType.of(
                        leftIsBuild,
                        joinType.isLeftOuter(),
                        joinType.isRightOuter(),
                        joinType == FlinkJoinType.SEMI,
                        joinType == FlinkJoinType.ANTI);
        if (LongHashJoinGenerator.support(hashJoinType, keyType, filterNulls)) {
            operator =
                    LongHashJoinGenerator.gen(
                            config,
                            hashJoinType,
                            keyType,
                            buildType,
                            probeType,
                            buildKeys,
                            probeKeys,
                            buildRowSize,
                            buildRowCount,
                            reverseJoin,
                            condFunc);
        } else {
            operator =
                    SimpleOperatorFactory.of(
                            HashJoinOperator.newHashJoinOperator(
                                    hashJoinType,
                                    condFunc,
                                    reverseJoin,
                                    filterNulls,
                                    buildProj,
                                    probeProj,
                                    tryDistinctBuildRow,
                                    buildRowSize,
                                    buildRowCount,
                                    probeRowCount,
                                    keyType));
        }

        long managedMemory =
                ExecNodeUtil.getMemorySize(
                        config, ExecutionConfigOptions.TABLE_EXEC_RESOURCE_HASH_JOIN_MEMORY);

        TwoInputTransformation<RowData, RowData, RowData> transform =
                ExecNodeUtil.createTwoInputTransformation(
                        buildTransform,
                        probeTransform,
                        getDesc(),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        probeTransform.getParallelism(),
                        managedMemory);

        if (inputsContainSingleton()) {
            transform.setParallelism(1);
            transform.setMaxParallelism(1);
        }
        return transform;
    }
}
