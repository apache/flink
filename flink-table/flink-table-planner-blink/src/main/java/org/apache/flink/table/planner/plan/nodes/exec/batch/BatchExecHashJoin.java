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
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.IntStream;

/** {@link BatchExecNode} for Hash Join. */
public class BatchExecHashJoin extends ExecNodeBase<RowData> implements BatchExecNode<RowData> {

    private final FlinkJoinType joinType;
    private final int[] leftKeys;
    private final int[] rightKeys;
    private final boolean[] filterNulls;
    private final @Nullable RexNode nonEquiConditions;
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
            @Nullable RexNode nonEquiConditions,
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
        this.joinType = joinType;

        Preconditions.checkNotNull(leftKeys);
        Preconditions.checkNotNull(rightKeys);
        Preconditions.checkNotNull(filterNulls);
        Preconditions.checkArgument(leftKeys.length == rightKeys.length);
        Preconditions.checkArgument(leftKeys.length == filterNulls.length);

        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.filterNulls = filterNulls;
        this.nonEquiConditions = nonEquiConditions;
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

        Transformation<RowData> lInput = leftInputNode.translateToPlan(planner);
        Transformation<RowData> rInput = rightInputNode.translateToPlan(planner);
        // get type
        RowType lType = (RowType) leftInputNode.getOutputType();
        RowType rType = (RowType) rightInputNode.getOutputType();

        LogicalType[] keyFieldTypes =
                IntStream.of(leftKeys).mapToObj(lType::getTypeAt).toArray(LogicalType[]::new);
        RowType keyType = RowType.of(keyFieldTypes);

        TableConfig config = planner.getTableConfig();
        GeneratedJoinCondition condFunc =
                JoinUtil.generateConditionFunction(config, nonEquiConditions, lType, rType);

        // projection for equals
        GeneratedProjection lProj =
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(config),
                        "HashJoinLeftProjection",
                        lType,
                        keyType,
                        leftKeys);
        GeneratedProjection rProj =
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(config),
                        "HashJoinRightProjection",
                        rType,
                        keyType,
                        rightKeys);

        Transformation<RowData> build;
        Transformation<RowData> probe;
        GeneratedProjection bProj;
        GeneratedProjection pProj;
        int[] bKeys;
        int[] pKeys;
        RowType bType;
        RowType pType;
        int bRowSize;
        long bRowCount;
        long pRowCount;
        boolean reverseJoin = !leftIsBuild;
        if (leftIsBuild) {
            build = lInput;
            bProj = lProj;
            bType = lType;
            bRowSize = estimatedLeftAvgRowSize;
            bRowCount = estimatedLeftRowCount;
            bKeys = leftKeys;

            probe = rInput;
            pProj = rProj;
            pType = rType;
            pRowCount = estimatedLeftRowCount;
            pKeys = rightKeys;
        } else {
            build = rInput;
            bProj = rProj;
            bType = rType;
            bRowSize = estimatedRightAvgRowSize;
            bRowCount = estimatedRightRowCount;
            bKeys = rightKeys;

            probe = lInput;
            pProj = lProj;
            pType = lType;
            pRowCount = estimatedLeftRowCount;
            pKeys = leftKeys;
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
                            bType,
                            pType,
                            bKeys,
                            pKeys,
                            bRowSize,
                            bRowCount,
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
                                    bProj,
                                    pProj,
                                    tryDistinctBuildRow,
                                    bRowSize,
                                    bRowCount,
                                    pRowCount,
                                    keyType));
        }

        long managedMemory =
                ExecNodeUtil.getMemorySize(
                        config, ExecutionConfigOptions.TABLE_EXEC_RESOURCE_HASH_JOIN_MEMORY);

        TwoInputTransformation<RowData, RowData, RowData> ret =
                ExecNodeUtil.createTwoInputTransformation(
                        build,
                        probe,
                        getDesc(),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        probe.getParallelism(),
                        managedMemory);

        if (inputsContainSingleton()) {
            ret.setParallelism(1);
            ret.setMaxParallelism(1);
        }
        return ret;
    }
}
