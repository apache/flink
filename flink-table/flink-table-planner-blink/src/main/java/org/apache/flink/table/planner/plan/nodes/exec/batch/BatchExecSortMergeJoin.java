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
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.SortSpec;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.SortUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.SortMergeJoinOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.IntStream;

/** {@link BatchExecNode} for Sort Merge Join. */
public class BatchExecSortMergeJoin extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData> {

    private final FlinkJoinType joinType;
    private final int[] leftKeys;
    private final int[] rightKeys;
    private final boolean[] filterNulls;
    private final @Nullable RexNode nonEquiConditions;
    private final boolean leftIsSmaller;

    public BatchExecSortMergeJoin(
            FlinkJoinType joinType,
            int[] leftKeys,
            int[] rightKeys,
            boolean[] filterNulls,
            @Nullable RexNode nonEquiConditions,
            boolean leftIsSmaller,
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
        this.leftIsSmaller = leftIsSmaller;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        ExecNode<RowData> leftInputNode = (ExecNode<RowData>) getInputNodes().get(0);
        ExecNode<RowData> rightInputNode = (ExecNode<RowData>) getInputNodes().get(1);

        // get type
        RowType lType = (RowType) leftInputNode.getOutputType();
        RowType rType = (RowType) rightInputNode.getOutputType();

        LogicalType[] keyFieldTypes =
                IntStream.of(leftKeys).mapToObj(lType::getTypeAt).toArray(LogicalType[]::new);
        RowType keyType = RowType.of(keyFieldTypes);

        TableConfig config = planner.getTableConfig();
        GeneratedJoinCondition condFunc =
                JoinUtil.generateConditionFunction(config, nonEquiConditions, lType, rType);

        long externalBufferMemory =
                ExecNodeUtil.getMemorySize(
                        config, ExecutionConfigOptions.TABLE_EXEC_RESOURCE_EXTERNAL_BUFFER_MEMORY);
        long sortMemory =
                ExecNodeUtil.getMemorySize(
                        config, ExecutionConfigOptions.TABLE_EXEC_RESOURCE_SORT_MEMORY);
        int externalBufferNum = 1;
        if (joinType == FlinkJoinType.FULL) {
            externalBufferNum = 2;
        }

        long managedMemory = externalBufferMemory * externalBufferNum + sortMemory * 2;

        SortCodeGenerator leftSortGen = newSortGen(config, leftKeys, lType);
        SortCodeGenerator rightSortGen = newSortGen(config, rightKeys, rType);

        int[] keyPositions = IntStream.range(0, leftKeys.length).toArray();
        SortMergeJoinOperator operator =
                new SortMergeJoinOperator(
                        1.0 * externalBufferMemory / managedMemory,
                        joinType,
                        leftIsSmaller,
                        condFunc,
                        ProjectionCodeGenerator.generateProjection(
                                new CodeGeneratorContext(config),
                                "SMJProjection",
                                lType,
                                keyType,
                                leftKeys),
                        ProjectionCodeGenerator.generateProjection(
                                new CodeGeneratorContext(config),
                                "SMJProjection",
                                rType,
                                keyType,
                                rightKeys),
                        leftSortGen.generateNormalizedKeyComputer("LeftComputer"),
                        leftSortGen.generateRecordComparator("LeftComparator"),
                        rightSortGen.generateNormalizedKeyComputer("RightComputer"),
                        rightSortGen.generateRecordComparator("RightComparator"),
                        newSortGen(config, keyPositions, keyType)
                                .generateRecordComparator("KeyComparator"),
                        filterNulls);

        Transformation<RowData> lInput = leftInputNode.translateToPlan(planner);
        Transformation<RowData> rInput = rightInputNode.translateToPlan(planner);
        TwoInputTransformation<RowData, RowData, RowData> ret =
                ExecNodeUtil.createTwoInputTransformation(
                        lInput,
                        rInput,
                        getDesc(),
                        SimpleOperatorFactory.of(operator),
                        InternalTypeInfo.of(getOutputType()),
                        rInput.getParallelism(),
                        managedMemory);

        if (inputsContainSingleton()) {
            ret.setParallelism(1);
            ret.setMaxParallelism(1);
        }
        return ret;
    }

    private SortCodeGenerator newSortGen(TableConfig config, int[] originalKeys, RowType type) {
        SortSpec sortSpec = SortUtil.getAscendingSortSpec(originalKeys);
        return new SortCodeGenerator(config, type, sortSpec);
    }
}
