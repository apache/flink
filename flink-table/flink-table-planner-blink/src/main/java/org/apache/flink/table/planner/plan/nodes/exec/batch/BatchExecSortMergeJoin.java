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
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.SortUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.SortMergeJoinOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link BatchExecNode} for Sort Merge Join. */
public class BatchExecSortMergeJoin extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private final FlinkJoinType joinType;
    private final int[] leftKeys;
    private final int[] rightKeys;
    private final boolean[] filterNulls;
    private final @Nullable RexNode nonEquiCondition;
    private final boolean leftIsSmaller;

    public BatchExecSortMergeJoin(
            FlinkJoinType joinType,
            int[] leftKeys,
            int[] rightKeys,
            boolean[] filterNulls,
            @Nullable RexNode nonEquiCondition,
            boolean leftIsSmaller,
            InputProperty leftInputProperty,
            InputProperty rightInputProperty,
            RowType outputType,
            String description) {
        super(Arrays.asList(leftInputProperty, rightInputProperty), outputType, description);
        this.joinType = checkNotNull(joinType);
        this.leftKeys = checkNotNull(leftKeys);
        this.rightKeys = checkNotNull(rightKeys);
        this.filterNulls = checkNotNull(filterNulls);
        checkArgument(leftKeys.length > 0 && leftKeys.length == rightKeys.length);
        checkArgument(leftKeys.length == filterNulls.length);

        this.nonEquiCondition = nonEquiCondition;
        this.leftIsSmaller = leftIsSmaller;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        ExecEdge leftInputEdge = getInputEdges().get(0);
        ExecEdge rightInputEdge = getInputEdges().get(1);

        // get input types
        RowType leftType = (RowType) leftInputEdge.getOutputType();
        RowType rightType = (RowType) rightInputEdge.getOutputType();

        LogicalType[] keyFieldTypes =
                IntStream.of(leftKeys).mapToObj(leftType::getTypeAt).toArray(LogicalType[]::new);
        RowType keyType = RowType.of(keyFieldTypes);

        TableConfig config = planner.getTableConfig();
        GeneratedJoinCondition condFunc =
                JoinUtil.generateConditionFunction(config, nonEquiCondition, leftType, rightType);

        long externalBufferMemory =
                config.getConfiguration()
                        .get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_EXTERNAL_BUFFER_MEMORY)
                        .getBytes();
        long sortMemory =
                config.getConfiguration()
                        .get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_SORT_MEMORY)
                        .getBytes();
        int externalBufferNum = 1;
        if (joinType == FlinkJoinType.FULL) {
            externalBufferNum = 2;
        }

        long managedMemory = externalBufferMemory * externalBufferNum + sortMemory * 2;

        SortCodeGenerator leftSortGen = newSortGen(config, leftKeys, leftType);
        SortCodeGenerator rightSortGen = newSortGen(config, rightKeys, rightType);

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
                                leftType,
                                keyType,
                                leftKeys),
                        ProjectionCodeGenerator.generateProjection(
                                new CodeGeneratorContext(config),
                                "SMJProjection",
                                rightType,
                                keyType,
                                rightKeys),
                        leftSortGen.generateNormalizedKeyComputer("LeftComputer"),
                        leftSortGen.generateRecordComparator("LeftComparator"),
                        rightSortGen.generateNormalizedKeyComputer("RightComputer"),
                        rightSortGen.generateRecordComparator("RightComparator"),
                        newSortGen(config, keyPositions, keyType)
                                .generateRecordComparator("KeyComparator"),
                        filterNulls);

        Transformation<RowData> leftInputTransform =
                (Transformation<RowData>) leftInputEdge.translateToPlan(planner);
        Transformation<RowData> rightInputTransform =
                (Transformation<RowData>) rightInputEdge.translateToPlan(planner);
        return ExecNodeUtil.createTwoInputTransformation(
                leftInputTransform,
                rightInputTransform,
                getDescription(),
                SimpleOperatorFactory.of(operator),
                InternalTypeInfo.of(getOutputType()),
                rightInputTransform.getParallelism(),
                managedMemory);
    }

    private SortCodeGenerator newSortGen(
            TableConfig config, int[] originalKeys, RowType inputType) {
        SortSpec sortSpec = SortUtil.getAscendingSortSpec(originalKeys);
        return new SortCodeGenerator(config, inputType, sortSpec);
    }
}
