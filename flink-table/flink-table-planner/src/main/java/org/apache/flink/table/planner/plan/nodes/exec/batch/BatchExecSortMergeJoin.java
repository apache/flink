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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.SorMergeJoinOperatorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.SortMergeJoinFunction;
import org.apache.flink.table.runtime.operators.join.SortMergeJoinOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNestedLoopJoin.FIELD_NAME_JOIN_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec.FIELD_NAME_FILTER_NULLS;
import static org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec.FIELD_NAME_LEFT_KEYS;
import static org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec.FIELD_NAME_NON_EQUI_CONDITION;
import static org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec.FIELD_NAME_RIGHT_KEYS;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link BatchExecNode} for Sort Merge Join. */
@ExecNodeMetadata(
        name = "batch-exec-sort-merge-join",
        version = 1,
        minPlanVersion = FlinkVersion.v1_20,
        minStateVersion = FlinkVersion.v1_20)
public class BatchExecSortMergeJoin extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private static final String FIELD_NAME_LEFT_IS_SMALLER = "leftIsSmaller";
    private static final String FIELD_NAME_LEFT_INPUT_PROPERTY = "leftInputProperty";
    private static final String FIELD_NAME_RIGHT_INPUT_PROPERTY = "rightInputProperty";

    @JsonProperty(FIELD_NAME_JOIN_TYPE)
    private final FlinkJoinType joinType;

    @JsonProperty(FIELD_NAME_LEFT_KEYS)
    private final int[] leftKeys;

    @JsonProperty(FIELD_NAME_RIGHT_KEYS)
    private final int[] rightKeys;

    @JsonProperty(FIELD_NAME_FILTER_NULLS)
    private final boolean[] filterNulls;

    @JsonProperty(FIELD_NAME_NON_EQUI_CONDITION)
    private final @Nullable RexNode nonEquiCondition;

    @JsonProperty(FIELD_NAME_LEFT_IS_SMALLER)
    private final boolean leftIsSmaller;

    public static final String FIELD_NAME_MATCH_SPEC = "matchSpec";

    public BatchExecSortMergeJoin(
            ReadableConfig tableConfig,
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
        this.leftIsSmaller = leftIsSmaller;
    }

    @JsonCreator
    public BatchExecSortMergeJoin(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_JOIN_TYPE) FlinkJoinType joinType,
            @JsonProperty(FIELD_NAME_LEFT_KEYS) int[] leftKeys,
            @JsonProperty(FIELD_NAME_RIGHT_KEYS) int[] rightKeys,
            @JsonProperty(FIELD_NAME_FILTER_NULLS) boolean[] filterNulls,
            @JsonProperty(FIELD_NAME_NON_EQUI_CONDITION) RexNode nonEquiCondition,
            @JsonProperty(FIELD_NAME_LEFT_IS_SMALLER) boolean leftIsSmaller,
            @JsonProperty(FIELD_NAME_LEFT_INPUT_PROPERTY) InputProperty leftInputProperty,
            @JsonProperty(FIELD_NAME_RIGHT_INPUT_PROPERTY) InputProperty rightInputProperty,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(
                id,
                context,
                persistedConfig,
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
        this.leftIsSmaller = leftIsSmaller;
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

        LogicalType[] keyFieldTypes =
                IntStream.of(leftKeys).mapToObj(leftType::getTypeAt).toArray(LogicalType[]::new);
        RowType keyType = RowType.of(keyFieldTypes);

        GeneratedJoinCondition condFunc =
                JoinUtil.generateConditionFunction(
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        nonEquiCondition,
                        leftType,
                        rightType);

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

        SortMergeJoinFunction sortMergeJoinFunction =
                SorMergeJoinOperatorUtil.getSortMergeJoinFunction(
                        planner.getFlinkContext().getClassLoader(),
                        config,
                        joinType,
                        leftType,
                        rightType,
                        leftKeys,
                        rightKeys,
                        keyType,
                        leftIsSmaller,
                        filterNulls,
                        condFunc,
                        1.0 * externalBufferMemory / managedMemory);

        Transformation<RowData> leftInputTransform =
                (Transformation<RowData>) leftInputEdge.translateToPlan(planner);
        Transformation<RowData> rightInputTransform =
                (Transformation<RowData>) rightInputEdge.translateToPlan(planner);
        return ExecNodeUtil.createTwoInputTransformation(
                leftInputTransform,
                rightInputTransform,
                createTransformationName(config),
                createTransformationDescription(config),
                SimpleOperatorFactory.of(new SortMergeJoinOperator(sortMergeJoinFunction)),
                InternalTypeInfo.of(getOutputType()),
                rightInputTransform.getParallelism(),
                managedMemory,
                false);
    }
}
