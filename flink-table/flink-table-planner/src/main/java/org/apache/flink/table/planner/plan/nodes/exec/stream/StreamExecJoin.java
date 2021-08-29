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
 *
 */

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.AbstractStreamingJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.StreamingSemiAntiJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link StreamExecNode} for regular Joins.
 *
 * <p>Regular joins are the most generic type of join in which any new records or changes to either
 * side of the join input are visible and are affecting the whole join result.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamExecJoin extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {
    public static final String FIELD_NAME_JOIN_SPEC = "joinSpec";
    public static final String FIELD_NAME_LEFT_UNIQUE_KEYS = "leftUniqueKeys";
    public static final String FIELD_NAME_RIGHT_UNIQUE_KEYS = "rightUniqueKeys";

    @JsonProperty(FIELD_NAME_JOIN_SPEC)
    private final JoinSpec joinSpec;

    @JsonProperty(FIELD_NAME_LEFT_UNIQUE_KEYS)
    private final List<int[]> leftUniqueKeys;

    @JsonProperty(FIELD_NAME_RIGHT_UNIQUE_KEYS)
    private final List<int[]> rightUniqueKeys;

    public StreamExecJoin(
            JoinSpec joinSpec,
            List<int[]> leftUniqueKeys,
            List<int[]> rightUniqueKeys,
            InputProperty leftInputProperty,
            InputProperty rightInputProperty,
            RowType outputType,
            String description) {
        this(
                joinSpec,
                leftUniqueKeys,
                rightUniqueKeys,
                getNewNodeId(),
                Lists.newArrayList(leftInputProperty, rightInputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecJoin(
            @JsonProperty(FIELD_NAME_JOIN_SPEC) JoinSpec joinSpec,
            @JsonProperty(FIELD_NAME_LEFT_UNIQUE_KEYS) List<int[]> leftUniqueKeys,
            @JsonProperty(FIELD_NAME_RIGHT_UNIQUE_KEYS) List<int[]> rightUniqueKeys,
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 2);
        this.joinSpec = checkNotNull(joinSpec);
        this.leftUniqueKeys = leftUniqueKeys;
        this.rightUniqueKeys = rightUniqueKeys;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecEdge leftInputEdge = getInputEdges().get(0);
        final ExecEdge rightInputEdge = getInputEdges().get(1);

        final Transformation<RowData> leftTransform =
                (Transformation<RowData>) leftInputEdge.translateToPlan(planner);
        final Transformation<RowData> rightTransform =
                (Transformation<RowData>) rightInputEdge.translateToPlan(planner);

        final RowType leftType = (RowType) leftInputEdge.getOutputType();
        final RowType rightType = (RowType) rightInputEdge.getOutputType();
        JoinUtil.validateJoinSpec(joinSpec, leftType, rightType, true);

        final int[] leftJoinKey = joinSpec.getLeftKeys();
        final int[] rightJoinKey = joinSpec.getRightKeys();

        final InternalTypeInfo<RowData> leftTypeInfo = InternalTypeInfo.of(leftType);
        final JoinInputSideSpec leftInputSpec =
                JoinUtil.analyzeJoinInput(leftTypeInfo, leftJoinKey, leftUniqueKeys);

        final InternalTypeInfo<RowData> rightTypeInfo = InternalTypeInfo.of(rightType);
        final JoinInputSideSpec rightInputSpec =
                JoinUtil.analyzeJoinInput(rightTypeInfo, rightJoinKey, rightUniqueKeys);

        final TableConfig tableConfig = planner.getTableConfig();
        GeneratedJoinCondition generatedCondition =
                JoinUtil.generateConditionFunction(tableConfig, joinSpec, leftType, rightType);

        long minRetentionTime = tableConfig.getMinIdleStateRetentionTime();

        AbstractStreamingJoinOperator operator;
        FlinkJoinType joinType = joinSpec.getJoinType();
        if (joinType == FlinkJoinType.ANTI || joinType == FlinkJoinType.SEMI) {
            operator =
                    new StreamingSemiAntiJoinOperator(
                            joinType == FlinkJoinType.ANTI,
                            leftTypeInfo,
                            rightTypeInfo,
                            generatedCondition,
                            leftInputSpec,
                            rightInputSpec,
                            joinSpec.getFilterNulls(),
                            minRetentionTime);
        } else {
            boolean leftIsOuter = joinType == FlinkJoinType.LEFT || joinType == FlinkJoinType.FULL;
            boolean rightIsOuter =
                    joinType == FlinkJoinType.RIGHT || joinType == FlinkJoinType.FULL;
            operator =
                    new StreamingJoinOperator(
                            leftTypeInfo,
                            rightTypeInfo,
                            generatedCondition,
                            leftInputSpec,
                            rightInputSpec,
                            leftIsOuter,
                            rightIsOuter,
                            joinSpec.getFilterNulls(),
                            minRetentionTime);
        }

        final RowType returnType = (RowType) getOutputType();
        final TwoInputTransformation<RowData, RowData, RowData> transform =
                new TwoInputTransformation<>(
                        leftTransform,
                        rightTransform,
                        getDescription(),
                        operator,
                        InternalTypeInfo.of(returnType),
                        leftTransform.getParallelism());

        // set KeyType and Selector for state
        RowDataKeySelector leftSelect =
                KeySelectorUtil.getRowDataSelector(leftJoinKey, leftTypeInfo);
        RowDataKeySelector rightSelect =
                KeySelectorUtil.getRowDataSelector(rightJoinKey, rightTypeInfo);
        transform.setStateKeySelectors(leftSelect, rightSelect);
        transform.setStateKeyType(leftSelect.getProducedType());
        return transform;
    }
}
