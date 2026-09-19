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

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.snapshot.LateralSnapshotJoinOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;

/**
 * {@link StreamExecNode} for the LATERAL SNAPSHOT processing-time temporal table join. The
 * underlying {@link LateralSnapshotJoinOperator} runs in two phases (LOAD then JOIN) gated by a
 * flip point on the build-side watermark.
 */
@ExecNodeMetadata(
        name = "stream-exec-lateral-snapshot-join",
        version = 1,
        producedTransformations =
                StreamExecLateralSnapshotJoin.LATERAL_SNAPSHOT_JOIN_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v2_4,
        minStateVersion = FlinkVersion.v2_4)
public class StreamExecLateralSnapshotJoin extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String LATERAL_SNAPSHOT_JOIN_TRANSFORMATION = "lateral-snapshot-join";

    public static final String FIELD_NAME_JOIN_SPEC = "joinSpec";
    public static final String FIELD_NAME_RIGHT_TIME_ATTRIBUTE_INDEX = "rightTimeAttributeIndex";
    public static final String FIELD_NAME_LOAD_COMPLETED_CONDITION = "loadCompletedCondition";
    public static final String FIELD_NAME_LOAD_COMPLETED_TIME = "loadCompletedTime";
    public static final String FIELD_NAME_LOAD_COMPLETED_IDLE_TIMEOUT_MS =
            "loadCompletedIdleTimeoutMs";
    public static final String FIELD_NAME_STATE_TTL_MS = "stateTtlMs";

    @JsonProperty(FIELD_NAME_JOIN_SPEC)
    private final JoinSpec joinSpec;

    /** Field index of the build-side (right) row-time attribute. */
    @JsonProperty(FIELD_NAME_RIGHT_TIME_ATTRIBUTE_INDEX)
    private final int rightTimeAttributeIndex;

    // Carried for explain output and plan serialization only; the operator uses loadCompletedTime,
    // which the planner already resolved from this condition.
    @JsonProperty(FIELD_NAME_LOAD_COMPLETED_CONDITION)
    private final String loadCompletedCondition;

    @JsonProperty(FIELD_NAME_LOAD_COMPLETED_TIME)
    private final Long loadCompletedTime;

    @JsonProperty(FIELD_NAME_LOAD_COMPLETED_IDLE_TIMEOUT_MS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final Long loadCompletedIdleTimeoutMs;

    @JsonProperty(FIELD_NAME_STATE_TTL_MS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final Long stateTtlMs;

    public StreamExecLateralSnapshotJoin(
            ReadableConfig tableConfig,
            JoinSpec joinSpec,
            int rightTimeAttributeIndex,
            String loadCompletedCondition,
            Long loadCompletedTime,
            @Nullable Long loadCompletedIdleTimeoutMs,
            @Nullable Long stateTtlMs,
            InputProperty leftInputProperty,
            InputProperty rightInputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecLateralSnapshotJoin.class),
                ExecNodeContext.newPersistedConfig(
                        StreamExecLateralSnapshotJoin.class, tableConfig),
                joinSpec,
                rightTimeAttributeIndex,
                loadCompletedCondition,
                loadCompletedTime,
                loadCompletedIdleTimeoutMs,
                stateTtlMs,
                List.of(leftInputProperty, rightInputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecLateralSnapshotJoin(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_JOIN_SPEC) JoinSpec joinSpec,
            @JsonProperty(FIELD_NAME_RIGHT_TIME_ATTRIBUTE_INDEX) int rightTimeAttributeIndex,
            @JsonProperty(FIELD_NAME_LOAD_COMPLETED_CONDITION) String loadCompletedCondition,
            @JsonProperty(FIELD_NAME_LOAD_COMPLETED_TIME) Long loadCompletedTime,
            @JsonProperty(FIELD_NAME_LOAD_COMPLETED_IDLE_TIMEOUT_MS) @Nullable
                    Long loadCompletedIdleTimeoutMs,
            @JsonProperty(FIELD_NAME_STATE_TTL_MS) @Nullable Long stateTtlMs,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        Preconditions.checkArgument(inputProperties.size() == 2);
        this.joinSpec = Preconditions.checkNotNull(joinSpec);
        Preconditions.checkArgument(
                rightTimeAttributeIndex >= 0,
                "rightTimeAttributeIndex must be non-negative, but was %s",
                rightTimeAttributeIndex);
        this.rightTimeAttributeIndex = rightTimeAttributeIndex;
        this.loadCompletedCondition = Preconditions.checkNotNull(loadCompletedCondition);
        this.loadCompletedTime = Preconditions.checkNotNull(loadCompletedTime);
        // the idle timeout and state TTL are optional and non-negative when set
        Preconditions.checkArgument(
                loadCompletedIdleTimeoutMs == null || loadCompletedIdleTimeoutMs >= 0,
                "loadCompletedIdleTimeoutMs must be non-negative, but was %s",
                loadCompletedIdleTimeoutMs);
        Preconditions.checkArgument(
                stateTtlMs == null || stateTtlMs >= 0,
                "stateTtlMs must be non-negative, but was %s",
                stateTtlMs);
        this.loadCompletedIdleTimeoutMs = loadCompletedIdleTimeoutMs;
        this.stateTtlMs = stateTtlMs;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge leftInputEdge = getInputEdges().get(0);
        final ExecEdge rightInputEdge = getInputEdges().get(1);
        final RowType leftInputType = (RowType) leftInputEdge.getOutputType();
        final RowType rightInputType = (RowType) rightInputEdge.getOutputType();

        JoinUtil.validateJoinSpec(joinSpec, leftInputType, rightInputType, true);

        // Defensive: the SQL grammar and the rewrite rule already restrict LATERAL joins to
        // INNER/LEFT, so this branch is not reachable from SQL.
        final FlinkJoinType joinType = joinSpec.getJoinType();
        if (joinType != FlinkJoinType.INNER && joinType != FlinkJoinType.LEFT) {
            throw new ValidationException(
                    "LATERAL SNAPSHOT join only supports INNER JOIN and LEFT OUTER JOIN, but was "
                            + joinType
                            + " JOIN.");
        }

        final boolean isLeftOuterJoin = joinType == FlinkJoinType.LEFT;
        final RowType returnType = (RowType) getOutputType();

        final GeneratedJoinCondition generatedJoinCondition =
                JoinUtil.generateConditionFunction(
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        joinSpec,
                        leftInputType,
                        rightInputType);

        // Fall back to the pipeline's state TTL when the SNAPSHOT call does not set state_ttl.
        final long effectiveStateTtlMs =
                stateTtlMs != null ? stateTtlMs : config.getStateRetentionTime();

        final LateralSnapshotJoinOperator operator =
                new LateralSnapshotJoinOperator(
                        isLeftOuterJoin,
                        InternalTypeInfo.of(leftInputType),
                        InternalTypeInfo.of(rightInputType),
                        rightTimeAttributeIndex,
                        generatedJoinCondition,
                        joinSpec.getFilterNulls(),
                        loadCompletedTime,
                        loadCompletedIdleTimeoutMs,
                        effectiveStateTtlMs);

        final Transformation<RowData> leftTransform =
                (Transformation<RowData>) leftInputEdge.translateToPlan(planner);
        final Transformation<RowData> rightTransform =
                (Transformation<RowData>) rightInputEdge.translateToPlan(planner);

        final TwoInputTransformation<RowData, RowData, RowData> transform =
                ExecNodeUtil.createTwoInputTransformation(
                        leftTransform,
                        rightTransform,
                        createTransformationMeta(LATERAL_SNAPSHOT_JOIN_TRANSFORMATION, config),
                        operator,
                        InternalTypeInfo.of(returnType),
                        leftTransform.getParallelism(),
                        false);

        final ClassLoader classLoader = planner.getFlinkContext().getClassLoader();
        final RowDataKeySelector leftKeySelector =
                KeySelectorUtil.getRowDataSelector(
                        classLoader, joinSpec.getLeftKeys(), InternalTypeInfo.of(leftInputType));
        final RowDataKeySelector rightKeySelector =
                KeySelectorUtil.getRowDataSelector(
                        classLoader, joinSpec.getRightKeys(), InternalTypeInfo.of(rightInputType));
        transform.setStateKeySelectors(leftKeySelector, rightKeySelector);
        transform.setStateKeyType(leftKeySelector.getProducedType());
        return transform;
    }
}
