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
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.LongHashJoinGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.fusion.generator.TwoInputOpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.fusion.spec.HashJoinFusionCodegenSpec;
import org.apache.flink.table.planner.plan.nodes.exec.AdaptiveJoinExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.HashJoinOperatorUtil;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.OperatorType;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.HashJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link BatchExecNode} for Hash Join. */
@ExecNodeMetadata(
        name = "batch-exec-join",
        version = 1,
        producedTransformations = BatchExecHashJoin.JOIN_TRANSFORMATION,
        consumedOptions = {
            "table.exec.resource.hash-join.memory",
            "table.exec.resource.external-buffer-memory",
            "table.exec.resource.sort.memory",
            "table.exec.spill-compression.enabled",
            "table.exec.spill-compression.block-size"
        },
        minPlanVersion = FlinkVersion.v2_0,
        minStateVersion = FlinkVersion.v2_0)
public class BatchExecHashJoin extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>,
                SingleTransformationTranslator<RowData>,
                AdaptiveJoinExecNode {

    public static final String JOIN_TRANSFORMATION = "join";
    public static final String FIELD_NAME_JOIN_SPEC = "joinSpec";
    public static final String FIELD_NAME_IS_BROADCAST = "isBroadcast";
    public static final String FIELD_NAME_LEFT_IS_BUILD = "leftIsBuild";
    public static final String FIELD_NAME_ESTIMATED_LEFT_AVG_ROW_SIZE = "estimatedLeftAvgRowSize";
    public static final String FIELD_NAME_ESTIMATED_RIGHT_AVG_ROW_SIZE = "estimatedRightAvgRowSize";
    public static final String FIELD_NAME_ESTIMATED_LEFT_ROW_COUNT = "estimatedLeftRowCount";
    public static final String FIELD_NAME_ESTIMATED_RIGHT_ROW_COUNT = "estimatedRightRowCount";
    public static final String FIELD_NAME_TRY_DISTINCT_BUILD_ROW = "tryDistinctBuildRow";
    public static final String FIELD_NAME_WITH_JOIN_STRATEGY_HINT = "withJobStrategyHint";

    @JsonProperty(FIELD_NAME_JOIN_SPEC)
    private final JoinSpec joinSpec;

    @JsonProperty(FIELD_NAME_IS_BROADCAST)
    private final boolean isBroadcast;

    @JsonProperty(FIELD_NAME_LEFT_IS_BUILD)
    private final boolean leftIsBuild;

    @JsonProperty(FIELD_NAME_ESTIMATED_LEFT_AVG_ROW_SIZE)
    private final int estimatedLeftAvgRowSize;

    @JsonProperty(FIELD_NAME_ESTIMATED_RIGHT_AVG_ROW_SIZE)
    private final int estimatedRightAvgRowSize;

    @JsonProperty(FIELD_NAME_ESTIMATED_LEFT_ROW_COUNT)
    private final long estimatedLeftRowCount;

    @JsonProperty(FIELD_NAME_ESTIMATED_RIGHT_ROW_COUNT)
    private final long estimatedRightRowCount;

    @JsonProperty(FIELD_NAME_WITH_JOIN_STRATEGY_HINT)
    private final boolean withJobStrategyHint;

    @JsonProperty(FIELD_NAME_TRY_DISTINCT_BUILD_ROW)
    private final boolean tryDistinctBuildRow;

    public BatchExecHashJoin(
            ReadableConfig tableConfig,
            JoinSpec joinSpec,
            int estimatedLeftAvgRowSize,
            int estimatedRightAvgRowSize,
            long estimatedLeftRowCount,
            long estimatedRightRowCount,
            boolean isBroadcast,
            boolean leftIsBuild,
            boolean tryDistinctBuildRow,
            InputProperty leftInputProperty,
            InputProperty rightInputProperty,
            RowType outputType,
            boolean withJobStrategyHint,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecHashJoin.class),
                ExecNodeContext.newPersistedConfig(BatchExecHashJoin.class, tableConfig),
                Arrays.asList(leftInputProperty, rightInputProperty),
                outputType,
                description);
        this.joinSpec = checkNotNull(joinSpec);
        this.isBroadcast = isBroadcast;
        this.leftIsBuild = leftIsBuild;
        this.estimatedLeftAvgRowSize = estimatedLeftAvgRowSize;
        this.estimatedRightAvgRowSize = estimatedRightAvgRowSize;
        this.estimatedLeftRowCount = estimatedLeftRowCount;
        this.estimatedRightRowCount = estimatedRightRowCount;
        this.tryDistinctBuildRow = tryDistinctBuildRow;
        this.withJobStrategyHint = withJobStrategyHint;
    }

    @JsonCreator
    public BatchExecHashJoin(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_JOIN_SPEC) JoinSpec joinSpec,
            @JsonProperty(FIELD_NAME_ESTIMATED_LEFT_AVG_ROW_SIZE) int estimatedLeftAvgRowSize,
            @JsonProperty(FIELD_NAME_ESTIMATED_RIGHT_AVG_ROW_SIZE) int estimatedRightAvgRowSize,
            @JsonProperty(FIELD_NAME_ESTIMATED_LEFT_ROW_COUNT) long estimatedLeftRowCount,
            @JsonProperty(FIELD_NAME_ESTIMATED_RIGHT_ROW_COUNT) long estimatedRightRowCount,
            @JsonProperty(FIELD_NAME_IS_BROADCAST) boolean isBroadcast,
            @JsonProperty(FIELD_NAME_LEFT_IS_BUILD) boolean leftIsBuild,
            @JsonProperty(FIELD_NAME_TRY_DISTINCT_BUILD_ROW) boolean tryDistinctBuildRow,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description,
            @JsonProperty(FIELD_NAME_WITH_JOIN_STRATEGY_HINT) boolean withJobStrategyHint) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 2);
        this.joinSpec = checkNotNull(joinSpec);
        this.isBroadcast = isBroadcast;
        this.leftIsBuild = leftIsBuild;
        this.estimatedLeftAvgRowSize = estimatedLeftAvgRowSize;
        this.estimatedRightAvgRowSize = estimatedRightAvgRowSize;
        this.estimatedLeftRowCount = estimatedLeftRowCount;
        this.estimatedRightRowCount = estimatedRightRowCount;
        this.tryDistinctBuildRow = tryDistinctBuildRow;
        this.withJobStrategyHint = withJobStrategyHint;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        ExecEdge leftInputEdge = getInputEdges().get(0);
        ExecEdge rightInputEdge = getInputEdges().get(1);

        Transformation<RowData> leftInputTransform =
                (Transformation<RowData>) leftInputEdge.translateToPlan(planner);
        Transformation<RowData> rightInputTransform =
                (Transformation<RowData>) rightInputEdge.translateToPlan(planner);
        // get input types
        RowType leftType = (RowType) leftInputEdge.getOutputType();
        RowType rightType = (RowType) rightInputEdge.getOutputType();
        long managedMemory = JoinUtil.getManagedMemory(joinSpec.getJoinType(), config);

        GeneratedJoinCondition condFunc =
                JoinUtil.generateConditionFunction(
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        joinSpec.getNonEquiCondition().orElse(null),
                        leftType,
                        rightType);

        StreamOperatorFactory<RowData> operator =
                HashJoinOperatorUtil.generateOperatorFactory(
                        joinSpec.getLeftKeys(),
                        joinSpec.getRightKeys(),
                        joinSpec.getJoinType(),
                        joinSpec.getFilterNulls(),
                        leftType,
                        rightType,
                        condFunc,
                        leftIsBuild,
                        estimatedLeftAvgRowSize,
                        estimatedRightAvgRowSize,
                        estimatedLeftRowCount,
                        estimatedRightRowCount,
                        tryDistinctBuildRow,
                        managedMemory,
                        config,
                        planner.getFlinkContext().getClassLoader());

        Transformation<RowData> buildTransform;
        Transformation<RowData> probeTransform;
        if (leftIsBuild) {
            buildTransform = leftInputTransform;
            probeTransform = rightInputTransform;
        } else {
            buildTransform = rightInputTransform;
            probeTransform = leftInputTransform;
        }

        return ExecNodeUtil.createTwoInputTransformation(
                buildTransform,
                probeTransform,
                createTransformationMeta(BatchExecHashJoin.JOIN_TRANSFORMATION, config),
                operator,
                InternalTypeInfo.of(getOutputType()),
                probeTransform.getParallelism(),
                managedMemory,
                false);
    }

    @Override
    public boolean supportFusionCodegen() {
        RowType leftType = (RowType) getInputEdges().get(0).getOutputType();
        LogicalType[] keyFieldTypes =
                IntStream.of(joinSpec.getLeftKeys())
                        .mapToObj(leftType::getTypeAt)
                        .toArray(LogicalType[]::new);
        RowType keyType = RowType.of(keyFieldTypes);
        FlinkJoinType joinType = joinSpec.getJoinType();
        HashJoinType hashJoinType =
                HashJoinType.of(
                        leftIsBuild,
                        joinType.isLeftOuter(),
                        joinType.isRightOuter(),
                        joinType == FlinkJoinType.SEMI,
                        joinType == FlinkJoinType.ANTI);
        // TODO decimal and multiKeys support and all HashJoinType support.
        return LongHashJoinGenerator.support(hashJoinType, keyType, joinSpec.getFilterNulls());
    }

    @Override
    protected OpFusionCodegenSpecGenerator translateToFusionCodegenSpecInternal(
            PlannerBase planner, ExecNodeConfig config, CodeGeneratorContext parentCtx) {
        OpFusionCodegenSpecGenerator leftInput =
                getInputEdges().get(0).translateToFusionCodegenSpec(planner, parentCtx);
        OpFusionCodegenSpecGenerator rightInput =
                getInputEdges().get(1).translateToFusionCodegenSpec(planner, parentCtx);
        boolean compressionEnabled =
                config.get(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED);
        int compressionBlockSize =
                (int)
                        config.get(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE)
                                .getBytes();
        long managedMemory = JoinUtil.getManagedMemory(joinSpec.getJoinType(), config);
        OpFusionCodegenSpecGenerator hashJoinGenerator =
                new TwoInputOpFusionCodegenSpecGenerator(
                        leftInput,
                        rightInput,
                        managedMemory,
                        (RowType) getOutputType(),
                        new HashJoinFusionCodegenSpec(
                                new CodeGeneratorContext(
                                        config,
                                        planner.getFlinkContext().getClassLoader(),
                                        parentCtx),
                                isBroadcast,
                                leftIsBuild,
                                joinSpec,
                                estimatedLeftAvgRowSize,
                                estimatedRightAvgRowSize,
                                estimatedLeftRowCount,
                                estimatedRightRowCount,
                                compressionEnabled,
                                compressionBlockSize));
        leftInput.addOutput(1, hashJoinGenerator);
        rightInput.addOutput(2, hashJoinGenerator);
        return hashJoinGenerator;
    }

    @Override
    public boolean canBeTransformedToAdaptiveJoin() {
        return !withJobStrategyHint && joinSpec.getJoinType() != FlinkJoinType.FULL;
    }

    @Override
    public BatchExecAdaptiveJoin toAdaptiveJoinNode() {
        return new BatchExecAdaptiveJoin(
                getPersistedConfig(),
                joinSpec,
                estimatedLeftAvgRowSize,
                estimatedRightAvgRowSize,
                estimatedLeftRowCount,
                estimatedRightRowCount,
                leftIsBuild,
                tryDistinctBuildRow,
                getInputProperties(),
                (RowType) getOutputType(),
                getDescription(),
                OperatorType.ShuffleHashJoin);
    }
}
