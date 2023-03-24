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
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ExprCodeGenerator;
import org.apache.flink.table.planner.codegen.FunctionCodeGenerator;
import org.apache.flink.table.planner.codegen.GeneratedExpression;
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
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.temporal.TemporalProcessTimeJoinOperator;
import org.apache.flink.table.runtime.operators.join.temporal.TemporalRowTimeJoinOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * {@link StreamExecNode} for temporal table join (FOR SYSTEM_TIME AS OF) and temporal TableFunction
 * join (LATERAL TemporalTableFunction(o.proctime)).
 *
 * <p>The legacy temporal table function join is the subset of temporal table join, the only
 * difference is the validation, we reuse most same logic here.
 */
@ExecNodeMetadata(
        name = "stream-exec-temporal-join",
        version = 1,
        producedTransformations = StreamExecTemporalJoin.TEMPORAL_JOIN_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecTemporalJoin extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String TEMPORAL_JOIN_TRANSFORMATION = "temporal-join";

    public static final String FIELD_NAME_JOIN_SPEC = "joinSpec";
    public static final String FIELD_NAME_IS_TEMPORAL_FUNCTION_JOIN = "isTemporalFunctionJoin";
    public static final String FIELD_NAME_LEFT_TIME_ATTRIBUTE_INDEX = "leftTimeAttributeIndex";
    public static final String FIELD_NAME_RIGHT_TIME_ATTRIBUTE_INDEX = "rightTimeAttributeIndex";
    public static final int FIELD_INDEX_FOR_PROC_TIME_ATTRIBUTE = -1;

    @JsonProperty(FIELD_NAME_JOIN_SPEC)
    private final JoinSpec joinSpec;

    @JsonProperty(FIELD_NAME_IS_TEMPORAL_FUNCTION_JOIN)
    private final boolean isTemporalFunctionJoin;

    @JsonProperty(FIELD_NAME_LEFT_TIME_ATTRIBUTE_INDEX)
    private final int leftTimeAttributeIndex;

    @JsonProperty(FIELD_NAME_RIGHT_TIME_ATTRIBUTE_INDEX)
    private final int rightTimeAttributeIndex;

    public StreamExecTemporalJoin(
            ReadableConfig tableConfig,
            JoinSpec joinSpec,
            boolean isTemporalTableFunctionJoin,
            int leftTimeAttributeIndex,
            int rightTimeAttributeIndex,
            InputProperty leftInputProperty,
            InputProperty rightInputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecTemporalJoin.class),
                ExecNodeContext.newPersistedConfig(StreamExecTemporalJoin.class, tableConfig),
                joinSpec,
                isTemporalTableFunctionJoin,
                leftTimeAttributeIndex,
                rightTimeAttributeIndex,
                Arrays.asList(leftInputProperty, rightInputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecTemporalJoin(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_JOIN_SPEC) JoinSpec joinSpec,
            @JsonProperty(FIELD_NAME_IS_TEMPORAL_FUNCTION_JOIN) boolean isTemporalTableFunctionJoin,
            @JsonProperty(FIELD_NAME_LEFT_TIME_ATTRIBUTE_INDEX) int leftTimeAttributeIndex,
            @JsonProperty(FIELD_NAME_RIGHT_TIME_ATTRIBUTE_INDEX) int rightTimeAttributeIndex,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        Preconditions.checkArgument(inputProperties.size() == 2);
        Preconditions.checkArgument(
                rightTimeAttributeIndex == FIELD_INDEX_FOR_PROC_TIME_ATTRIBUTE
                        || rightTimeAttributeIndex >= 0);
        this.joinSpec = Preconditions.checkNotNull(joinSpec);
        this.isTemporalFunctionJoin = isTemporalTableFunctionJoin;
        this.leftTimeAttributeIndex = leftTimeAttributeIndex;
        this.rightTimeAttributeIndex = rightTimeAttributeIndex;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        ExecEdge leftInputEdge = getInputEdges().get(0);
        ExecEdge rightInputEdge = getInputEdges().get(1);
        RowType leftInputType = (RowType) leftInputEdge.getOutputType();
        RowType rightInputType = (RowType) rightInputEdge.getOutputType();

        JoinUtil.validateJoinSpec(joinSpec, leftInputType, rightInputType, true);

        FlinkJoinType joinType = joinSpec.getJoinType();
        if (isTemporalFunctionJoin) {
            if (joinType != FlinkJoinType.INNER) {
                throw new ValidationException(
                        "Temporal table function join currently only support INNER JOIN, "
                                + "but was "
                                + joinType
                                + " JOIN.");
            }
        } else {
            if (joinType != FlinkJoinType.LEFT && joinType != FlinkJoinType.INNER) {
                throw new TableException(
                        "Temporal table join currently only support INNER JOIN and LEFT JOIN, "
                                + "but was "
                                + joinType
                                + " JOIN.");
            }
        }

        RowType returnType = (RowType) getOutputType();

        TwoInputStreamOperator<RowData, RowData, RowData> joinOperator =
                getJoinOperator(
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        leftInputType,
                        rightInputType);
        Transformation<RowData> leftTransform =
                (Transformation<RowData>) leftInputEdge.translateToPlan(planner);
        Transformation<RowData> rightTransform =
                (Transformation<RowData>) rightInputEdge.translateToPlan(planner);

        TwoInputTransformation<RowData, RowData, RowData> ret =
                ExecNodeUtil.createTwoInputTransformation(
                        leftTransform,
                        rightTransform,
                        createTransformationMeta(TEMPORAL_JOIN_TRANSFORMATION, config),
                        joinOperator,
                        InternalTypeInfo.of(returnType),
                        leftTransform.getParallelism(),
                        false);

        // set KeyType and Selector for state
        RowDataKeySelector leftKeySelector =
                getLeftKeySelector(planner.getFlinkContext().getClassLoader(), leftInputType);
        RowDataKeySelector rightKeySelector =
                getRightKeySelector(planner.getFlinkContext().getClassLoader(), rightInputType);
        ret.setStateKeySelectors(leftKeySelector, rightKeySelector);
        ret.setStateKeyType(leftKeySelector.getProducedType());
        return ret;
    }

    private RowDataKeySelector getLeftKeySelector(ClassLoader classLoader, RowType leftInputType) {
        return KeySelectorUtil.getRowDataSelector(
                classLoader, joinSpec.getLeftKeys(), InternalTypeInfo.of(leftInputType));
    }

    private RowDataKeySelector getRightKeySelector(
            ClassLoader classLoader, RowType rightInputType) {
        return KeySelectorUtil.getRowDataSelector(
                classLoader, joinSpec.getRightKeys(), InternalTypeInfo.of(rightInputType));
    }

    private TwoInputStreamOperator<RowData, RowData, RowData> getJoinOperator(
            ExecNodeConfig config,
            ClassLoader classLoader,
            RowType leftInputType,
            RowType rightInputType) {

        // input must not be nullable, because the runtime join function will make sure
        // the code-generated function won't process null inputs
        final CodeGeneratorContext ctx = new CodeGeneratorContext(config, classLoader);
        final ExprCodeGenerator exprGenerator =
                new ExprCodeGenerator(ctx, false)
                        .bindInput(
                                leftInputType,
                                CodeGenUtils.DEFAULT_INPUT1_TERM(),
                                JavaScalaConversionUtil.toScala(Optional.empty()))
                        .bindSecondInput(
                                rightInputType,
                                CodeGenUtils.DEFAULT_INPUT2_TERM(),
                                JavaScalaConversionUtil.toScala(Optional.empty()));

        String body = "return true;";
        if (joinSpec.getNonEquiCondition().isPresent()) {
            final GeneratedExpression condition =
                    exprGenerator.generateExpression(joinSpec.getNonEquiCondition().get());
            body = String.format("%s\nreturn %s;", condition.code(), condition.resultTerm());
        }

        GeneratedJoinCondition generatedJoinCondition =
                FunctionCodeGenerator.generateJoinCondition(
                        ctx,
                        "ConditionFunction",
                        body,
                        CodeGenUtils.DEFAULT_INPUT1_TERM(),
                        CodeGenUtils.DEFAULT_INPUT2_TERM());

        return createJoinOperator(config, leftInputType, rightInputType, generatedJoinCondition);
    }

    private TwoInputStreamOperator<RowData, RowData, RowData> createJoinOperator(
            ExecNodeConfig config,
            RowType leftInputType,
            RowType rightInputType,
            GeneratedJoinCondition generatedJoinCondition) {

        boolean isLeftOuterJoin = joinSpec.getJoinType() == FlinkJoinType.LEFT;
        long minRetentionTime = config.getStateRetentionTime();
        long maxRetentionTime = TableConfigUtils.getMaxIdleStateRetentionTime(config);
        if (rightTimeAttributeIndex >= 0) {
            return new TemporalRowTimeJoinOperator(
                    InternalTypeInfo.of(leftInputType),
                    InternalTypeInfo.of(rightInputType),
                    generatedJoinCondition,
                    leftTimeAttributeIndex,
                    rightTimeAttributeIndex,
                    minRetentionTime,
                    maxRetentionTime,
                    isLeftOuterJoin);
        } else {
            if (isTemporalFunctionJoin) {
                return new TemporalProcessTimeJoinOperator(
                        InternalTypeInfo.of(rightInputType),
                        generatedJoinCondition,
                        minRetentionTime,
                        maxRetentionTime,
                        isLeftOuterJoin);
            } else {
                // The exsiting TemporalProcessTimeJoinOperator has already supported temporal table
                // join.
                // However, the semantic of this implementation is problematic, because the join
                // processing
                // for left stream doesn't wait for the complete snapshot of temporal table, this
                // may
                // mislead users in production environment. See FLINK-19830 for more details.
                throw new TableException("Processing-time temporal join is not supported yet.");
            }
        }
    }
}
