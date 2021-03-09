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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.TableConfig;
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
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.temporal.TemporalProcessTimeJoinOperator;
import org.apache.flink.table.runtime.operators.join.temporal.TemporalRowTimeJoinOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Optional;
import java.util.stream.IntStream;

/**
 * {@link StreamExecNode} for temporal table join (FOR SYSTEM_TIME AS OF) and temporal TableFunction
 * join (LATERAL TemporalTableFunction(o.proctime)).
 *
 * <p>The legacy temporal table function join is the subset of temporal table join, the only
 * difference is the validation, we reuse most same logic here.
 */
public class StreamExecTemporalJoin extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData> {

    public static final int FIELD_INDEX_FOR_PROC_TIME_ATTRIBUTE = -1;

    private final JoinSpec joinSpec;
    private final boolean isTemporalFunctionJoin;
    private final int leftTimeAttributeIndex;
    private final int rightTimeAttributeIndex;

    public StreamExecTemporalJoin(
            JoinSpec joinSpec,
            boolean isTemporalTableFunctionJoin,
            int leftTimeAttributeIndex,
            int rightTimeAttributeIndex,
            InputProperty leftInputProperty,
            InputProperty rightInputProperty,
            RowType outputType,
            String description) {
        super(Lists.newArrayList(leftInputProperty, rightInputProperty), outputType, description);
        Preconditions.checkArgument(
                rightTimeAttributeIndex == FIELD_INDEX_FOR_PROC_TIME_ATTRIBUTE
                        || rightTimeAttributeIndex >= 0);
        this.joinSpec = joinSpec;
        this.isTemporalFunctionJoin = isTemporalTableFunctionJoin;
        this.leftTimeAttributeIndex = leftTimeAttributeIndex;
        this.rightTimeAttributeIndex = rightTimeAttributeIndex;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
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
                getJoinOperator(planner.getTableConfig(), leftInputType, rightInputType);
        Transformation<RowData> leftTransform =
                (Transformation<RowData>) leftInputEdge.translateToPlan(planner);
        Transformation<RowData> rightTransform =
                (Transformation<RowData>) rightInputEdge.translateToPlan(planner);

        TwoInputTransformation<RowData, RowData, RowData> ret =
                new TwoInputTransformation<>(
                        leftTransform,
                        rightTransform,
                        getDescription(),
                        joinOperator,
                        InternalTypeInfo.of(returnType),
                        leftTransform.getParallelism());

        if (inputsContainSingleton()) {
            ret.setParallelism(1);
            ret.setMaxParallelism(1);
        }

        // set KeyType and Selector for state
        RowDataKeySelector leftKeySelector = getLeftKeySelector(leftInputType);
        RowDataKeySelector rightKeySelector = getRightKeySelector(rightInputType);
        ret.setStateKeySelectors(leftKeySelector, rightKeySelector);
        LogicalType[] keyTypes =
                IntStream.of(joinSpec.getLeftKeys())
                        .mapToObj(leftInputType::getTypeAt)
                        .toArray(LogicalType[]::new);
        ret.setStateKeyType(InternalTypeInfo.ofFields(keyTypes));
        return ret;
    }

    private RowDataKeySelector getLeftKeySelector(RowType leftInputType) {
        return KeySelectorUtil.getRowDataSelector(
                joinSpec.getLeftKeys(), InternalTypeInfo.of(leftInputType));
    }

    private RowDataKeySelector getRightKeySelector(RowType rightInputType) {
        return KeySelectorUtil.getRowDataSelector(
                joinSpec.getRightKeys(), InternalTypeInfo.of(rightInputType));
    }

    private TwoInputStreamOperator<RowData, RowData, RowData> getJoinOperator(
            TableConfig config, RowType leftInputType, RowType rightInputType) {

        // input must not be nullable, because the runtime join function will make sure
        // the code-generated function won't process null inputs
        final CodeGeneratorContext ctx = new CodeGeneratorContext(config);
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
            TableConfig tableConfig,
            RowType leftInputType,
            RowType rightInputType,
            GeneratedJoinCondition generatedJoinCondition) {

        boolean isLeftOuterJoin = joinSpec.getJoinType() == FlinkJoinType.LEFT;
        long minRetentionTime = tableConfig.getMinIdleStateRetentionTime();
        long maxRetentionTime = tableConfig.getMaxIdleStateRetentionTime();
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
