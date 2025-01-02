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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.functions.TemporalTableFunctionImpl;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.utils.TableSqlFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalJoin;
import org.apache.flink.table.planner.plan.optimize.program.FlinkOptimizeContext;
import org.apache.flink.table.planner.plan.utils.ExpandTableScanShuttle;
import org.apache.flink.table.planner.plan.utils.RexDefaultVisitor;
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;
import org.immutables.value.Value;

import java.util.Optional;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The initial temporal TableFunction join (LATERAL TemporalTableFunction(o.proctime)) is a
 * correlate. Rewrite it into a Join with a special temporal join condition wraps time attribute and
 * primary key information. The join will be translated into {@link StreamExecTemporalJoin} in
 * physical.
 */
@Value.Enclosing
public class LogicalCorrelateToJoinFromTemporalTableFunctionRule
        extends RelRule<
                LogicalCorrelateToJoinFromTemporalTableFunctionRule
                        .LogicalCorrelateToJoinFromTemporalTableFunctionRuleConfig> {

    public static final LogicalCorrelateToJoinFromTemporalTableFunctionRule INSTANCE =
            LogicalCorrelateToJoinFromTemporalTableFunctionRule
                    .LogicalCorrelateToJoinFromTemporalTableFunctionRuleConfig.DEFAULT
                    .toRule();

    private LogicalCorrelateToJoinFromTemporalTableFunctionRule(
            LogicalCorrelateToJoinFromTemporalTableFunctionRuleConfig config) {
        super(config);
    }

    private String extractNameFromTimeAttribute(Expression timeAttribute) {
        if (timeAttribute instanceof FieldReferenceExpression) {
            FieldReferenceExpression f = (FieldReferenceExpression) timeAttribute;
            if (f.getOutputDataType()
                    .getLogicalType()
                    .isAnyOf(
                            LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                            LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
                return f.getName();
            }
        }
        throw new ValidationException(
                "Invalid timeAttribute [" + timeAttribute + "] in TemporalTableFunction");
    }

    private boolean isProctimeReference(TemporalTableFunctionImpl temporalTableFunction) {
        FieldReferenceExpression fieldRef =
                (FieldReferenceExpression) temporalTableFunction.getTimeAttribute();
        return isProctimeAttribute(fieldRef.getOutputDataType().getLogicalType());
    }

    private String extractNameFromPrimaryKeyAttribute(Expression expression) {
        if (expression instanceof FieldReferenceExpression) {
            FieldReferenceExpression f = (FieldReferenceExpression) expression;
            return f.getName();
        }
        throw new ValidationException(
                "Unsupported expression ["
                        + expression
                        + "] as primary key. "
                        + "Only top-level (not nested) field references are supported.");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalCorrelate logicalCorrelate = call.rel(0);
        RelNode leftNode = call.rel(1);
        TableFunctionScan rightTableFunctionScan = call.rel(2);

        RelOptCluster cluster = logicalCorrelate.getCluster();

        Optional<TemporalTableFunctionCall> temporalTableFunctionCall =
                new GetTemporalTableFunctionCall(cluster.getRexBuilder(), leftNode)
                        .visit(rightTableFunctionScan.getCall());

        if (temporalTableFunctionCall.isPresent()
                && temporalTableFunctionCall.get().getTemporalTableFunction()
                        instanceof TemporalTableFunctionImpl) {
            TemporalTableFunctionImpl rightTemporalTableFunction =
                    (TemporalTableFunctionImpl)
                            temporalTableFunctionCall.get().getTemporalTableFunction();
            RexNode leftTimeAttribute = temporalTableFunctionCall.get().getTimeAttribute();

            // If TemporalTableFunction was found, rewrite LogicalCorrelate to TemporalJoin
            QueryOperation underlyingHistoryTable =
                    rightTemporalTableFunction.getUnderlyingHistoryTable();
            RexBuilder rexBuilder = cluster.getRexBuilder();

            FlinkOptimizeContext flinkContext =
                    (FlinkOptimizeContext) ShortcutUtils.unwrapContext(call.getPlanner());
            FlinkRelBuilder relBuilder = flinkContext.getFlinkRelBuilder();

            RelNode temporalTable = relBuilder.queryOperation(underlyingHistoryTable).build();
            // expand QueryOperationCatalogViewTable in Table Scan
            ExpandTableScanShuttle shuttle = new ExpandTableScanShuttle();
            RelNode rightNode = temporalTable.accept(shuttle);

            RexNode rightTimeIndicatorExpression =
                    createRightExpression(
                            rexBuilder,
                            leftNode,
                            rightNode,
                            extractNameFromTimeAttribute(
                                    rightTemporalTableFunction.getTimeAttribute()));

            RexNode rightPrimaryKeyExpression =
                    createRightExpression(
                            rexBuilder,
                            leftNode,
                            rightNode,
                            extractNameFromPrimaryKeyAttribute(
                                    rightTemporalTableFunction.getPrimaryKey()));

            relBuilder.push(leftNode);
            relBuilder.push(rightNode);

            RexNode condition;
            if (isProctimeReference(rightTemporalTableFunction)) {
                condition =
                        TemporalJoinUtil.makeProcTimeTemporalFunctionJoinConCall(
                                rexBuilder, leftTimeAttribute, rightPrimaryKeyExpression);
            } else {
                condition =
                        TemporalJoinUtil.makeRowTimeTemporalFunctionJoinConCall(
                                rexBuilder,
                                leftTimeAttribute,
                                rightTimeIndicatorExpression,
                                rightPrimaryKeyExpression);
            }

            relBuilder.join(JoinRelType.INNER, condition);
            call.transformTo(relBuilder.build());
        } else {
            // Do nothing and handle standard TableFunction
        }
    }

    private RexNode createRightExpression(
            RexBuilder rexBuilder, RelNode leftNode, RelNode rightNode, String field) {
        int rightReferencesOffset = leftNode.getRowType().getFieldCount();
        RelDataTypeField rightDataTypeField = rightNode.getRowType().getField(field, false, false);
        return rexBuilder.makeInputRef(
                rightDataTypeField.getType(),
                rightReferencesOffset + rightDataTypeField.getIndex());
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface LogicalCorrelateToJoinFromTemporalTableFunctionRuleConfig
            extends RelRule.Config {
        LogicalCorrelateToJoinFromTemporalTableFunctionRule
                        .LogicalCorrelateToJoinFromTemporalTableFunctionRuleConfig
                DEFAULT =
                        ImmutableLogicalCorrelateToJoinFromTemporalTableFunctionRule
                                .LogicalCorrelateToJoinFromTemporalTableFunctionRuleConfig.builder()
                                .build()
                                .withOperandSupplier(
                                        b0 ->
                                                b0.operand(LogicalCorrelate.class)
                                                        .inputs(
                                                                b1 ->
                                                                        b1.operand(RelNode.class)
                                                                                .anyInputs(),
                                                                b2 ->
                                                                        b2.operand(
                                                                                        TableFunctionScan
                                                                                                .class)
                                                                                .noInputs()))
                                .withDescription(
                                        "LogicalCorrelateToJoinFromTemporalTableFunctionRule");

        @Override
        default LogicalCorrelateToJoinFromTemporalTableFunctionRule toRule() {
            return new LogicalCorrelateToJoinFromTemporalTableFunctionRule(this);
        }
    }
}

/**
 * Simple pojo class for extracted {@link TemporalTableFunction} with time attribute extracted from
 * RexNode with {@link TemporalTableFunction} call.
 */
class TemporalTableFunctionCall {
    private TemporalTableFunction temporalTableFunction;
    private RexNode timeAttribute;

    public TemporalTableFunctionCall(
            TemporalTableFunction temporalTableFunction, RexNode timeAttribute) {
        this.temporalTableFunction = temporalTableFunction;
        this.timeAttribute = timeAttribute;
    }

    public TemporalTableFunction getTemporalTableFunction() {
        return temporalTableFunction;
    }

    public void setTemporalTableFunction(TemporalTableFunction temporalTableFunction) {
        this.temporalTableFunction = temporalTableFunction;
    }

    public RexNode getTimeAttribute() {
        return timeAttribute;
    }

    public void setTimeAttribute(RexNode timeAttribute) {
        this.timeAttribute = timeAttribute;
    }
}

/**
 * Find {@link TemporalTableFunction} call and run {@link CorrelatedFieldAccessRemoval} on it's
 * operand.
 */
class GetTemporalTableFunctionCall extends RexVisitorImpl<TemporalTableFunctionCall> {
    private final RexBuilder rexBuilder;
    private final RelNode leftSide;

    GetTemporalTableFunctionCall(RexBuilder rexBuilder, RelNode leftSide) {
        super(false);
        this.rexBuilder = rexBuilder;
        this.leftSide = leftSide;
    }

    Optional<TemporalTableFunctionCall> visit(RexNode node) {
        TemporalTableFunctionCall result = node.accept(this);
        return result != null ? Optional.of(result) : Optional.empty();
    }

    @Override
    public TemporalTableFunctionCall visitCall(RexCall rexCall) {
        FunctionDefinition functionDefinition;
        SqlOperator sqlOperator = rexCall.getOperator();
        if (sqlOperator instanceof TableSqlFunction) {
            functionDefinition = ((TableSqlFunction) sqlOperator).udtf();
        } else if (sqlOperator instanceof BridgingSqlFunction) {
            functionDefinition = ((BridgingSqlFunction) sqlOperator).getDefinition();
        } else {
            return null;
        }

        if (!(functionDefinition instanceof TemporalTableFunctionImpl)) {
            return null;
        }
        TemporalTableFunctionImpl temporalTableFunction =
                (TemporalTableFunctionImpl) functionDefinition;

        checkState(
                rexCall.getOperands().size() == 1,
                "TemporalTableFunction call [%s] must have exactly one argument",
                rexCall);
        CorrelatedFieldAccessRemoval correlatedFieldAccessRemoval =
                new CorrelatedFieldAccessRemoval(temporalTableFunction, rexBuilder, leftSide);
        return new TemporalTableFunctionCall(
                temporalTableFunction,
                rexCall.getOperands().get(0).accept(correlatedFieldAccessRemoval));
    }
}

/**
 * This converts field accesses like `$cor0.o_rowtime` to valid input references for join condition
 * context without `$cor` reference.
 */
class CorrelatedFieldAccessRemoval extends RexDefaultVisitor<RexNode> {
    private TemporalTableFunctionImpl temporalTableFunction;
    private RexBuilder rexBuilder;
    private RelNode leftSide;

    public CorrelatedFieldAccessRemoval(
            TemporalTableFunctionImpl temporalTableFunction,
            RexBuilder rexBuilder,
            RelNode leftSide) {
        this.temporalTableFunction = temporalTableFunction;
        this.rexBuilder = rexBuilder;
        this.leftSide = leftSide;
    }

    @Override
    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
        int leftIndex = leftSide.getRowType().getFieldList().indexOf(fieldAccess.getField());
        if (leftIndex < 0) {
            throw new IllegalStateException(
                    "Failed to find reference to field ["
                            + fieldAccess.getField()
                            + "] in node ["
                            + leftSide
                            + "]");
        }
        return rexBuilder.makeInputRef(leftSide, leftIndex);
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
        return inputRef;
    }

    @Override
    public RexNode visitNode(RexNode rexNode) {
        throw new ValidationException(
                "Unsupported argument ["
                        + rexNode
                        + "] "
                        + "in "
                        + TemporalTableFunction.class.getSimpleName()
                        + " call of "
                        + "["
                        + temporalTableFunction.getUnderlyingHistoryTable()
                        + "] table");
    }
}
