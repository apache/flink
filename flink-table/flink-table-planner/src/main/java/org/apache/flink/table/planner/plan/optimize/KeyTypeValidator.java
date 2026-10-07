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

package org.apache.flink.table.planner.plan.optimize;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.planner.plan.nodes.calcite.TableAggregate;
import org.apache.flink.table.planner.plan.nodes.calcite.WatermarkAssigner;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Rejects queries that group, join, partition, sort or compare values of a type that is not a
 * comparable key type, see {@link LogicalTypeChecks#isComparableKeyType(LogicalType)}.
 *
 * <p>The check runs on the logical plan before optimization. Keys that the optimizer adds later
 * only match a value with copies of itself and need no check.
 */
public final class KeyTypeValidator {

    private static final String EQUALITY = "equality";
    private static final String ORDER = "order";

    private KeyTypeValidator() {}

    /**
     * Throws a {@link ValidationException} if a plan groups, joins, partitions, sorts or compares
     * values of a type that is not a comparable key type.
     */
    public static void validate(final List<RelNode> roots) {
        roots.forEach(root -> new PlanChecker().go(root));
    }

    private static final class PlanChecker extends RelVisitor {

        private final RexShuttle expressionChecker = new ExpressionChecker();

        @Override
        public void visit(final RelNode node, final int ordinal, @Nullable final RelNode parent) {
            checkKeys(node);
            node.accept(expressionChecker);
            super.visit(node, ordinal, parent);
        }

        private void checkKeys(final RelNode node) {
            if (node instanceof Aggregate) {
                final Aggregate aggregate = (Aggregate) node;
                final RelDataType inputType = aggregate.getInput().getRowType();
                checkFields(inputType, aggregate.getGroupSet(), "as a grouping key", EQUALITY);
                for (final AggregateCall call : aggregate.getAggCallList()) {
                    if (call.isDistinct()) {
                        checkFields(
                                inputType,
                                ImmutableBitSet.of(call.getArgList()),
                                "as an argument of a DISTINCT aggregate",
                                EQUALITY);
                    }
                }
            } else if (node instanceof TableAggregate) {
                final TableAggregate aggregate = (TableAggregate) node;
                checkFields(
                        aggregate.getInput().getRowType(),
                        aggregate.getGroupSet(),
                        "as a grouping key",
                        EQUALITY);
            } else if (node instanceof SetOp) {
                final SetOp setOp = (SetOp) node;
                if (!(setOp instanceof Union && setOp.all)) {
                    final RelDataType rowType = setOp.getRowType();
                    checkFields(
                            rowType,
                            ImmutableBitSet.range(rowType.getFieldCount()),
                            "in " + setOp.kind.sql + (setOp.all ? " ALL" : ""),
                            EQUALITY);
                }
            } else if (node instanceof Sort) {
                final Sort sort = (Sort) node;
                checkCollation(
                        sort.getInput().getRowType(), sort.getCollation(), "as an ORDER BY key");
            } else if (node instanceof Match) {
                // Match and WatermarkAssigner do not expose their expressions to a RexShuttle
                final Match match = (Match) node;
                final RelDataType inputType = match.getInput().getRowType();
                checkFields(
                        inputType,
                        match.getPartitionKeys(),
                        "as a PARTITION BY key of MATCH_RECOGNIZE",
                        EQUALITY);
                checkCollation(
                        inputType, match.getOrderKeys(), "as an ORDER BY key of MATCH_RECOGNIZE");
                match.getPatternDefinitions().values().forEach(e -> e.accept(expressionChecker));
                match.getMeasures().values().forEach(e -> e.accept(expressionChecker));
            } else if (node instanceof WatermarkAssigner) {
                ((WatermarkAssigner) node).watermarkExpr().accept(expressionChecker);
            }
        }

        private void checkCollation(
                final RelDataType rowType, final RelCollation collation, final String usage) {
            for (final RelFieldCollation fieldCollation : collation.getFieldCollations()) {
                checkField(rowType, fieldCollation.getFieldIndex(), usage, ORDER);
            }
        }
    }

    private static final class ExpressionChecker extends RexShuttle {

        @Override
        public RexNode visitCall(final RexCall call) {
            if (call instanceof RexTableArgCall) {
                final RexTableArgCall tableArg = (RexTableArgCall) call;
                checkFields(
                        tableArg.getType(),
                        ImmutableBitSet.of(tableArg.getPartitionKeys()),
                        "as a PARTITION BY key of a table argument",
                        EQUALITY);
                checkFields(
                        tableArg.getType(),
                        ImmutableBitSet.of(tableArg.getOrderKeys()),
                        "as an ORDER BY key of a table argument",
                        ORDER);
            } else if (call.isA(SqlKind.BINARY_COMPARISON) || call.isA(SqlKind.COMPARISON)) {
                checkComparison(
                        call.getOperands(),
                        call.getOperator().getName(),
                        call.isA(SqlKind.ORDER_COMPARISON) ? ORDER : EQUALITY);
            }
            return super.visitCall(call);
        }

        @Override
        public RexNode visitOver(final RexOver over) {
            if (over.isDistinct()) {
                for (final RexNode operand : over.getOperands()) {
                    checkType(
                            "An expression",
                            operand.getType(),
                            "as an argument of a DISTINCT aggregate",
                            EQUALITY);
                }
            }
            for (final RexNode key : over.getWindow().partitionKeys) {
                checkType(
                        "An expression",
                        key.getType(),
                        "as a PARTITION BY key of an OVER window",
                        EQUALITY);
            }
            for (final RexFieldCollation key : over.getWindow().orderKeys) {
                checkType(
                        "An expression",
                        key.getKey().getType(),
                        "as an ORDER BY key of an OVER window",
                        ORDER);
            }
            return super.visitOver(over);
        }

        @Override
        public RexNode visitSubQuery(final RexSubQuery subQuery) {
            if (subQuery.isA(SqlKind.IN) || subQuery.isA(SqlKind.NOT_IN)) {
                checkComparison(subQuery.getOperands(), subQuery.getOperator().getName(), EQUALITY);
            } else if (subQuery.getOperator() instanceof SqlQuantifyOperator) {
                final SqlKind comparisonKind =
                        ((SqlQuantifyOperator) subQuery.getOperator()).comparisonKind;
                checkComparison(
                        subQuery.getOperands(),
                        subQuery.getOperator().getName(),
                        SqlKind.ORDER_COMPARISON.contains(comparisonKind) ? ORDER : EQUALITY);
            }
            new PlanChecker().go(subQuery.rel);
            return super.visitSubQuery(subQuery);
        }

        private static void checkComparison(
                final List<RexNode> operands, final String operatorName, final String requirement) {
            // a comparison with NULL never compares two values
            if (operands.stream().anyMatch(operand -> RexUtil.isNullLiteral(operand, true))) {
                return;
            }
            for (final RexNode operand : operands) {
                checkType(
                        "An expression",
                        operand.getType(),
                        "in a comparison with '" + operatorName + "'",
                        requirement);
            }
        }
    }

    private static void checkFields(
            final RelDataType rowType,
            final ImmutableBitSet fields,
            final String usage,
            final String requirement) {
        for (final int field : fields) {
            checkField(rowType, field, usage, requirement);
        }
    }

    private static void checkField(
            final RelDataType rowType,
            final int index,
            final String usage,
            final String requirement) {
        final RelDataTypeField field = rowType.getFieldList().get(index);
        final String name = field.getName();
        // names such as EXPR$0 or $f0 come from the planner and mean nothing to the user
        final String subject =
                name.startsWith("$") || SqlUtil.isGeneratedAlias(name)
                        ? "An expression"
                        : String.format("Column '%s'", name);
        checkType(subject, field.getType(), usage, requirement);
    }

    private static void checkType(
            final String subject,
            final RelDataType type,
            final String usage,
            final String requirement) {
        final LogicalType logicalType = FlinkTypeFactory.toLogicalType(type);
        if (!LogicalTypeChecks.isComparableKeyType(logicalType)) {
            throw new ValidationException(
                    String.format(
                            "%s of type %s cannot be used %s, because the type has no %s. "
                                    + "Cast the value to a comparable type first.",
                            subject, logicalType.copy(true).asSummaryString(), usage, requirement));
        }
    }
}
