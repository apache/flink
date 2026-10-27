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
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.planner.plan.nodes.calcite.TableAggregate;
import org.apache.flink.table.planner.plan.nodes.calcite.WatermarkAssigner;
import org.apache.flink.table.planner.utils.ShortcutUtils;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Set;

/**
 * Rejects a query that compares values of a type that is not comparable, see {@link
 * LogicalTypeChecks#isComparableType(LogicalType)}.
 *
 * <p>A query checks values for equality when it groups, joins or partitions by them, applies
 * DISTINCT or a set operation other than UNION ALL to them, or compares them with an operator such
 * as {@code =}, {@code <>}, {@code IN} or {@code IS DISTINCT FROM}. It also checks array elements
 * and map keys for equality when it passes them to a function such as {@code ARRAY_CONTAINS} or
 * {@code MAP_CONTAINS_KEY}, looks up a map key with {@code m[k]}, or collects values with {@code
 * COLLECT}. It orders values when it sorts by them with ORDER BY, or compares them with {@code <},
 * {@code <=}, {@code >} or {@code >=}.
 *
 * <p>The check runs on the logical plan before optimization. Comparisons that the optimizer adds
 * later only match a value with copies of itself and need no check.
 */
public final class ComparableTypeValidator {

    private static final String EQUALITY = "equality";
    private static final String ORDER = "order";

    /** Operators that check their operands for equality, also in a join condition. */
    private static final Set<SqlKind> EQUALITY_OPERATORS =
            Set.of(
                    SqlKind.EQUALS,
                    SqlKind.NOT_EQUALS,
                    SqlKind.IS_DISTINCT_FROM,
                    SqlKind.IS_NOT_DISTINCT_FROM,
                    SqlKind.IN,
                    SqlKind.NOT_IN);

    /** Operators that order their operands. */
    private static final Set<SqlKind> ORDER_OPERATORS =
            Set.of(
                    SqlKind.LESS_THAN,
                    SqlKind.LESS_THAN_OR_EQUAL,
                    SqlKind.GREATER_THAN,
                    SqlKind.GREATER_THAN_OR_EQUAL);

    /** Built-in functions that compare the elements of an array or the keys of a map. */
    private static final Set<FunctionDefinition> ELEMENT_COMPARING_FUNCTIONS =
            Set.of(
                    BuiltInFunctionDefinitions.ARRAY_CONTAINS,
                    BuiltInFunctionDefinitions.ARRAY_DISTINCT,
                    BuiltInFunctionDefinitions.ARRAY_POSITION,
                    BuiltInFunctionDefinitions.ARRAY_REMOVE,
                    BuiltInFunctionDefinitions.ARRAY_UNION,
                    BuiltInFunctionDefinitions.ARRAY_EXCEPT,
                    BuiltInFunctionDefinitions.ARRAY_INTERSECT,
                    BuiltInFunctionDefinitions.MAP_CONTAINS_KEY,
                    BuiltInFunctionDefinitions.MAP_UNION);

    /** Operators that compare the keys of a map or the values they collect. */
    private static final Set<SqlKind> ELEMENT_COMPARING_OPERATORS =
            Set.of(SqlKind.ITEM, SqlKind.COLLECT);

    private ComparableTypeValidator() {}

    /**
     * Throws a {@link ValidationException} if a plan checks values of a type that is not comparable
     * for equality or orders them.
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
                    } else if (ELEMENT_COMPARING_OPERATORS.contains(
                            call.getAggregation().getKind())) {
                        checkFields(
                                inputType,
                                ImmutableBitSet.of(call.getArgList()),
                                "as an argument of " + call.getAggregation().getName(),
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
            } else if (call.isA(EQUALITY_OPERATORS)) {
                checkComparison(call.getOperands(), call.getOperator().getName(), EQUALITY);
            } else if (call.isA(ORDER_OPERATORS)) {
                checkComparison(call.getOperands(), call.getOperator().getName(), ORDER);
            } else if (call.isA(ELEMENT_COMPARING_OPERATORS)) {
                checkElements(call);
            } else {
                final FunctionDefinition definition = ShortcutUtils.unwrapFunctionDefinition(call);
                if (definition != null && ELEMENT_COMPARING_FUNCTIONS.contains(definition)) {
                    checkElements(call);
                }
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
            } else if (over.isA(ELEMENT_COMPARING_OPERATORS)) {
                for (final RexNode operand : over.getOperands()) {
                    checkType(
                            "An expression",
                            operand.getType(),
                            "as an argument of " + over.getOperator().getName(),
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
            if (subQuery.isA(EQUALITY_OPERATORS)) {
                checkComparison(subQuery.getOperands(), subQuery.getOperator().getName(), EQUALITY);
            } else if (subQuery.getOperator() instanceof SqlQuantifyOperator) {
                final SqlKind comparisonKind =
                        ((SqlQuantifyOperator) subQuery.getOperator()).comparisonKind;
                checkComparison(
                        subQuery.getOperands(),
                        subQuery.getOperator().getName(),
                        ORDER_OPERATORS.contains(comparisonKind) ? ORDER : EQUALITY);
            }
            new PlanChecker().go(subQuery.rel);
            return super.visitSubQuery(subQuery);
        }

        private static void checkElements(final RexCall call) {
            final String usage = "in " + call.getOperator().getName();
            for (final RexNode operand : call.getOperands()) {
                final RelDataType type = operand.getType();
                // an index into an array compares no elements
                if (type.getSqlTypeName() == SqlTypeName.ARRAY && !call.isA(SqlKind.ITEM)) {
                    checkType("An array element", type.getComponentType(), usage, EQUALITY);
                } else if (type.getSqlTypeName() == SqlTypeName.MAP) {
                    checkType("A map key", type.getKeyType(), usage, EQUALITY);
                }
            }
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
        if (!LogicalTypeChecks.isComparableType(logicalType)) {
            throw new ValidationException(
                    String.format(
                            "%s of type %s cannot be used %s, because the type has no %s. "
                                    + "Cast the value to a comparable type first.",
                            subject, logicalType.copy(true).asSummaryString(), usage, requirement));
        }
    }
}
