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

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_IN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OR;

/**
 * Rule for converting a cascade of predicates to {@link SqlStdOperatorTable#IN} or {@link
 * SqlStdOperatorTable#NOT_IN}.
 *
 * <p>For example, 1. convert predicate: {@code (x = 1 OR x = 2 OR x = 3 OR x = 4) AND y = 5} to
 * predicate: {@code x IN (1, 2, 3, 4) AND y = 5}. 2. convert predicate: {@code (x <> 1 AND x <> 2
 * AND x <> 3 AND x <> 4) AND y = 5} to predicate: {@code x NOT IN (1, 2, 3, 4) AND y = 5}.
 */
@Value.Enclosing
public class ConvertToNotInOrInRule
        extends RelRule<ConvertToNotInOrInRule.ConvertToNotInOrInRuleConfig> {

    public static final ConvertToNotInOrInRule INSTANCE =
            ConvertToNotInOrInRule.ConvertToNotInOrInRuleConfig.DEFAULT.toRule();
    // these threshold values are set by OptimizableHashSet benchmark test on different type.
    // threshold for non-float and non-double type
    private static final int THRESHOLD = 4;
    // threshold for float and double type
    private static final int FRACTIONAL_THRESHOLD = 20;

    protected ConvertToNotInOrInRule(ConvertToNotInOrInRuleConfig config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        RexNode condition = filter.getCondition();

        // convert equal expression connected by OR to IN
        Optional<RexNode> inExpr = convertToNotInOrIn(call.builder(), condition, IN);
        // convert not-equal expression connected by AND to NOT_IN
        Optional<RexNode> notInExpr =
                convertToNotInOrIn(call.builder(), inExpr.orElse(condition), NOT_IN);

        // check IN conversion if NOT_IN conversion is fail
        if (notInExpr.isPresent() || inExpr.isPresent()) {
            RexNode expr = notInExpr.orElseGet(inExpr::get);
            Filter newFilter = filter.copy(filter.getTraitSet(), filter.getInput(), expr);
            call.transformTo(newFilter);
        }
    }

    /**
     * Returns a condition decomposed by {@link SqlStdOperatorTable#AND} or {@link
     * SqlStdOperatorTable#OR}.
     */
    private List<RexNode> decomposedBy(RexNode rex, SqlBinaryOperator operator) {
        final SqlKind kind = operator.getKind();
        switch (kind) {
            case AND:
                return RelOptUtil.conjunctions(rex);
            case OR:
                return RelOptUtil.disjunctions(rex);
            default:
                throw new AssertionError("Unsupported operator " + kind);
        }
    }

    /**
     * Convert a cascade predicates to {@link SqlStdOperatorTable#IN} or {@link
     * SqlStdOperatorTable#NOT_IN}.
     *
     * @param builder The {@link RelBuilder} to build the {@link RexNode}.
     * @param rex The predicates to be converted.
     * @return The converted predicates.
     */
    private Optional<RexNode> convertToNotInOrIn(
            RelBuilder builder, RexNode rex, SqlBinaryOperator toOperator) {

        // For example, when convert to [[IN]], fromOperator is [[EQUALS]].
        // We convert a cascade of [[EQUALS]] to [[IN]].
        // A connect operator is used to connect the fromOperator.
        // A composed operator may contains sub [[IN]] or [[NOT_IN]].
        SqlBinaryOperator fromOperator;
        SqlBinaryOperator connectOperator;
        SqlBinaryOperator composedOperator;
        switch (toOperator.kind) {
            case IN:
                fromOperator = EQUALS;
                connectOperator = OR;
                composedOperator = AND;
                break;
            case NOT_IN:
                fromOperator = NOT_EQUALS;
                connectOperator = AND;
                composedOperator = OR;
                break;
            default:
                throw new AssertionError("Unsupported operator " + toOperator);
        }

        List<RexNode> decomposed = decomposedBy(rex, connectOperator);
        Map<String, List<RexCall>> combineMap = new HashMap<>();
        List<RexNode> rexBuffer = new ArrayList<>();
        boolean[] beenConverted = new boolean[] {false};

        // traverse decomposed predicates
        decomposed.forEach(
                rexNode -> {
                    if (rexNode instanceof RexCall) {
                        RexCall call = (RexCall) rexNode;

                        if (call.getOperator() == fromOperator) {
                            // put same predicates into combine map
                            RexNode rexNode0 = call.operands.get(0);
                            RexNode rexNode1 = call.operands.get(1);
                            if (rexNode1 instanceof RexLiteral) {
                                combineMap
                                        .computeIfAbsent(
                                                rexNode0.toString(), k -> new ArrayList<>())
                                        .add(call);
                            } else if (rexNode0 instanceof RexLiteral) {
                                combineMap
                                        .computeIfAbsent(
                                                rexNode1.toString(), k -> new ArrayList<>())
                                        .add(
                                                call.clone(
                                                        call.getType(),
                                                        Arrays.asList(rexNode1, rexNode0)));
                            } else {
                                rexBuffer.add(call);
                            }
                        } else if (call.getOperator() == composedOperator) {
                            // process sub predicates
                            List<RexNode> newRex =
                                    decomposedBy(call, composedOperator).stream()
                                            .map(
                                                    r -> {
                                                        Optional<RexNode> ex =
                                                                convertToNotInOrIn(
                                                                        builder, r, toOperator);
                                                        if (ex.isPresent()) {
                                                            beenConverted[0] = true;
                                                            return ex.get();
                                                        } else {
                                                            return r;
                                                        }
                                                    })
                                            .collect(Collectors.toList());
                            switch (composedOperator.kind) {
                                case AND:
                                    rexBuffer.add(builder.and(newRex));
                                    break;
                                case OR:
                                    rexBuffer.add(builder.or(newRex));
                                    break;
                                default:
                                    throw new AssertionError(
                                            "Unsupported operator " + composedOperator);
                            }
                        } else {
                            rexBuffer.add(call);
                        }
                    } else {
                        rexBuffer.add(rexNode);
                    }
                });

        combineMap
                .values()
                .forEach(
                        list -> {
                            if (needConvert(list)) {
                                RexNode inputRef = list.get(0).getOperands().get(0);
                                List<RexNode> values =
                                        list.stream()
                                                .map(
                                                        call ->
                                                                call.getOperands()
                                                                        .get(
                                                                                call.getOperands()
                                                                                                .size()
                                                                                        - 1))
                                                .collect(Collectors.toList());
                                RexNode call =
                                        toOperator == IN
                                                ? builder.getRexBuilder().makeIn(inputRef, values)
                                                : builder.getRexBuilder()
                                                        .makeCall(
                                                                NOT,
                                                                builder.getRexBuilder()
                                                                        .makeIn(inputRef, values));
                                rexBuffer.add(call);
                                beenConverted[0] = true;
                            } else {
                                switch (connectOperator.kind) {
                                    case AND:
                                        rexBuffer.add(builder.and(list));
                                        break;
                                    case OR:
                                        rexBuffer.add(builder.or(list));
                                        break;
                                    default:
                                        throw new AssertionError(
                                                "Unsupported operator " + connectOperator);
                                }
                            }
                        });

        if (beenConverted[0]) {
            // return result if has been converted
            if (connectOperator == AND) {
                return Optional.of(builder.and(rexBuffer));
            } else {
                return Optional.of(builder.or(rexBuffer));
            }
        } else {
            return Optional.empty();
        }
    }

    private boolean needConvert(List<RexCall> rexNodes) {
        RexNode inputRef = rexNodes.get(0).getOperands().get(0);
        LogicalTypeRoot logicalTypeRoot =
                FlinkTypeFactory.toLogicalType(inputRef.getType()).getTypeRoot();
        switch (logicalTypeRoot) {
            case FLOAT:
            case DOUBLE:
                return rexNodes.size() >= FRACTIONAL_THRESHOLD;
            default:
                return rexNodes.size() >= THRESHOLD;
        }
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface ConvertToNotInOrInRuleConfig extends RelRule.Config {
        ConvertToNotInOrInRule.ConvertToNotInOrInRuleConfig DEFAULT =
                ImmutableConvertToNotInOrInRule.ConvertToNotInOrInRuleConfig.builder()
                        .operandSupplier(b0 -> b0.operand(Filter.class).anyInputs())
                        .description("ConvertToNotInOrInRule")
                        .build();

        @Override
        default ConvertToNotInOrInRule toRule() {
            return new ConvertToNotInOrInRule(this);
        }
    }
}
