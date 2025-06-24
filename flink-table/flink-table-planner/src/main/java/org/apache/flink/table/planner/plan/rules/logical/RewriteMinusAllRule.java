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

import org.apache.flink.table.planner.plan.utils.SetOpRewriteUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;
import org.immutables.value.Value;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.GREATER_THAN;

/**
 * Replaces logical {@link Minus} operator using a combination of union all, aggregate and table
 * function.
 *
 * <p>Original Query : {@code SELECT c1 FROM ut1 EXCEPT ALL SELECT c1 FROM ut2 }
 *
 * <pre>Rewritten Query:
 * {@code SELECT c1 FROM ( SELECT c1, sum_val FROM ( SELECT c1, sum(vcol_marker)
 * AS sum_val FROM ( SELECT c1, 1L as vcol_marker FROM ut1 UNION ALL SELECT c1, -1L as vcol_marker
 * FROM ut2 ) AS union_all GROUP BY union_all.c1 ) WHERE sum_val > 0 )
 * LATERAL TABLE(replicate_row(sum_val, c1)) AS T(c1) }
 * </pre>
 *
 * <p>Only handle the case of input size 2.
 */
@Value.Enclosing
public class RewriteMinusAllRule extends RelRule<RewriteMinusAllRule.RewriteMinusAllRuleConfig> {
    public static final RewriteMinusAllRule INSTANCE = RewriteMinusAllRuleConfig.DEFAULT.toRule();

    protected RewriteMinusAllRule(RewriteMinusAllRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Minus minus = call.rel(0);
        return minus.all && minus.getInputs().size() == 2;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Minus minus = call.rel(0);
        RelNode left = minus.getInput(0);
        RelNode right = minus.getInput(1);

        List<Integer> fields = Util.range(minus.getRowType().getFieldCount());

        // 1. add vcol_marker to left rel node
        RelBuilder leftBuilder = call.builder();
        RelNode leftWithAddedVirtualCols =
                leftBuilder
                        .push(left)
                        .project(
                                Stream.concat(
                                                leftBuilder.fields(fields).stream(),
                                                Stream.of(
                                                        leftBuilder.alias(
                                                                leftBuilder.cast(
                                                                        leftBuilder.literal(1L),
                                                                        BIGINT),
                                                                "vcol_marker")))
                                        .collect(Collectors.toList()))
                        .build();

        // 2. add vcol_marker to right rel node
        RelBuilder rightBuilder = call.builder();
        RelNode rightWithAddedVirtualCols =
                rightBuilder
                        .push(right)
                        .project(
                                Stream.concat(
                                                rightBuilder.fields(fields).stream(),
                                                Stream.of(
                                                        rightBuilder.alias(
                                                                leftBuilder.cast(
                                                                        leftBuilder.literal(-1L),
                                                                        BIGINT),
                                                                "vcol_marker")))
                                        .collect(Collectors.toList()))
                        .build();

        // 3. add union all and aggregate
        RelBuilder builder = call.builder();
        builder.push(leftWithAddedVirtualCols)
                .push(rightWithAddedVirtualCols)
                .union(true)
                .aggregate(
                        builder.groupKey(builder.fields(fields)),
                        builder.sum(false, "sum_vcol_marker", builder.field("vcol_marker")))
                .filter(
                        builder.call(
                                GREATER_THAN, builder.field("sum_vcol_marker"), builder.literal(0)))
                .project(
                        Stream.concat(
                                        Stream.of(builder.field("sum_vcol_marker")),
                                        builder.fields(fields).stream())
                                .collect(Collectors.toList()));

        // 4. add table function to replicate rows
        RelNode output = SetOpRewriteUtil.replicateRows(builder, minus.getRowType(), fields);

        call.transformTo(output);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface RewriteMinusAllRuleConfig extends RelRule.Config {
        RewriteMinusAllRule.RewriteMinusAllRuleConfig DEFAULT =
                ImmutableRewriteMinusAllRule.RewriteMinusAllRuleConfig.builder()
                        .operandSupplier(b0 -> b0.operand(Minus.class).anyInputs())
                        .relBuilderFactory(RelFactories.LOGICAL_BUILDER)
                        .description("RewriteMinusAllRule")
                        .build();

        @Override
        default RewriteMinusAllRule toRule() {
            return new RewriteMinusAllRule(this);
        }
    }
}
