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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;
import org.immutables.value.Value;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.GREATER_THAN;
import static org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.GREATER_THAN_OR_EQUAL;
import static org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.IF;
import static org.apache.flink.table.planner.plan.utils.SetOpRewriteUtil.replicateRows;

/**
 * Replaces logical {@link Intersect} operator using a combination of union all, aggregate and table
 * function.
 *
 * <p>Original Query : {@code SELECT c1 FROM ut1 INTERSECT ALL SELECT c1 FROM ut2 }
 *
 * <pre>Rewritten Query:
 * {@code
 *   SELECT c1
 *   FROM (
 *     SELECT c1, If (vcol_left_cnt > vcol_right_cnt, vcol_right_cnt, vcol_left_cnt) AS min_count
 *     FROM (
 *       SELECT
 *         c1,
 *         count(vcol_left_marker) as vcol_left_cnt,
 *         count(vcol_right_marker) as vcol_right_cnt
 *       FROM (
 *         SELECT c1, true as vcol_left_marker, null as vcol_right_marker FROM ut1
 *         UNION ALL
 *         SELECT c1, null as vcol_left_marker, true as vcol_right_marker FROM ut2
 *       ) AS union_all
 *       GROUP BY c1
 *     )
 *     WHERE vcol_left_cnt >= 1 AND vcol_right_cnt >= 1
 *     )
 *   )
 *   LATERAL TABLE(replicate_row(min_count, c1)) AS T(c1) }
 * </pre>
 *
 * <p>Only handle the case of input size 2.
 */
@Value.Enclosing
public class RewriteIntersectAllRule
        extends RelRule<RewriteIntersectAllRule.RewriteIntersectAllRuleConfig> {
    public static final RewriteIntersectAllRule INSTANCE =
            RewriteIntersectAllRule.RewriteIntersectAllRuleConfig.DEFAULT.toRule();

    protected RewriteIntersectAllRule(RewriteIntersectAllRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Intersect intersect = call.rel(0);
        return intersect.all && intersect.getInputs().size() == 2;
    }

    public void onMatch(RelOptRuleCall call) {
        RelNode intersect = call.rel(0);
        RelNode left = intersect.getInput(0);
        RelNode right = intersect.getInput(1);

        List<Integer> fields = Util.range(intersect.getRowType().getFieldCount());

        // 1. add marker to left rel node
        RelBuilder leftBuilder = call.builder();
        RelDataType boolType = leftBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
        RelNode leftWithMarker =
                leftBuilder
                        .push(left)
                        .project(
                                Stream.concat(
                                                fields.stream().map(leftBuilder::field),
                                                Stream.of(
                                                        leftBuilder.alias(
                                                                leftBuilder.literal(true),
                                                                "vcol_left_marker"),
                                                        leftBuilder.alias(
                                                                leftBuilder
                                                                        .getRexBuilder()
                                                                        .makeNullLiteral(boolType),
                                                                "vcol_right_marker")))
                                        .collect(Collectors.toList()))
                        .build();

        // 2. add marker to right rel node
        RelBuilder rightBuilder = call.builder();
        RelNode rightWithMarker =
                rightBuilder
                        .push(right)
                        .project(
                                Stream.concat(
                                                fields.stream().map(rightBuilder::field),
                                                Stream.of(
                                                        rightBuilder.alias(
                                                                rightBuilder
                                                                        .getRexBuilder()
                                                                        .makeNullLiteral(boolType),
                                                                "vcol_left_marker"),
                                                        rightBuilder.alias(
                                                                rightBuilder.literal(true),
                                                                "vcol_right_marker")))
                                        .collect(Collectors.toList()))
                        .build();

        // 3. union and aggregate
        RelBuilder builder = call.builder();
        builder.push(leftWithMarker)
                .push(rightWithMarker)
                .union(true)
                .aggregate(
                        builder.groupKey(builder.fields(fields)),
                        builder.count(false, "vcol_left_cnt", builder.field("vcol_left_marker")),
                        builder.count(false, "vcol_right_cnt", builder.field("vcol_right_marker")))
                .filter(
                        builder.and(
                                builder.call(
                                        GREATER_THAN_OR_EQUAL,
                                        builder.field("vcol_left_cnt"),
                                        builder.literal(1)),
                                builder.call(
                                        GREATER_THAN_OR_EQUAL,
                                        builder.field("vcol_right_cnt"),
                                        builder.literal(1))))
                .project(
                        Stream.concat(
                                        Stream.of(
                                                builder.call(
                                                        IF,
                                                        builder.call(
                                                                GREATER_THAN,
                                                                builder.field("vcol_left_cnt"),
                                                                builder.field("vcol_right_cnt")),
                                                        builder.field("vcol_right_cnt"),
                                                        builder.field("vcol_left_cnt"))),
                                        builder.fields(fields).stream())
                                .collect(Collectors.toList()));

        // 4. add table function to replicate rows
        RelNode output = replicateRows(builder, intersect.getRowType(), fields);

        call.transformTo(output);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface RewriteIntersectAllRuleConfig extends RelRule.Config {
        RewriteIntersectAllRule.RewriteIntersectAllRuleConfig DEFAULT =
                ImmutableRewriteIntersectAllRule.RewriteIntersectAllRuleConfig.builder()
                        .operandSupplier(b0 -> b0.operand(Intersect.class).anyInputs())
                        .relBuilderFactory(RelFactories.LOGICAL_BUILDER)
                        .description("RewriteIntersectAllRule")
                        .build();

        @Override
        default RewriteIntersectAllRule toRule() {
            return new RewriteIntersectAllRule(this);
        }
    }
}
