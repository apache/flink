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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Util class that rewrite {@link org.apache.calcite.rel.core.SetOp}. */
public class SetOpRewriteUtil {

    /**
     * Generate equals condition by keys (The index on both sides is the same) to join left relNode
     * and right relNode.
     */
    public static List<RexNode> generateEqualsCondition(
            RelBuilder relBuilder, RelNode left, RelNode right, List<Integer> keys) {
        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        List<RelDataType> leftTypes = RelOptUtil.getFieldTypeList(left.getRowType());
        List<RelDataType> rightTypes = RelOptUtil.getFieldTypeList(right.getRowType());

        List<RexNode> conditions =
                keys.stream()
                        .map(
                                key -> {
                                    RexNode leftRex =
                                            rexBuilder.makeInputRef(leftTypes.get(key), key);
                                    RexNode rightRex =
                                            rexBuilder.makeInputRef(
                                                    rightTypes.get(key), leftTypes.size() + key);
                                    RexNode equalCond =
                                            rexBuilder.makeCall(
                                                    SqlStdOperatorTable.EQUALS, leftRex, rightRex);
                                    return relBuilder.or(
                                            equalCond,
                                            relBuilder.and(
                                                    relBuilder.isNull(leftRex),
                                                    relBuilder.isNull(rightRex)));
                                })
                        .collect(Collectors.toList());

        return conditions;
    }

    /**
     * Use table function to replicate the row N times. First field is long type, and the rest are
     * the row fields.
     */
    public static RelNode replicateRows(
            RelBuilder relBuilder, RelDataType outputRelDataType, List<Integer> fields) {
        RelOptCluster cluster = relBuilder.getCluster();

        BridgingSqlFunction sqlFunction =
                BridgingSqlFunction.of(cluster, BuiltInFunctionDefinitions.INTERNAL_REPLICATE_ROWS);

        FlinkRelBuilder.pushFunctionScan(
                relBuilder,
                sqlFunction,
                0,
                relBuilder.fields(Util.range(fields.size() + 1)),
                outputRelDataType.getFieldNames());

        // correlated join
        Set<CorrelationId> corSet = Collections.singleton(cluster.createCorrel());
        RelNode output =
                relBuilder
                        .join(JoinRelType.INNER, relBuilder.literal(true), corSet)
                        .project(
                                relBuilder.fields(
                                        Util.range(fields.size() + 1, fields.size() * 2 + 1)))
                        .build();
        return output;
    }
}
