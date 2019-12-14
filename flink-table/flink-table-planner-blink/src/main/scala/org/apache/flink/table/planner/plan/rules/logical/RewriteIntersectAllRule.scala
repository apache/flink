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

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.{GREATER_THAN, GREATER_THAN_OR_EQUAL, IF}
import org.apache.flink.table.planner.plan.utils.SetOpRewriteUtil.replicateRows

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.core.{Intersect, RelFactories}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.util.Util

import scala.collection.JavaConversions._

/**
  * Replaces logical [[Intersect]] operator using a combination of union all, aggregate
  * and table function.
  *
  * Original Query :
  * {{{
  *    SELECT c1 FROM ut1 INTERSECT ALL SELECT c1 FROM ut2
  * }}}
  *
  * Rewritten Query:
  * {{{
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
  *   LATERAL TABLE(replicate_row(min_count, c1)) AS T(c1)
  * }}}
  *
  * Only handle the case of input size 2.
  */
class RewriteIntersectAllRule extends RelOptRule(
  operand(classOf[Intersect], any),
  RelFactories.LOGICAL_BUILDER,
  "RewriteIntersectAllRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val intersect: Intersect = call.rel(0)
    intersect.all && intersect.getInputs.size() == 2
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val intersect: Intersect = call.rel(0)
    val left = intersect.getInput(0)
    val right = intersect.getInput(1)

    val fields = Util.range(intersect.getRowType.getFieldCount)

    // 1. add marker to left rel node
    val leftBuilder = call.builder
    val boolType = leftBuilder.getTypeFactory.createSqlType(SqlTypeName.BOOLEAN)
    val leftWithMarker = leftBuilder
        .push(left)
        .project(
          leftBuilder.fields(fields) ++ Seq(
            leftBuilder.alias(leftBuilder.literal(true), "vcol_left_marker"),
            leftBuilder.alias(
              leftBuilder.getRexBuilder.makeNullLiteral(boolType), "vcol_right_marker")))
        .build()

    // 2. add marker to right rel node
    val rightBuilder = call.builder
    val rightWithMarker = rightBuilder
        .push(right)
        .project(
          rightBuilder.fields(fields) ++ Seq(
            rightBuilder.alias(
              rightBuilder.getRexBuilder.makeNullLiteral(boolType), "vcol_left_marker"),
            rightBuilder.alias(rightBuilder.literal(true), "vcol_right_marker")))
        .build()

    // 3. union and aggregate
    val builder = call.builder
    builder
        .push(leftWithMarker)
        .push(rightWithMarker)
        .union(true)
        .aggregate(
          builder.groupKey(builder.fields(fields)),
          builder.count(false, "vcol_left_cnt", builder.field("vcol_left_marker")),
          builder.count(false, "vcol_right_cnt", builder.field("vcol_right_marker")))
        .filter(builder.and(
          builder.call(
            GREATER_THAN_OR_EQUAL,
            builder.field("vcol_left_cnt"),
            builder.literal(1)),
          builder.call(
            GREATER_THAN_OR_EQUAL,
            builder.field("vcol_right_cnt"),
            builder.literal(1))))
        .project(Seq(builder.call(
          IF,
          builder.call(
            GREATER_THAN,
            builder.field("vcol_left_cnt"),
            builder.field("vcol_right_cnt")),
          builder.field("vcol_right_cnt"),
          builder.field("vcol_left_cnt"))) ++ builder.fields(fields))

    // 4. add table function to replicate rows
    val output = replicateRows(builder, intersect.getRowType, fields)

    call.transformTo(output)
  }
}

object RewriteIntersectAllRule {
  val INSTANCE: RelOptRule = new RewriteIntersectAllRule
}
