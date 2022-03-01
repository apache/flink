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

import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.GREATER_THAN
import org.apache.flink.table.planner.plan.utils.SetOpRewriteUtil.replicateRows

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.core.{Minus, RelFactories}
import org.apache.calcite.sql.`type`.SqlTypeName.BIGINT
import org.apache.calcite.util.Util

import scala.collection.JavaConversions._

/**
  * Replaces logical [[Minus]] operator using a combination of union all, aggregate
  * and table function.
  *
  * Original Query :
  * {{{
  *    SELECT c1 FROM ut1 EXCEPT ALL SELECT c1 FROM ut2
  * }}}
  *
  * Rewritten Query:
  * {{{
  *   SELECT c1
  *   FROM (
  *     SELECT c1, sum_val
  *     FROM (
  *       SELECT c1, sum(vcol_marker) AS sum_val
  *       FROM (
  *         SELECT c1, 1L as vcol_marker FROM ut1
  *         UNION ALL
  *         SELECT c1, -1L as vcol_marker FROM ut2
  *       ) AS union_all
  *       GROUP BY union_all.c1
  *     )
  *     WHERE sum_val > 0
  *   )
  *   LATERAL TABLE(replicate_row(sum_val, c1)) AS T(c1)
  * }}}
  *
  * Only handle the case of input size 2.
  */
class RewriteMinusAllRule extends RelOptRule(
  operand(classOf[Minus], any),
  RelFactories.LOGICAL_BUILDER,
  "RewriteMinusAllRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val minus: Minus = call.rel(0)
    minus.all && minus.getInputs.size() == 2
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val minus: Minus = call.rel(0)
    val left = minus.getInput(0)
    val right = minus.getInput(1)

    val fields = Util.range(minus.getRowType.getFieldCount)

    // 1. add vcol_marker to left rel node
    val leftBuilder = call.builder
    val leftWithAddedVirtualCols = leftBuilder
        .push(left)
        .project(leftBuilder.fields(fields) ++
            Seq(leftBuilder.alias(
              leftBuilder.cast(leftBuilder.literal(1L), BIGINT), "vcol_marker")))
        .build()

    // 2. add vcol_marker to right rel node
    val rightBuilder = call.builder
    val rightWithAddedVirtualCols = rightBuilder
        .push(right)
        .project(rightBuilder.fields(fields) ++
            Seq(rightBuilder.alias(
              leftBuilder.cast(leftBuilder.literal(-1L), BIGINT), "vcol_marker")))
        .build()

    // 3. add union all and aggregate
    val builder = call.builder
    builder
        .push(leftWithAddedVirtualCols)
        .push(rightWithAddedVirtualCols)
        .union(true)
        .aggregate(
          builder.groupKey(builder.fields(fields)),
          builder.sum(false, "sum_vcol_marker", builder.field("vcol_marker")))
        .filter(builder.call(
          GREATER_THAN,
          builder.field("sum_vcol_marker"),
          builder.literal(0)))
        .project(Seq(builder.field("sum_vcol_marker")) ++ builder.fields(fields))

    // 4. add table function to replicate rows
    val output = replicateRows(builder, minus.getRowType, fields)

    call.transformTo(output)
  }
}

object RewriteMinusAllRule {
  val INSTANCE: RelOptRule = new RewriteMinusAllRule
}
