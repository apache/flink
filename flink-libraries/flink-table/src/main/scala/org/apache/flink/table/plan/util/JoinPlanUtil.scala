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

package org.apache.flink.table.plan.util

import scala.collection.JavaConverters._
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType, Project}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.sql.fun.{SqlMonotonicBinaryOperator, SqlStdOperatorTable}

/**
  * Utilities for join plan translation.
  */
object JoinPlanUtil {

  /** Checks whether a join has equi-conditions. */
  def hasEqualityPredicates(joinInfo: JoinInfo): Boolean = {
    !joinInfo.pairs().isEmpty
  }

  /**
    * Checks whether the predicates can be replaced with a Calc followed by an equi-join.
    * (e.g., replace =($1, -($0, 1)) with $2=[-($0, 1)] and =($2, $1))
    * @param remainCond the join remaining conditions
    * @return true if the conditions can be replaced with equi-ones.
    */
  def canBeReplacedWithEquiPreds(remainCond: RexNode): Boolean = {
    var canBeReplaced: Boolean = false

    def checkCond(call: RexNode, andBranch: Boolean): Unit = call match {
      case x: RexCall if x.op == SqlStdOperatorTable.OR =>
        x.operands.asScala.foreach(checkCond(_, andBranch = false))
      case x: RexCall if x.op == SqlStdOperatorTable.AND =>
        x.operands.asScala.foreach(checkCond(_, andBranch))
      case x: RexCall if x.op == SqlStdOperatorTable.EQUALS =>
        // Found a predicate with "=" comparator.
        x.operands.asScala.foreach {
          case _: RexCall if andBranch => canBeReplaced = true
          case _ =>
        }
      case _ => // Just omit others.
    }

    checkCond(remainCond, andBranch = true)
    canBeReplaced
  }

  /** Checks whether joining with a single row. */
  def isSingleRowJoin(joinType: JoinRelType, left: RelNode, right: RelNode): Boolean = {
    joinType match {
      case JoinRelType.INNER if isSingleRow(right) || isSingleRow(left) => true
      case JoinRelType.LEFT if isSingleRow(right) => true
      case JoinRelType.RIGHT if isSingleRow(left) => true
      case _ => false
    }
  }

  /**
    * Recursively checks if a [[RelNode]] returns a single row.
    * The input must be a global aggregation possibly followed by projections or filters.
    */
  private def isSingleRow(node: RelNode): Boolean = {
    node match {
      case ss: RelSubset => isSingleRow(ss.getOriginal)
      case lp: Project => isSingleRow(lp.getInput)
      case lf: Filter => isSingleRow(lf.getInput)
      case lc: Calc => isSingleRow(lc.getInput)
      case la: Aggregate => la.getGroupSet.isEmpty
      case _ => false
    }
  }
}
