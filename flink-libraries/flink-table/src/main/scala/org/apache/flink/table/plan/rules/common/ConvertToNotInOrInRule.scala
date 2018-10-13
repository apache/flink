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

package org.apache.flink.table.plan.rules.common

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rex.{RexCall, RexLiteral, RexNode}
import org.apache.calcite.sql.SqlBinaryOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{IN, NOT_IN, EQUALS, NOT_EQUALS, AND, OR}
import org.apache.calcite.tools.RelBuilder

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Rule for converting a cascade of predicates to [[IN]] or [[NOT_IN]].
  *
  * For example, convert predicate: (x = 1 OR x = 2 OR x = 3) AND y = 4 to
  * predicate: x IN (1, 2, 3) AND y = 4.
  *
  * @param toOperator       The toOperator, for example, when convert to [[IN]], toOperator is
  *                         [[IN]]. We convert a cascade of [[EQUALS]] to [[IN]].
  * @param description      The description of the rule.
  */
class ConvertToNotInOrInRule(
    toOperator: SqlBinaryOperator,
    description: String)
  extends RelOptRule(
    operand(classOf[Filter], any),
    description) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val filter: Filter = call.rel(0)
    convertToNotInOrIn(call.builder(), filter.getCondition) match {
      case Some(newRex) =>
        call.transformTo(filter.copy(
          filter.getTraitSet,
          filter.getInput,
          newRex))

      case None => // do nothing
    }
  }

  /**
    * Returns a condition decomposed by [[AND]] or [[OR]].
    */
  private def decomposedBy(rex: RexNode, operator: SqlBinaryOperator): Seq[RexNode] = {
    operator match {
      case AND => RelOptUtil.conjunctions(rex)
      case OR => RelOptUtil.disjunctions(rex)
    }
  }

  /**
    * Convert a cascade predicates to [[IN]] or [[NOT_IN]].
    *
    * @param builder The [[RelBuilder]] to build the [[RexNode]].
    * @param rex     The predicates to be converted.
    * @return The converted predicates.
    */
  private def convertToNotInOrIn(
    builder: RelBuilder,
    rex: RexNode): Option[RexNode] = {

    // For example, when convert to [[IN]], fromOperator is [[EQUALS]].
    // We convert a cascade of [[EQUALS]] to [[IN]].
    // A connect operator is used to connect the fromOperator.
    // A composed operator may contains sub [[IN]] or [[NOT_IN]].
    val (fromOperator, connectOperator, composedOperator) = toOperator match {
      case IN => (EQUALS, OR, AND)
      case NOT_IN => (NOT_EQUALS, AND, OR)
    }

    val decomposed = decomposedBy(rex, connectOperator)
    val combineMap = new mutable.HashMap[String, mutable.ListBuffer[RexCall]]
    val rexBuffer = new mutable.ArrayBuffer[RexNode]
    var beenConverted = false

    // traverse decomposed predicates
    decomposed.foreach {
      case call: RexCall =>
        call.getOperator match {
          // put same predicates into combine map
          case `fromOperator` =>
            (call.operands(0), call.operands(1)) match {
              case (ref, _: RexLiteral) =>
                combineMap.getOrElseUpdate(ref.toString, mutable.ListBuffer[RexCall]()) += call
              case (l: RexLiteral, ref) =>
                combineMap.getOrElseUpdate(ref.toString, mutable.ListBuffer[RexCall]()) +=
                  call.clone(call.getType, List(ref, l))
              case _ => rexBuffer += call
            }

          // process sub predicates
          case `composedOperator` =>
            val newRex = decomposedBy(call, composedOperator).map({ r =>
              convertToNotInOrIn(builder, r) match {
                case Some(newRex) =>
                  beenConverted = true
                  newRex
                case None => r
              }
            })
            composedOperator match {
              case AND => rexBuffer += builder.and(newRex)
              case OR => rexBuffer += builder.or(newRex)
            }

          case _ => rexBuffer += call
        }

      case rex => rexBuffer += rex
    }

    combineMap.values.foreach { list =>
      // only convert to IN or NOT_IN when size >= 3.
      if (list.size >= 3) {
        val inputRef = list.head.getOperands.head
        val values = list.map(_.getOperands.last)
        rexBuffer += builder.getRexBuilder.makeCall(toOperator, List(inputRef) ++ values)
        beenConverted = true
      } else {
        connectOperator match {
          case AND => rexBuffer += builder.and(list)
          case OR => rexBuffer += builder.or(list)
        }
      }
    }

    if (beenConverted) {
      // return result if has been converted
      connectOperator match {
        case AND => Some(builder.and(rexBuffer))
        case OR => Some(builder.or(rexBuffer))
      }
    } else {
      None
    }
  }
}

object ConvertToNotInOrInRule {

  /**
    * Rule to convert multi [[EQUALS]] to [[IN]].
    *
    * For example, convert predicate: (x = 1 OR x = 2 OR x = 3) AND y = 4 to
    * predicate: x IN (1, 2, 3) AND y = 4.
    *
    */
  val IN_INSTANCE = new ConvertToNotInOrInRule(IN, "MergeMultiEqualsToInRule")

  /**
    * Rule to convert multi [[NOT_EQUALS]] to [[NOT_IN]].
    *
    * For example, convert predicate: (x <> 1 AND x <> 2 AND x <> 3) OR y <> 4 to
    * predicate: x NOT_IN (1, 2, 3) OR y <> 4.
    *
    */
  val NOT_IN_INSTANCE = new ConvertToNotInOrInRule(NOT_IN, "MergeMultiNotEqualsToNotInRule")
}
