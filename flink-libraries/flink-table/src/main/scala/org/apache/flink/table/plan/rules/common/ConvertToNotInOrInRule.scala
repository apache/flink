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
import org.apache.calcite.plan.{RelOptRule, RelOptUtil}
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rex.{RexCall, RexLiteral, RexNode}
import org.apache.calcite.sql.SqlBinaryOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.tools.RelBuilder

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Common convert rule for converting a cascade of predicates to IN or NOT_IN.
  */
abstract class ConvertToNotInOrInRule extends RelOptRule(
  operand(classOf[Filter], any), "ConvertToNotInOrInRule") {

  /**
    * Returns a condition decomposed by AND or OR.
    */
  def decomposedBy(rex: RexNode, operator: SqlBinaryOperator): Seq[RexNode] = {
    operator match {
      case AND => RelOptUtil.conjunctions(rex)
      case OR => RelOptUtil.disjunctions(rex)
    }
  }

  /**
    * Convert a cascade predicates to IN or NOT_IN.
    *
    * @param builder         The [[RelBuilder]] to build the [[RexNode]].
    * @param rex             The predicates to be converted.
    * @param fromOperator    The fromOperator, for example, when convert to IN,
    *                        fromOperator is EQUALS. We convert a cascade of EQUALS to IN.
    * @param connectOperator The connect operator to connect the fromOperator.
    * @param breakOperator   The break operator that break predicates into multi IN or NOT_IN.
    * @param toOperator      The toOperator, for example, when convert to IN, toOperator is IN.
    *                        We convert a cascade of EQUALS to IN.
    * @return The converted predicates.
    */
  protected def convertToNotInOrIn(
    builder: RelBuilder,
    rex: RexNode,
    fromOperator: SqlBinaryOperator,
    connectOperator: SqlBinaryOperator,
    breakOperator: SqlBinaryOperator,
    toOperator: SqlBinaryOperator): Option[RexNode] = {

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
          case `breakOperator` =>
            val newRex = decomposedBy(call, breakOperator).map({ r =>
              convertToNotInOrIn(
                builder, r, fromOperator, connectOperator, breakOperator, toOperator) match {
                case Some(newRex) =>
                  beenConverted = true
                  newRex
                case None => r
              }
            })
            breakOperator match {
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
