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

package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.planner.plan.stats._
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil.{ColumnRelatedVisitor, getLiteralValueByBroadType}

import org.apache.calcite.rex.{RexBuilder, RexCall, RexInputRef, RexLiteral, RexNode, RexUtil}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlKind.{EQUALS, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL}
import org.apache.calcite.sql.fun.SqlStdOperatorTable

import scala.collection.JavaConversions._

/**
  * Helper for FlinkRelMdColumnInterval/FlinkRelMdFilteredColumnInterval.
  */
object ColumnIntervalUtil {

  /**
    * try get value interval if the operator is  +, -, *
    */
  def getValueIntervalOfRexCall(
      rexCall: RexCall,
      leftInterval: ValueInterval,
      rightInterval: ValueInterval): ValueInterval = {

    rexCall.op match {
      case SqlStdOperatorTable.PLUS =>
        getValueIntervalOfPlus(leftInterval, rightInterval)
      case SqlStdOperatorTable.MINUS =>
        getValueIntervalOfPlus(leftInterval, getNegativeOfValueInterval(rightInterval))
      case SqlStdOperatorTable.MULTIPLY =>
        getValueIntervalOfMultiply(leftInterval, rightInterval)
      case _ => null
    }

  }

  def getNegativeOfValueInterval(valueInterval: ValueInterval): ValueInterval = {
    valueInterval match {
      case EmptyValueInterval => valueInterval
      case InfiniteValueInterval => valueInterval
      case finite: FiniteValueInterval =>
        (convertNumberToString(finite.upper), convertNumberToString(finite.lower)) match {
          case (Some(lower), Some(upper)) =>
            val lowerValue = new java.math.BigDecimal(lower).negate()
            val upperValue = new java.math.BigDecimal(upper).negate()
            FiniteValueInterval(
              lowerValue,
              upperValue,
              finite.includeUpper,
              finite.includeLower
            )
          case _ => null
        }
      case left: LeftSemiInfiniteValueInterval =>
        convertNumberToString(left.upper) match {
          case Some(lower) =>
            val lowerValue = new java.math.BigDecimal(lower).negate()
            RightSemiInfiniteValueInterval(lowerValue, left.includeUpper)
          case _ => null
        }
      case right: RightSemiInfiniteValueInterval =>
        convertNumberToString(right.lower) match {
          case Some(upper) =>
            val upperValue = new java.math.BigDecimal(upper).negate()
            LeftSemiInfiniteValueInterval(upperValue, right.includeLower)
          case _ => null
        }
      case _ => null
    }
  }

  def getValueIntervalOfPlus(
      leftInterval: ValueInterval,
      rightInterval: ValueInterval): ValueInterval = {

    val lower = (leftInterval, rightInterval) match {
      case (l: WithLower, r: WithLower) =>
        (convertNumberToString(l.lower), convertNumberToString(r.lower)) match {
          case (Some(ll), Some(rl)) =>
            val newLower = new java.math.BigDecimal(ll).add(new java.math.BigDecimal(rl))
            Some(newLower, l.includeLower && r.includeLower)
          case _ => None
        }
      case _ => None
    }

    val upper = (leftInterval, rightInterval) match {
      case (l: WithUpper, r: WithUpper) =>
        (convertNumberToString(l.upper), convertNumberToString(r.upper)) match {
          case (Some(lu), Some(ru)) =>
            val newUpper = new java.math.BigDecimal(lu).add(new java.math.BigDecimal(ru))
            Some(newUpper, l.includeUpper && r.includeUpper)
        }
      case _ => None
    }

    (lower, upper) match {
      case (Some((low, includeLow)), Some((up, includeUp))) =>
        FiniteValueInterval(low, up, includeLow, includeUp)
      case (Some((low, includeLow)), _) =>
        RightSemiInfiniteValueInterval(low, includeLow)
      case (_, Some((up, includeUp))) =>
        LeftSemiInfiniteValueInterval(up, includeUp)
      case _ => null
    }
  }

  def getValueIntervalOfMultiply(
      leftInterval: ValueInterval,
      rightInterval: ValueInterval): ValueInterval = {

    (leftInterval, rightInterval) match {
      case (fl: FiniteValueInterval, fr: FiniteValueInterval) =>
        (convertNumberToString(fl.lower), convertNumberToString(fr.lower),
          convertNumberToString(fl.upper), convertNumberToString(fr.upper)) match {
          case (Some(ll), Some(rl), Some(lu), Some(ru)) =>
            val leftLower = new java.math.BigDecimal(ll)
            val rightLower = new java.math.BigDecimal(rl)
            val leftUpper = new java.math.BigDecimal(lu)
            val rightUpper = new java.math.BigDecimal(ru)
            val allMultiplyResults = Seq(
              leftLower.multiply(rightLower),
              leftLower.multiply(rightUpper),
              leftUpper.multiply(rightLower),
              leftUpper.multiply(rightUpper)).zip(
              Seq(
                fl.includeLower && fr.includeLower,
                fl.includeLower && fr.includeUpper,
                fl.includeUpper && fr.includeLower,
                fl.includeUpper && fr.includeUpper
              )
            )
            val sortedResult = allMultiplyResults.sortWith(
              (res1, res2) => res1._1.compareTo(res2._1) < 0
            )

            val lower = sortedResult.head._1
            val upper = sortedResult.last._1
            val lowerInclude = sortedResult.exists {
              case (value, include) => (value == lower) && include
            }
            val upperInclude = sortedResult.exists {
              case (value, include) => (value == upper) && include
            }

            FiniteValueInterval(
              lower,
              upper,
              lowerInclude,
              upperInclude)
          case _ => null
        }
      // TODO add more case
      case _ => null
    }

  }

  /**
    * Calculate the interval of column which is referred in predicate expression, and intersect the
    * result with the origin interval of the column.
    *
    * e.g for condition $1 <= 2 and $1 >= -1
    * the interval of $1 is originInterval intersect with [-1, 2]
    *
    * for condition: $1 <= 2 and not ($1 < -1 or $2 is true),
    * the interval of $1 is originInterval intersect with [-1, 2]
    *
    * for condition $1 <= 2 or $1 > -1
    * the interval of $1 is (originInterval intersect with (-Inf, 2]) union
    * (originInterval intersect with (-1, Inf])
    *
    * @param originInterval origin interval of the column
    * @param oriPred        the predicate expression
    * @param inputRef       the index of the given column
    * @param rexBuilder     RexBuilder instance to analyze the predicate expression
    * @return
    */
  def getColumnIntervalWithFilter(
      originInterval: Option[ValueInterval],
      oriPred: RexNode,
      inputRef: Int,
      rexBuilder: RexBuilder): ValueInterval = {
    val predicate = FlinkRexUtil.expandSearch(rexBuilder, oriPred)
    val isRelated = (r: RexNode) => r.accept(new ColumnRelatedVisitor(inputRef))
    val (relatedSubRexNode, _) = FlinkRelOptUtil.partition(predicate, rexBuilder, isRelated)
    val beginInterval = originInterval match {
      case Some(i) => i
      case _ => ValueInterval.infinite
    }
    val interval = relatedSubRexNode match {
      case Some(rexNode) =>
        if (rexNode.isAlwaysTrue) {
          beginInterval
        } else if (rexNode.isAlwaysFalse) {
          ValueInterval.empty
        } else if (RexUtil.isConstant(rexNode)) {
          // this should not happen, just protect the following code
          ValueInterval.infinite
        } else {
          val orParts = RexUtil.flattenOr(Vector(RexUtil.toDnf(rexBuilder, rexNode)))
          orParts.map(or => {
            val andParts = RexUtil.flattenAnd(Vector(or))
            val andIntervals = andParts.map(and => columnIntervalOfSinglePredicate(and))
            val res = andIntervals
              .filter(_ != null)
              .foldLeft(beginInterval)(ValueInterval.intersect)
            res
          }).reduceLeft(ValueInterval.union)
        }
      case _ => beginInterval
    }
    if (interval == ValueInterval.infinite) null else interval
  }

  private def columnIntervalOfSinglePredicate(condition: RexNode): ValueInterval = {
    val convertedCondition = condition.asInstanceOf[RexCall]
    if (convertedCondition == null || convertedCondition.operands.size() != 2) {
      null
    } else {
      val (literalValue, op) = (convertedCondition.operands.head, convertedCondition.operands.last)
      match {
        case (_: RexInputRef, literal: RexLiteral) =>
          (getLiteralValueByBroadType(literal), convertedCondition.getKind)
        case (rex: RexCall, literal: RexLiteral) if rex.getKind == SqlKind.AS =>
          (getLiteralValueByBroadType(literal), convertedCondition.getKind)
        case (literal: RexLiteral, _: RexInputRef) =>
          (getLiteralValueByBroadType(literal), convertedCondition.getKind.reverse())
        case (literal: RexLiteral, rex: RexCall) if rex.getKind == SqlKind.AS =>
          (getLiteralValueByBroadType(literal), convertedCondition.getKind.reverse())
        case _ => (null, null)
      }
      if (op == null || literalValue == null) {
        null
      } else {
        op match {
          case EQUALS => ValueInterval(literalValue, literalValue)
          case LESS_THAN => ValueInterval(null, literalValue, includeUpper = false)
          case LESS_THAN_OR_EQUAL => ValueInterval(null, literalValue)
          case GREATER_THAN => ValueInterval(literalValue, null, includeLower = false)
          case GREATER_THAN_OR_EQUAL => ValueInterval(literalValue, null)
          case _ => null
        }
      }
    }
  }

  /**
    * return ValueInterval with BigDecimal bound
    */
  def toBigDecimalInterval(value: ValueInterval): ValueInterval = {
    value match {
      case finite@ FiniteValueInterval(l, u, lb, ub) =>
        (convertNumberToString(l), convertNumberToString(u)) match {
          case (Some(lv), Some(uv)) =>
            FiniteValueInterval(new java.math.BigDecimal(lv), new java.math.BigDecimal(uv), lb, ub)
          case (Some(lv), None) =>
            FiniteValueInterval(new java.math.BigDecimal(lv), u, lb, ub)
          case (None, Some(uv)) =>
            FiniteValueInterval(l, new java.math.BigDecimal(uv), lb, ub)
          case _ => finite
        }
      case left@ LeftSemiInfiniteValueInterval(u, ub) =>
        convertNumberToString(u) match {
          case Some(uv) =>
            LeftSemiInfiniteValueInterval(new java.math.BigDecimal(uv), ub)
          case _ => left
        }
      case right@ RightSemiInfiniteValueInterval(l, lb) =>
        convertNumberToString(l) match {
          case Some(lv) =>
            RightSemiInfiniteValueInterval(new java.math.BigDecimal(lv), lb)
          case _ => right
        }
      case _ => value
    }
  }

  def convertNumberToString(number: Any): Option[String] = number match {
    // java number and scala BigInt, BigDecimal
    case jNum: java.lang.Number => Some(jNum.toString)
    case sByte: scala.Byte => Some(sByte.toString)
    case sShort: scala.Short => Some(sShort.toString)
    case sInt: scala.Int => Some(sInt.toString)
    case sLong: scala.Long => Some(sLong.toString)
    case sFloat: scala.Float => Some(sFloat.toString)
    case sDouble: scala.Double => Some(sDouble.toString)
    case _ => None
  }

}
