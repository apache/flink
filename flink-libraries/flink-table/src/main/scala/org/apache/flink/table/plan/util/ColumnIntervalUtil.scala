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

import org.apache.flink.table.plan.stats._

import org.apache.calcite.rex.RexCall
import org.apache.calcite.sql.fun.SqlStdOperatorTable

/**
  * Helper for FlinkRelMdColumnInterval.
  */
object ColumnIntervalUtil {

  /**
    * try get value interval if the operator is  +, -, *。
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

  def convertStringToNumber(number: String, clazz: Class[_]): Option[Comparable[_]] = {
    if (clazz == classOf[java.lang.Byte]) {
      Some(java.lang.Byte.valueOf(number))
    } else if (clazz == classOf[java.lang.Short]) {
      Some(java.lang.Short.valueOf(number))
    } else if (clazz == classOf[java.lang.Integer]) {
      Some(java.lang.Integer.valueOf(number))
    } else if (clazz == classOf[java.lang.Float]) {
      Some(java.lang.Float.valueOf(number))
    } else if (clazz == classOf[java.lang.Long]) {
      Some(java.lang.Long.valueOf(number))
    } else if (clazz == classOf[java.lang.Double]) {
      Some(java.lang.Double.valueOf(number))
    } else if (clazz == classOf[java.math.BigDecimal]) {
      Some(new java.math.BigDecimal(number))
    } else if (clazz == classOf[java.math.BigInteger]) {
      Some(new java.math.BigInteger(number))
    } else if (clazz == classOf[scala.Byte]) {
      Some(number.toByte)
    } else if (clazz == classOf[scala.Short]) {
      Some(number.toShort)
    } else if (clazz == classOf[scala.Int]) {
      Some(number.toInt)
    } else if (clazz == classOf[scala.Long]) {
      Some(number.toLong)
    } else if (clazz == classOf[scala.Float]) {
      Some(number.toFloat)
    } else if (clazz == classOf[scala.Double]) {
      Some(number.toDouble)
    } else {
      None
    }
  }

}
