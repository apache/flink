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

package org.apache.flink.api.table.expressions

import java.math.BigDecimal

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.table.typeutils.IntervalTypeInfo

object ExpressionUtils {

  private[flink] def toMonthInterval(expr: Expression, multiplier: Int): Expression = expr match {
    case Literal(value: Int, BasicTypeInfo.INT_TYPE_INFO) =>
      Literal(value * multiplier, IntervalTypeInfo.INTERVAL_MONTHS)
    case _ =>
      Cast(Mul(expr, Literal(multiplier)), IntervalTypeInfo.INTERVAL_MONTHS)
  }

  private[flink] def toMilliInterval(expr: Expression, multiplier: Long): Expression = expr match {
    case Literal(value: Int, BasicTypeInfo.INT_TYPE_INFO) =>
      Literal(value * multiplier, IntervalTypeInfo.INTERVAL_MILLIS)
    case _ =>
      Cast(Mul(expr, Literal(multiplier)), IntervalTypeInfo.INTERVAL_MILLIS)
  }

  // ----------------------------------------------------------------------------------------------
  // RexNode conversion functions (see org.apache.calcite.sql2rel.StandardConvertletTable)
  // ----------------------------------------------------------------------------------------------

  /**
    * Copy of [[org.apache.calcite.sql2rel.StandardConvertletTable#getFactor()]].
    */
  private[flink] def getFactor(unit: TimeUnit): BigDecimal = unit match {
    case TimeUnit.DAY => java.math.BigDecimal.ONE
    case TimeUnit.HOUR => TimeUnit.DAY.multiplier
    case TimeUnit.MINUTE => TimeUnit.HOUR.multiplier
    case TimeUnit.SECOND => TimeUnit.MINUTE.multiplier
    case TimeUnit.YEAR => java.math.BigDecimal.ONE
    case TimeUnit.MONTH => TimeUnit.YEAR.multiplier
    case _ => throw new IllegalArgumentException("Invalid start unit.")
  }

  /**
    * Copy of [[org.apache.calcite.sql2rel.StandardConvertletTable#mod()]].
    */
  private[flink] def mod(
      rexBuilder: RexBuilder,
      resType: RelDataType,
      res: RexNode,
      value: BigDecimal)
    : RexNode = {
    if (value == BigDecimal.ONE) return res
    rexBuilder.makeCall(SqlStdOperatorTable.MOD, res, rexBuilder.makeExactLiteral(value, resType))
  }

  /**
    * Copy of [[org.apache.calcite.sql2rel.StandardConvertletTable#divide()]].
    */
  private[flink] def divide(rexBuilder: RexBuilder, res: RexNode, value: BigDecimal): RexNode = {
    if (value == BigDecimal.ONE) return res
    if (value.compareTo(BigDecimal.ONE) < 0 && value.signum == 1) {
      try {
        val reciprocal = BigDecimal.ONE.divide(value, BigDecimal.ROUND_UNNECESSARY)
        return rexBuilder.makeCall(
          SqlStdOperatorTable.MULTIPLY,
          res,
          rexBuilder.makeExactLiteral(reciprocal))
      } catch {
        case e: ArithmeticException => // ignore
      }
    }
    rexBuilder.makeCall(
      SqlStdOperatorTable.DIVIDE_INTEGER,
      res,
      rexBuilder.makeExactLiteral(value))
  }

}
