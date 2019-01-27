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

package org.apache.flink.table.codegen.expr

import java.sql.{Date, Time, Timestamp}

import org.apache.flink.table.api.functions.DeclarativeAggregateFunction
import org.apache.flink.table.api.types._
import org.apache.flink.table.expressions._

/**
  * built-in rank like function. examples:rank,dense_rank and so on
  */
abstract class RankLikeFunction(orderKeyType: Array[InternalType])
  extends DeclarativeAggregateFunction {

  protected lazy val sequence = UnresolvedAggBufferReference("seq", DataTypes.LONG)
  protected lazy val lastVal: Array[UnresolvedAggBufferReference] =
    orderKeyType.zipWithIndex.map{
      case (expr, idx) => UnresolvedAggBufferReference("lastVal_" + idx.toString, expr)}

  override def inputCount: Int = orderKeyType.length

  override def mergeExpressions: Seq[Expression] = Seq()

  override def getValueExpression: Expression = sequence

  override def getResultType: InternalType = DataTypes.LONG

  protected val orderEquals: Expression = orderKeyType.zipWithIndex.map {
    case (_, idx) =>
      val expr1 = lastVal.apply(idx)
      val expr2 = operands(idx)
      If(IsNull(expr1),
        If(IsNull(expr2), Literal(true), Literal(false)),
        EqualTo(lastVal.apply(idx), operands(idx)))}.reduceOption(And).getOrElse(Literal(true))

  protected def generateInitLiteral(orderType: InternalType): Literal = {
    Literal(orderType match {
        case DataTypes.BOOLEAN => false
        case DataTypes.BYTE => 0.toByte
        case DataTypes.SHORT => 0.toShort
        case DataTypes.INT => 0
        case DataTypes.LONG => 0L
        case DataTypes.FLOAT => 0.toFloat
        case DataTypes.DOUBLE => 0.toDouble
        case _: DecimalType => java.math.BigDecimal.ZERO
        case DataTypes.STRING => ""
        case DataTypes.DATE => new Date(0)
        case DataTypes.TIME => new Time(0)
        case DataTypes.TIMESTAMP => new Timestamp(0)
      }, orderType)
  }
}
