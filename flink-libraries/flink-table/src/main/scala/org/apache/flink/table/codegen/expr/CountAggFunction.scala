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

import org.apache.flink.table.api.functions.DeclarativeAggregateFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.expressions.{Expression, IsNull, Literal, UnresolvedAggBufferReference}

/**
  * built-in count aggregate function
  */
class CountAggFunction extends DeclarativeAggregateFunction {

  override def inputCount: Int = 1

  private lazy val count = UnresolvedAggBufferReference("count", DataTypes.LONG)

  override def aggBufferAttributes: Seq[UnresolvedAggBufferReference] = Seq(count)

  override def initialValuesExpressions: Seq[Expression] = Seq(
    /* count = */ Literal(0L)
  )

  override def accumulateExpressions: Seq[Expression] = Seq(
    /* count = */ IsNull(operands(0)) ? (count, count + 1)
  )

  override def retractExpressions: Seq[Expression] = Seq(
    /* count = */ IsNull(operands(0)) ? (count, count - 1)
  )

  override def mergeExpressions: Seq[Expression] = Seq(
    /* count = */ count.left + count.right
  )

  // If all input are nulls, count will be 0 and we will get result 0.
  override def getValueExpression: Expression = count

  override def getResultType: InternalType = DataTypes.LONG
}
