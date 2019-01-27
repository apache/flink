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
import org.apache.flink.table.expressions._

/**
  * built-in row_number aggregate function
  */
class RowNumberFunction extends DeclarativeAggregateFunction {

  override def inputCount: Int = 1

  private lazy val sequence = UnresolvedAggBufferReference("seq", DataTypes.LONG)

  override def aggBufferAttributes: Seq[UnresolvedAggBufferReference] = Seq(sequence)

  override def initialValuesExpressions: Seq[Expression] = Seq(
    Literal(0L)
  )

  override def accumulateExpressions: Seq[Expression] = Seq(
    sequence + 1
  )

  override def mergeExpressions: Seq[Expression] = Seq()

  override def getValueExpression: Expression = sequence

  override def getResultType: InternalType = DataTypes.LONG
}
