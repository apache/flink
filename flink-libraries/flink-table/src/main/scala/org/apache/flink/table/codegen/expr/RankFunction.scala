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

import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.expressions.{Literal, _}

/**
  * built-in rank aggregate function
  */
class RankFunction(orderKeyType: Array[InternalType])
  extends RankLikeFunction(orderKeyType) {

  private lazy val currNum = UnresolvedAggBufferReference("currNum", DataTypes.LONG)

  override def aggBufferAttributes: Seq[UnresolvedAggBufferReference] =
    currNum +: sequence +: lastVal

  override def initialValuesExpressions: Seq[Expression] =
    Literal(0L) +: Literal(0L) +: orderKeyType.map(orderType => generateInitLiteral(orderType))

  override def accumulateExpressions: Seq[Expression] =
    (currNum + 1) +:
        If(And(orderEquals, Not(EqualTo(sequence, Literal(0)))), sequence, currNum) +:
        operands
}
