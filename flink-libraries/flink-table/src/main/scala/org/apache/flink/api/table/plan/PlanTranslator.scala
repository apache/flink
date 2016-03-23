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
package org.apache.flink.api.table.plan

import org.apache.calcite.rel.RelNode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.expressions.{ExpressionParser, Expression}
import org.apache.flink.api.table.Table

import scala.language.reflectiveCalls

/**
 * Base class for translators that transform the logical plan in a [[Table]] to an executable
 * Flink plan and also for creating a [[Table]] from a DataSet.
 */
abstract class PlanTranslator {

  type Representation[A] <: { def getType(): TypeInformation[A] }

  /**
   * Translates the given Table API back to the underlying representation, i.e, a DataSet.
   */
  def translate[A](op: RelNode)(implicit tpe: TypeInformation[A]): Representation[A]

  /**
   * Creates a [[Table]] from a DataSet (the underlying representation).
   */
  def createTable[A](
    repr: Representation[A],
    fieldIndexes: Array[Int],
    fieldNames: Array[String]): Table

  /**
   * Creates a [[Table]] from the given DataSet.
   */
  def createTable[A](repr: Representation[A]): Table = {

    val (fieldNames, fieldIndexes) = TranslationContext.getFieldInfo(repr.getType())
    createTable(repr, fieldIndexes, fieldNames)
  }

  /**
   * Creates a [[Table]] from the given DataSet while only taking those
   * fields mentioned in the field expressions.
   */
  def createTable[A](repr: Representation[A], expression: String): Table = {

    val exprs = ExpressionParser
      .parseExpressionList(expression)
      .toArray

    createTable(repr, exprs)
  }

  /**
    * Creates a [[Table]] from the given DataSet while only taking those
    * fields mentioned in the field expressions.
    */
  def createTable[A](repr: Representation[A], exprs: Array[Expression]): Table = {

    val (fieldNames, fieldIndexes) = TranslationContext.getFieldInfo(repr.getType(), exprs)
    createTable(repr, fieldIndexes.toArray, fieldNames.toArray)
  }

}
