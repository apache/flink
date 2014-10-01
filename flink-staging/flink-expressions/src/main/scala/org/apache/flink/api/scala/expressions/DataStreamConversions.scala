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

package org.apache.flink.api.scala.expressions

import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.expressions._
import org.apache.flink.api.expressions.tree.{Expression, UnresolvedFieldReference}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

class DataStreamConversions[T](set: DataStream[T], inputType: CompositeType[T]) {

  /**
   * Converts the [[DataStream]] to an [[ExpressionOperation]]. The field names of the resulting
   * expression operation can be specified like this:
   *
   * {{{
   *   val in: DataSet[(String, Int)] = ...
   *   val expr = in.as('a, 'b)
   * }}}
   *
   * This results in an expression operation that has field `a` of type `String` and field `b`
   * of type `Int`.
   */

  def as(fields: Expression*): ExpressionOperation[ScalaStreamingTranslator] = {
     new ScalaStreamingTranslator().createExpressionOperation(set, fields.toArray)
  }

  /**
   * Converts the [[DataStream]] to an [[ExpressionOperation]]. The field names of the resulting
   * expression operation will be taken from the field names of the input type:
   *
   * {{{
   *   val in: DataSet[(String, Int)] = ...
   *   val expr = in.toExpression
   * }}}
   *
   * This results in an expression operation that has field `_1` of type `String` and field `_2`
   * of type `Int`.
   */

  def toExpression: ExpressionOperation[ScalaStreamingTranslator] = {
    val resultFields = inputType.getFieldNames.map(UnresolvedFieldReference)
    as(resultFields: _*)
  }

}

