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

package org.apache.flink.api.scala.table

import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.table._
import org.apache.flink.api.table.expressions.{Expression, UnresolvedFieldReference}
import org.apache.flink.streaming.api.scala.DataStream

class DataStreamConversions[T](stream: DataStream[T], inputType: CompositeType[T]) {

  /**
   * Converts the [[DataStream]] to a [[Table]]. The field names can be specified like this:
   *
   * {{{
   *   val in: DataSet[(String, Int)] = ...
   *   val table = in.as('a, 'b)
   * }}}
   *
   * This results in a [[Table]] that has field `a` of type `String` and field `b`
   * of type `Int`.
   */

  def as(fields: Expression*): Table = {
     new ScalaStreamingTranslator().createTable(
       stream,
       fields.toArray,
       checkDeterministicFields = true)
  }

  /**
   * Converts the [[DataStream]] to a [[Table]]. The field names will be taken from the field
   * names of the input type.
   *
   * Example:
   *
   * {{{
   *   val in: DataSet[(String, Int)] = ...
   *   val table = in.toTable
   * }}}
   *
   * This results in a [[Table]] that has field `_1` of type `String` and field `_2`
   * of type `Int`.
   */

  def toTable: Table = {
    val resultFields = inputType.getFieldNames.map(UnresolvedFieldReference)
    as(resultFields: _*)
  }

}

