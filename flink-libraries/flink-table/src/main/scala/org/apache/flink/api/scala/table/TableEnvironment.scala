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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.api.table.{TableConfig, Table}

/**
 * Environment for working with the Table API.
 *
 * This can be used to convert a [[DataSet]] to a [[Table]] and back again. You
 * can also use the provided methods to create a [[Table]] directly from a data source.
 */
class TableEnvironment {

  private val config = new TableConfig()

  /**
   * Returns the table config to define the runtime behavior of the Table API.
   */
  def getConfig = config

  /**
   * Converts the [[DataSet]] to a [[Table]]. The field names can be specified like this:
   *
   * {{{
   *   val in: DataSet[(String, Int)] = ...
   *   val table = in.as('a, 'b)
   * }}}
   *
   * This results in a [[Table]] that has field `a` of type `String` and field `b`
   * of type `Int`.
   */
  def fromDataSet[T](set: DataSet[T], fields: Expression*): Table = {
    new ScalaBatchTranslator(config).createTable(set, fields.toArray)
  }

  /**
   * Transforms the given DataSet to a [[org.apache.flink.api.table.Table]].
   * The fields of the DataSet type are used to name the
   * [[org.apache.flink.api.table.Table]] fields.
   */
  def fromDataSet[T](set: DataSet[T]): Table = {
    new ScalaBatchTranslator(config).createTable(set)
  }

  /**
   * Converts the given [[org.apache.flink.api.table.Table]] to
   * a DataSet. The given type must have exactly the same fields as the
   * [[org.apache.flink.api.table.Table]]. That is, the names of the
   * fields and the types must match.
   */
  def toDataSet[T: TypeInformation](table: Table): DataSet[T] = {
     new ScalaBatchTranslator(config).translate[T](table.relNode)
  }

}

