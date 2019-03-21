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
package org.apache.flink.table.api.scala

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableConfig, TableEnvironment}

/**
  * The [[TableEnvironment]] for a Scala [[StreamExecutionEnvironment]] that works with
  * [[DataStream]]s.
  *
  * A TableEnvironment can be used to:
  * - convert a [[DataStream]] to a [[Table]]
  * - register a [[DataStream]] in the [[TableEnvironment]]'s catalog
  * - register a [[Table]] in the [[TableEnvironment]]'s catalog
  * - scan a registered table to obtain a [[Table]]
  * - specify a SQL query on registered tables to obtain a [[Table]]
  * - convert a [[Table]] into a [[DataStream]]
  * - explain the AST and execution plan of a [[Table]]
  *
  * @param execEnv The Scala [[StreamExecutionEnvironment]] of the TableEnvironment.
  * @param config The configuration of the TableEnvironment.
  */
class StreamTableEnvironment @deprecated(
      "This constructor will be removed. Use StreamTableEnvironment.create() instead.",
      "1.8.0") (
    execEnv: StreamExecutionEnvironment,
    config: TableConfig)
  extends org.apache.flink.table.api.StreamTableEnvironment(
    execEnv.getWrappedStreamExecutionEnvironment,
    config) {

  /**
    * Converts the given [[DataStream]] into a [[Table]].
    *
    * The field names of the [[Table]] are automatically derived from the type of the
    * [[DataStream]].
    *
    * @param dataStream The [[DataStream]] to be converted.
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  def fromDataStream[T](dataStream: DataStream[T]): Table = {

    val name = createUniqueTableName()
    registerDataStreamInternal(name, dataStream.javaStream)
    scan(name)
  }

  /**
    * Converts the given [[DataStream]] into a [[Table]] with specified field names.
    *
    * Example:
    *
    * {{{
    *   val stream: DataStream[(String, Long)] = ...
    *   val tab: Table = tableEnv.fromDataStream(stream, 'a, 'b)
    * }}}
    *
    * @param dataStream The [[DataStream]] to be converted.
    * @param fields The field names of the resulting [[Table]].
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  // TODO: Change fields type to `Expression*` after introducing [Expression]
  def fromDataStream[T](dataStream: DataStream[T], fields: Symbol*): Table = {
    val exprs = fields.map(_.name).toArray
    val name = createUniqueTableName()
    registerDataStreamInternal(name, dataStream.javaStream, exprs)
    scan(name)
  }

  /**
    * Registers the given [[DataStream]] as table in the
    * [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived
    * from the type of the [[DataStream]].
    *
    * @param name The name under which the [[DataStream]] is registered in the catalog.
    * @param dataStream The [[DataStream]] to register.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerDataStream[T](name: String, dataStream: DataStream[T]): Unit = {

    checkValidTableName(name)
    registerDataStreamInternal(name, dataStream.javaStream)
  }

  /**
    * Registers the given [[DataStream]] as table with specified field names in the
    * [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * Example:
    *
    * {{{
    *   val set: DataStream[(String, Long)] = ...
    *   tableEnv.registerDataStream("myTable", set, 'a, 'b)
    * }}}
    *
    * @param name The name under which the [[DataStream]] is registered in the catalog.
    * @param dataStream The [[DataStream]] to register.
    * @param fields The field names of the registered table.
    * @tparam T The type of the [[DataStream]] to register.
    */
  // TODO: Change fields type to `Expression*` after introducing [Expression]
  def registerDataStream[T](name: String, dataStream: DataStream[T], fields: Symbol*): Unit = {
    val exprs = fields.map(_.name).toArray
    checkValidTableName(name)
    registerDataStreamInternal(name, dataStream.javaStream, exprs)
  }


}

object StreamTableEnvironment {

  /**
    * The [[TableEnvironment]] for a Scala [[StreamExecutionEnvironment]] that works with
    * [[DataStream]]s.
    *
    * A TableEnvironment can be used to:
    * - convert a [[DataStream]] to a [[Table]]
    * - register a [[DataStream]] in the [[TableEnvironment]]'s catalog
    * - register a [[Table]] in the [[TableEnvironment]]'s catalog
    * - scan a registered table to obtain a [[Table]]
    * - specify a SQL query on registered tables to obtain a [[Table]]
    * - convert a [[Table]] into a [[DataStream]]
    * - explain the AST and execution plan of a [[Table]]
    *
    * @param executionEnvironment The Scala [[StreamExecutionEnvironment]] of the TableEnvironment.
    */
  def create(executionEnvironment: StreamExecutionEnvironment): StreamTableEnvironment = {
    new StreamTableEnvironment(executionEnvironment, new TableConfig())
  }

  /**
    * The [[TableEnvironment]] for a Scala [[StreamExecutionEnvironment]] that works with
    * [[DataStream]]s.
    *
    * A TableEnvironment can be used to:
    * - convert a [[DataStream]] to a [[Table]]
    * - register a [[DataStream]] in the [[TableEnvironment]]'s catalog
    * - register a [[Table]] in the [[TableEnvironment]]'s catalog
    * - scan a registered table to obtain a [[Table]]
    * - specify a SQL query on registered tables to obtain a [[Table]]
    * - convert a [[Table]] into a [[DataStream]]
    * - explain the AST and execution plan of a [[Table]]
    *
    * @param executionEnvironment The Scala [[StreamExecutionEnvironment]] of the TableEnvironment.
    * @param tableConfig The configuration of the TableEnvironment.
    */
  def create(
    executionEnvironment: StreamExecutionEnvironment,
    tableConfig: TableConfig): StreamTableEnvironment = {

    new StreamTableEnvironment(executionEnvironment, tableConfig)
  }
}
