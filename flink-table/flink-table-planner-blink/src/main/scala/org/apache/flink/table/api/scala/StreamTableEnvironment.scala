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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, asScalaStream}
import org.apache.flink.table.api.{Table, TableConfig, TableEnvironment}
import org.apache.flink.table.catalog.{CatalogManager, GenericInMemoryCatalog}
import org.apache.flink.table.functions.{AggregateFunction, TableFunction}

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
    config: TableConfig,
    catalogManager: CatalogManager)
  extends org.apache.flink.table.api.StreamTableEnvironment(
    execEnv.getWrappedStreamExecutionEnvironment,
    config,
    catalogManager) {

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
    createTable(asQueryOperation(dataStream.javaStream, None))
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
    createTable(asQueryOperation(dataStream.javaStream, Some(exprs)))
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
    registerTable(name, fromDataStream(dataStream))
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
    registerTable(name, fromDataStream(dataStream, fields: _*))
  }

  /**
    * Converts the given [[Table]] into an append [[DataStream]] of a specified type.
    *
    * The [[Table]] must only have insert (append) changes. If the [[Table]] is also modified
    * by update or delete changes, the conversion will fail.
    *
    * The fields of the [[Table]] are mapped to [[DataStream]] fields as follows:
    * - [[org.apache.flink.types.Row]] and Scala Tuple types: Fields are mapped by position, field
    * types must match.
    * - POJO [[DataStream]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @tparam T The type of the resulting [[DataStream]].
    * @return The converted [[DataStream]].
    */
  def toAppendStream[T: TypeInformation](table: Table): DataStream[T] = {
    val returnType = createTypeInformation[T]
    asScalaStream(translateToDataStream[T](
      table,
      updatesAsRetraction = false,
      withChangeFlag = false,
      returnType))
  }

  /**
    * Converts the given [[Table]] into a [[DataStream]] of add and retract messages.
    * The message will be encoded as [[Tuple2]]. The first field is a [[Boolean]] flag,
    * the second field holds the record of the specified type [[T]].
    *
    * A true [[Boolean]] flag indicates an add message, a false flag indicates a retract message.
    *
    * @param table The [[Table]] to convert.
    * @tparam T The type of the requested data type.
    * @return The converted [[DataStream]].
    */
  def toRetractStream[T: TypeInformation](table: Table): DataStream[(Boolean, T)] = {
    val returnType = createTypeInformation[(Boolean, T)]
    asScalaStream(translateToDataStream[(Boolean, T)](
      table,
      updatesAsRetraction = true,
      withChangeFlag = true,
      returnType))
  }

  /**
    * Registers an [[AggregateFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param f The AggregateFunction to register.
    * @tparam T The type of the output value.
    * @tparam ACC The type of aggregate accumulator.
    */
  def registerFunction[T: TypeInformation, ACC: TypeInformation](
      name: String, f: AggregateFunction[T, ACC]): Unit = {
    registerAggregateFunctionInternal[T, ACC](name, f)
  }

  /**
    * Registers a [[TableFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param tf The TableFunction to register.
    * @tparam T The type of the output row.
    */
  def registerFunction[T: TypeInformation](name: String, tf: TableFunction[T]): Unit = {
    registerTableFunctionInternal[T](name, tf)
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
    create(executionEnvironment, new TableConfig())
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
    val catalogManager = new CatalogManager(
      tableConfig.getBuiltInCatalogName,
      new GenericInMemoryCatalog(
        tableConfig.getBuiltInCatalogName,
        tableConfig.getBuiltInDatabaseName)
    )
    create(executionEnvironment, tableConfig, catalogManager)
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
    * @param catalogManager a catalog manager that encapsulates all available catalogs.
    */
  def create(
      executionEnvironment: StreamExecutionEnvironment,
      tableConfig: TableConfig,
      catalogManager: CatalogManager): StreamTableEnvironment = {
    new StreamTableEnvironment(executionEnvironment, tableConfig, catalogManager)
  }
}
