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
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.catalog.{CatalogManager, GenericInMemoryCatalog}
import org.apache.flink.table.functions.{AggregateFunction, TableFunction}

/**
  * The [[TableEnvironment]] for a Scala [[StreamExecutionEnvironment]].
  */
class BatchTableEnvironment(
    execEnv: StreamExecutionEnvironment,
    config: TableConfig,
    catalogManager: CatalogManager)
  extends org.apache.flink.table.api.BatchTableEnvironment(
    execEnv.getWrappedStreamExecutionEnvironment,
    config,
    catalogManager) {

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

object BatchTableEnvironment {

  /**
    * Returns a [[TableEnvironment]] for a Scala [[StreamExecutionEnvironment]].
    *
    * A TableEnvironment can be used to:
    * - register a [[Table]] in the [[TableEnvironment]]'s catalog
    * - scan a registered table to obtain a [[Table]]
    * - specify a SQL query on registered tables to obtain a [[Table]]
    * - explain the AST and execution plan of a [[Table]]
    *
    * @param executionEnvironment The Java [[StreamExecutionEnvironment]] of the TableEnvironment.
    */
  def create(executionEnvironment: StreamExecutionEnvironment): BatchTableEnvironment = {
    create(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[TableEnvironment]] for a Scala [[StreamExecutionEnvironment]].
    *
    * A TableEnvironment can be used to:
    * - register a [[Table]] in the [[TableEnvironment]]'s catalog
    * - scan a registered table to obtain a [[Table]]
    * - specify a SQL query on registered tables to obtain a [[Table]]
    * - explain the AST and execution plan of a [[Table]]
    *
    * @param executionEnvironment The Java [[StreamExecutionEnvironment]] of the TableEnvironment.
    * @param tableConfig The configuration of the TableEnvironment.
    */
  def create(
      executionEnvironment: StreamExecutionEnvironment,
      tableConfig: TableConfig): BatchTableEnvironment = {
    val catalogManager = new CatalogManager(
      tableConfig.getBuiltInCatalogName,
      new GenericInMemoryCatalog(
        tableConfig.getBuiltInCatalogName,
        tableConfig.getBuiltInDatabaseName)
    )
    create(executionEnvironment, tableConfig, catalogManager)
  }

  /**
    * Returns a [[TableEnvironment]] for a Scala [[StreamExecutionEnvironment]].
    *
    * A TableEnvironment can be used to:
    * - register a [[Table]] in the [[TableEnvironment]]'s catalog
    * - scan a registered table to obtain a [[Table]]
    * - specify a SQL query on registered tables to obtain a [[Table]]
    * - explain the AST and execution plan of a [[Table]]
    *
    * @param executionEnvironment The Java [[StreamExecutionEnvironment]] of the TableEnvironment.
    * @param tableConfig The configuration of the TableEnvironment.
    * @param catalogManager a catalog manager that encapsulates all available catalogs.
    */
  def create(
      executionEnvironment: StreamExecutionEnvironment,
      tableConfig: TableConfig,
      catalogManager: CatalogManager): BatchTableEnvironment = {
    new BatchTableEnvironment(executionEnvironment, tableConfig, catalogManager)
  }
}
