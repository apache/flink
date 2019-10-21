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

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.{TableEnvironment, _}
import org.apache.flink.table.catalog.{CatalogManager, GenericInMemoryCatalog}
import org.apache.flink.table.descriptors.{BatchTableDescriptor, ConnectorDescriptor}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{AggregateFunction, TableFunction}
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.sinks.TableSink

/**
  * The [[TableEnvironment]] for a Scala batch [[ExecutionEnvironment]] that works
  * with [[DataSet]]s.
  *
  * A TableEnvironment can be used to:
  * - convert a [[DataSet]] to a [[Table]]
  * - register a [[DataSet]] in the [[TableEnvironment]]'s catalog
  * - register a [[Table]] in the [[TableEnvironment]]'s catalog
  * - scan a registered table to obtain a [[Table]]
  * - specify a SQL query on registered tables to obtain a [[Table]]
  * - convert a [[Table]] into a [[DataSet]]
  * - explain the AST and execution plan of a [[Table]]
  */
trait BatchTableEnvironment extends TableEnvironment {

  /**
    * Registers a [[TableFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param tf The TableFunction to register.
    * @tparam T The type of the output row.
    */
  def registerFunction[T: TypeInformation](name: String, tf: TableFunction[T]): Unit

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
    name: String,
    f: AggregateFunction[T, ACC]): Unit

  /**
    * Converts the given [[DataSet]] into a [[Table]].
    *
    * The field names of the [[Table]] are automatically derived from the type of the [[DataSet]].
    *
    * @param dataSet The [[DataSet]] to be converted.
    * @tparam T The type of the [[DataSet]].
    * @return The converted [[Table]].
    */
  def fromDataSet[T](dataSet: DataSet[T]): Table

  /**
    * Converts the given [[DataSet]] into a [[Table]] with specified field names.
    *
    * Example:
    *
    * {{{
    *   val set: DataSet[(String, Long)] = ...
    *   val tab: Table = tableEnv.fromDataSet(set, 'a, 'b)
    * }}}
    *
    * @param dataSet The [[DataSet]] to be converted.
    * @param fields The field names of the resulting [[Table]].
    * @tparam T The type of the [[DataSet]].
    * @return The converted [[Table]].
    */
  def fromDataSet[T](dataSet: DataSet[T], fields: Expression*): Table

  /**
    * Creates a view from the given [[DataSet]].
    * Registered views can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived from the type of the [[DataSet]].
    *
    * The view is registered in the namespace of the current catalog and database. To register the
    * view in a different catalog use [[createTemporaryView]].
    *
    * Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
    * it will be inaccessible in the current session. To make the permanent object available again
    * you can drop the corresponding temporary object.
    *
    * @param name The name under which the [[DataSet]] is registered in the catalog.
    * @param dataSet The [[DataSet]] to register.
    * @tparam T The type of the [[DataSet]] to register.
    * @deprecated use [[createTemporaryView]]
    */
  @deprecated
  def registerDataSet[T](name: String, dataSet: DataSet[T]): Unit

  /**
    * Creates a view from the given [[DataSet]] in a given path.
    * Registered tables can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived
    * from the type of the [[DataSet]].
    *
    * Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
    * it will be inaccessible in the current session. To make the permanent object available again
    * you can drop the corresponding temporary object.
    *
    * @param path The path under which the [[DataSet]] is created.
    *             See also the [[TableEnvironment]] class description for the format of the path.
    * @param dataSet The [[DataSet]] out of which to create the view.
    * @tparam T The type of the [[DataSet]].
    */
  def createTemporaryView[T](path: String, dataSet: DataSet[T]): Unit

  /**
    * Creates a view from the given [[DataSet]] in a given path with specified field names.
    * Registered views can be referenced in SQL queries.
    *
    * Example:
    *
    * {{{
    *   val set: DataSet[(String, Long)] = ...
    *   tableEnv.registerDataSet("myTable", set, 'a, 'b)
    * }}}
    *
    * The view is registered in the namespace of the current catalog and database. To register the
    * view in a different catalog use [[createTemporaryView]].
    *
    * Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
    * it will be inaccessible in the current session. To make the permanent object available again
    * you can drop the corresponding temporary object.
    *
    * @param name The name under which the [[DataSet]] is registered in the catalog.
    * @param dataSet The [[DataSet]] to register.
    * @param fields The field names of the registered table.
    * @tparam T The type of the [[DataSet]] to register.
    * @deprecated use [[createTemporaryView]]
    */
  @deprecated
  def registerDataSet[T](name: String, dataSet: DataSet[T], fields: Expression*): Unit

  /**
    * Creates a view from the given [[DataSet]] in a given path with specified field names.
    * Registered views can be referenced in SQL queries.
    *
    * Example:
    *
    * {{{
    *   val set: DataSet[(String, Long)] = ...
    *   tableEnv.createTemporaryView("cat.db.myTable", set, 'a, 'b)
    * }}}
    *
    * Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
    * it will be inaccessible in the current session. To make the permanent object available again
    * you can drop the corresponding temporary object.
    *
    * @param path The path under which the [[DataSet]] is created.
    *             See also the [[TableEnvironment]] class description for the format of the
    *             path.
    * @param dataSet The [[DataSet]] out of which to create the view.
    * @param fields The field names of the created view.
    * @tparam T The type of the [[DataSet]].
    */
  def createTemporaryView[T](path: String, dataSet: DataSet[T], fields: Expression*): Unit

  /**
    * Converts the given [[Table]] into a [[DataSet]] of a specified type.
    *
    * The fields of the [[Table]] are mapped to [[DataSet]] fields as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataSet]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @tparam T The type of the resulting [[DataSet]].
    * @return The converted [[DataSet]].
    */
  def toDataSet[T: TypeInformation](table: Table): DataSet[T]

  /**
    * Converts the given [[Table]] into a [[DataSet]] of a specified type.
    *
    * The fields of the [[Table]] are mapped to [[DataSet]] fields as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataSet]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @param queryConfig The configuration of the query to generate.
    * @tparam T The type of the resulting [[DataSet]].
    * @return The converted [[DataSet]].
    */
  def toDataSet[T: TypeInformation](
    table: Table,
    queryConfig: BatchQueryConfig): DataSet[T]

  /**
    * Evaluates a SQL statement such as INSERT, UPDATE or DELETE; or a DDL statement;
    * NOTE: Currently only SQL INSERT statements are supported.
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[Table]] is automatically registered when its [[Table#toString()]] method is
    * called, for example when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[Table]] as follows:
    *
    * {{{
    *   // register the configured table sink into which the result is inserted.
    *   tEnv.registerTableSink("sinkTable", configuredSink);
    *   Table sourceTable = ...
    *   String tableName = sourceTable.toString();
    *   // sourceTable is not registered to the table environment
    *   tEnv.sqlUpdate(s"INSERT INTO sinkTable SELECT * FROM tableName", config);
    * }}}
    *
    * @param stmt   The SQL statement to evaluate.
    * @param config The [[BatchQueryConfig]] to use.
    */
  def sqlUpdate(stmt: String, config: BatchQueryConfig): Unit

  /**
    * Writes the [[Table]] to a [[TableSink]] that was registered under the specified name.
    *
    * See the documentation of TableEnvironment#useDatabase or
    * TableEnvironment.useCatalog(String) for the rules on the path resolution.
    *
    * @param table             The Table to write to the sink.
    * @param queryConfig       The [[BatchQueryConfig]] to use.
    * @param sinkPath          The first part of the path of the registered [[TableSink]] to
    *                          which the [[Table]] is written. This is to ensure at least the
    *                          name of the [[TableSink]] is provided.
    * @param sinkPathContinued The remaining part of the path of the registered [[TableSink]] to
    *                          which the [[Table]] is written.
    * @deprecated use `TableEnvironment#insertInto(String, Table)`
    */
  @deprecated
  def insertInto(
    table: Table,
    queryConfig: BatchQueryConfig,
    sinkPath: String,
    sinkPathContinued: String*): Unit

  /**
    * Triggers the program execution. The environment will execute all parts of
    * the program.
    *
    * The program execution will be logged and displayed with the provided name
    *
    * It calls the ExecutionEnvironment#execute on the underlying
    * [[ExecutionEnvironment]]. In contrast to the [[TableEnvironment]] this
    * environment translates queries eagerly.
    *
    * @param jobName Desired name of the job
    * @return The result of the job execution, containing elapsed time and accumulators.
    * @throws Exception which occurs during job execution.
    */
  @throws[Exception]
  override def execute(jobName: String): JobExecutionResult

  /**
    * Creates a table source and/or table sink from a descriptor.
    *
    * Descriptors allow for declaring the communication to external systems in an
    * implementation-agnostic way. The classpath is scanned for suitable table factories that match
    * the desired configuration.
    *
    * The following example shows how to read from a connector using a JSON format and
    * registering a table source as "MyTable":
    *
    * {{{
    *
    * tableEnv
    *   .connect(
    *     new ExternalSystemXYZ()
    *       .version("0.11"))
    *   .withFormat(
    *     new Json()
    *       .jsonSchema("{...}")
    *       .failOnMissingField(false))
    *   .withSchema(
    *     new Schema()
    *       .field("user-name", "VARCHAR").from("u_name")
    *       .field("count", "DECIMAL")
    *   .registerSource("MyTable")
    * }}}
    *
    * @param connectorDescriptor connector descriptor describing the external system
    */
  override def connect(connectorDescriptor: ConnectorDescriptor): BatchTableDescriptor
}

object BatchTableEnvironment {

  /**
    * The [[TableEnvironment]] for a Scala batch [[ExecutionEnvironment]] that works
    * with [[DataSet]]s.
    *
    * A TableEnvironment can be used to:
    * - convert a [[DataSet]] to a [[Table]]
    * - register a [[DataSet]] in the [[TableEnvironment]]'s catalog
    * - register a [[Table]] in the [[TableEnvironment]]'s catalog
    * - scan a registered table to obtain a [[Table]]
    * - specify a SQL query on registered tables to obtain a [[Table]]
    * - convert a [[Table]] into a [[DataSet]]
    * - explain the AST and execution plan of a [[Table]]
    *
    * @param executionEnvironment The Scala batch [[ExecutionEnvironment]] of the TableEnvironment.
    */
  def create(executionEnvironment: ExecutionEnvironment): BatchTableEnvironment = {
    create(executionEnvironment, new TableConfig)
  }

  /**
    * The [[TableEnvironment]] for a Scala batch [[ExecutionEnvironment]] that works
    * with [[DataSet]]s.
    *
    * A TableEnvironment can be used to:
    * - convert a [[DataSet]] to a [[Table]]
    * - register a [[DataSet]] in the [[TableEnvironment]]'s catalog
    * - register a [[Table]] in the [[TableEnvironment]]'s catalog
    * - scan a registered table to obtain a [[Table]]
    * - specify a SQL query on registered tables to obtain a [[Table]]
    * - convert a [[Table]] into a [[DataSet]]
    * - explain the AST and execution plan of a [[Table]]
    *
    * @param executionEnvironment The Scala batch [[ExecutionEnvironment]] of the TableEnvironment.
    * @param tableConfig The configuration of the TableEnvironment.
    */
  def create(executionEnvironment: ExecutionEnvironment, tableConfig: TableConfig)
  : BatchTableEnvironment = {
    try {
      val clazz = Class
        .forName("org.apache.flink.table.api.scala.internal.BatchTableEnvironmentImpl")
      val const = clazz
        .getConstructor(
          classOf[ExecutionEnvironment],
          classOf[TableConfig],
          classOf[CatalogManager],
          classOf[ModuleManager])
      val builtInCatalog = "default_catalog"
      val catalogManager = new CatalogManager(
        "default_catalog",
        new GenericInMemoryCatalog(
          builtInCatalog,
          "default_database")
      )
      val moduleManager = new ModuleManager
      const.newInstance(executionEnvironment, tableConfig, catalogManager, moduleManager)
        .asInstanceOf[BatchTableEnvironment]
    } catch {
      case t: Throwable => throw new TableException("Create BatchTableEnvironment failed.", t)
    }
  }
}
