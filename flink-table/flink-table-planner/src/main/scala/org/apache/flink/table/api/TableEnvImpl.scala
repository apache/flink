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

package org.apache.flink.table.api

import _root_.java.util.Optional
import _root_.java.util.concurrent.atomic.AtomicInteger

import org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema
import org.apache.calcite.plan._
import org.apache.calcite.sql._
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools._
import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.calcite._
import org.apache.flink.table.catalog._
import org.apache.flink.table.expressions._
import org.apache.flink.table.factories.{TableFactoryService, TableFactoryUtil, TableSinkFactory}
import org.apache.flink.table.functions._
import org.apache.flink.table.operations.{CatalogQueryOperation, OperationTreeBuilder, PlannerQueryOperation, TableSourceQueryOperation}
import org.apache.flink.table.planner.PlanningConfigurationBuilder
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.util.JavaScalaConversionUtil
import org.apache.flink.util.StringUtils

import _root_.scala.collection.JavaConverters._

/**
  * The abstract base class for the implementation of batch and stream TableEnvironments.
  *
  * @param config The configuration of the TableEnvironment
  */
abstract class TableEnvImpl(
    val config: TableConfig,
    private val catalogManager: CatalogManager)
  extends TableEnvironment {

  // Table API/SQL function catalog
  private[flink] val functionCatalog: FunctionCatalog = new FunctionCatalog()

  protected val defaultCatalogName: String = config.getBuiltInCatalogName
  protected val defaultDatabaseName: String = config.getBuiltInDatabaseName

  // temporary bridge between API and planner
  private[flink] val expressionBridge: ExpressionBridge[PlannerExpression] =
    new ExpressionBridge[PlannerExpression](functionCatalog, PlannerExpressionConverter.INSTANCE)

  // a counter for unique attribute names
  private[flink] val attrNameCntr: AtomicInteger = new AtomicInteger(0)

  private[flink] val operationTreeBuilder = new OperationTreeBuilder(this)

  protected val planningConfigurationBuilder: PlanningConfigurationBuilder =
    new PlanningConfigurationBuilder(
      config,
      functionCatalog,
      asRootSchema(new CatalogManagerCalciteSchema(catalogManager, isBatch)),
      expressionBridge)

  def getConfig: TableConfig = config

  private def isBatch: Boolean = this match {
    case _: BatchTableEnvImpl => true
    case _ => false
  }

  private[flink] def queryConfig: QueryConfig = this match {
    case _: BatchTableEnvImpl => new BatchQueryConfig
    case _: StreamTableEnvImpl => new StreamQueryConfig
    case _ => null
  }

  override def registerExternalCatalog(name: String, externalCatalog: ExternalCatalog): Unit = {
    catalogManager.registerExternalCatalog(name, externalCatalog)
  }

  override def getRegisteredExternalCatalog(name: String): ExternalCatalog = {
    JavaScalaConversionUtil.toScala(catalogManager.getExternalCatalog(name)) match {
      case Some(catalog) => catalog
      case None => throw new ExternalCatalogNotExistException(name)
    }
  }

  override def registerFunction(name: String, function: ScalarFunction): Unit = {
    functionCatalog.registerScalarFunction(
      name,
      function)
  }

  /**
    * Registers a [[TableFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  private[flink] def registerTableFunctionInternal[T: TypeInformation](
      name: String,
      function: TableFunction[T])
    : Unit = {
    val resultTypeInfo: TypeInformation[T] = UserFunctionsTypeHelper
      .getReturnTypeOfTableFunction(
        function,
        implicitly[TypeInformation[T]])

    functionCatalog.registerTableFunction(
      name,
      function,
      resultTypeInfo)
  }

  /**
    * Registers an [[AggregateFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  private[flink] def registerAggregateFunctionInternal[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: UserDefinedAggregateFunction[T, ACC])
    : Unit = {
    val resultTypeInfo: TypeInformation[T] = UserFunctionsTypeHelper
      .getReturnTypeOfAggregateFunction(
        function,
        implicitly[TypeInformation[T]])

    val accTypeInfo: TypeInformation[ACC] = UserFunctionsTypeHelper
      .getAccumulatorTypeOfAggregateFunction(
      function,
      implicitly[TypeInformation[ACC]])

    functionCatalog.registerAggregateFunction(
      name,
      function,
      resultTypeInfo,
      accTypeInfo)
  }

  override def registerCatalog(catalogName: String, catalog: Catalog): Unit = {
    catalogManager.registerCatalog(catalogName, catalog)
  }

  override def getCatalog(catalogName: String): Optional[Catalog] = {
    catalogManager.getCatalog(catalogName)
  }

  override def getCurrentCatalog: String = {
    catalogManager.getCurrentCatalog
  }

  override def getCurrentDatabase: String = {
    catalogManager.getCurrentDatabase
  }

  override def useCatalog(catalogName: String): Unit = {
    catalogManager.setCurrentCatalog(catalogName)
  }

  override def useDatabase(databaseName: String): Unit = {
    catalogManager.setCurrentDatabase(databaseName)
  }

  override def registerTable(name: String, table: Table): Unit = {

    // check that table belongs to this table environment
    if (table.asInstanceOf[TableImpl].tableEnv != this) {
      throw new TableException(
        "Only tables that belong to this TableEnvironment can be registered.")
    }

    val tableTable = new QueryOperationCatalogView(table.getQueryOperation)
    registerTableInternal(name, tableTable)
  }

  override def registerTableSource(name: String, tableSource: TableSource[_]): Unit = {
    validateTableSource(tableSource)
    registerTableSourceInternal(name, tableSource)
  }

  override def registerTableSink(
    name: String,
    fieldNames: Array[String],
    fieldTypes: Array[TypeInformation[_]],
    tableSink: TableSink[_]): Unit = {

    if (fieldNames == null) {
      throw new TableException("fieldNames must not be null.")
    }
    if (fieldTypes == null) {
      throw new TableException("fieldTypes must not be null.")
    }
    if (fieldNames.length == 0) {
      throw new TableException("fieldNames must not be empty.")
    }
    if (fieldNames.length != fieldTypes.length) {
      throw new TableException("Same number of field names and types required.")
    }

    val configuredSink = tableSink.configure(fieldNames, fieldTypes)
    registerTableSinkInternal(name, configuredSink)
  }

  override def registerTableSink(name: String, configuredSink: TableSink[_]): Unit = {
    // validate
    if (configuredSink.getTableSchema.getFieldNames.length == 0) {
      throw new TableException("Field names must not be empty.")
    }

    validateTableSink(configuredSink)
    registerTableSinkInternal(name, configuredSink)
  }

  override def fromTableSource(source: TableSource[_]): Table = {
    new TableImpl(this, new TableSourceQueryOperation(source, isBatch))
  }

  /**
    * Perform batch or streaming specific validations of the [[TableSource]].
    * This method should throw [[ValidationException]] if the [[TableSource]] cannot be used
    * in this [[TableEnvironment]].
    *
    * @param tableSource table source to validate
    */
  protected def validateTableSource(tableSource: TableSource[_]): Unit

  /**
    * Perform batch or streaming specific validations of the [[TableSink]].
    * This method should throw [[ValidationException]] if the [[TableSink]] cannot be used
    * in this [[TableEnvironment]].
    *
    * @param tableSink table source to validate
    */
  protected def validateTableSink(tableSink: TableSink[_]): Unit

  private def registerTableSourceInternal(
    name: String,
    tableSource: TableSource[_])
  : Unit = {
    // register
    getCatalogTable(defaultCatalogName, defaultDatabaseName, name) match {

      // check if a table (source or sink) is registered
      case Some(table: ConnectorCatalogTable[_, _]) =>
        if (table.getTableSource.isPresent) {
          // wrapper contains source
          throw new TableException(s"Table '$name' already exists. " +
            s"Please choose a different name.")
        } else {
          // wrapper contains only sink (not source)
          replaceTableInternal(
            name,
            ConnectorCatalogTable
              .sourceAndSink(tableSource, table.getTableSink.get, isBatch))
        }

      // no table is registered
      case _ =>
        registerTableInternal(name, ConnectorCatalogTable.source(tableSource, isBatch))
    }
  }

  private def registerTableSinkInternal(
    name: String,
    tableSink: TableSink[_])
  : Unit = {
    // check if a table (source or sink) is registered
    getCatalogTable(defaultCatalogName, defaultDatabaseName, name) match {

      // table source and/or sink is registered
      case Some(table: ConnectorCatalogTable[_, _]) =>
        if (table.getTableSink.isPresent) {
          // wrapper contains sink
          throw new TableException(s"Table '$name' already exists. " +
            s"Please choose a different name.")
        } else {
          // wrapper contains only source (not sink)
          replaceTableInternal(
            name,
            ConnectorCatalogTable
              .sourceAndSink(table.getTableSource.get, tableSink, isBatch))
        }

      // no table is registered
      case _ =>
        registerTableInternal(name, ConnectorCatalogTable.sink(tableSink, isBatch))
    }
  }

  private def checkValidTableName(name: String) = {
    if (StringUtils.isNullOrWhitespaceOnly(name)) {
      throw new ValidationException("A table name cannot be null or consist of only whitespaces.")
    }
  }

  protected def registerTableInternal(name: String, table: CatalogBaseTable): Unit = {
    checkValidTableName(name)
    val path = new ObjectPath(defaultDatabaseName, name)
    JavaScalaConversionUtil.toScala(catalogManager.getCatalog(defaultCatalogName)) match {
      case Some(catalog) =>
        catalog.createTable(
          path,
          table,
          false)
      case None => throw new TableException("The default catalog does not exist.")
    }
  }

  protected def replaceTableInternal(name: String, table: CatalogBaseTable): Unit = {
    checkValidTableName(name)
    val path = new ObjectPath(defaultDatabaseName, name)
    JavaScalaConversionUtil.toScala(catalogManager.getCatalog(defaultCatalogName)) match {
      case Some(catalog) =>
        catalog.alterTable(
          path,
          table,
          false)
      case None => throw new TableException("The default catalog does not exist.")
    }
  }

  @throws[TableException]
  override def scan(tablePath: String*): Table = {
    scanInternal(tablePath.toArray) match {
      case Some(table) => new TableImpl(this, table)
      case None => throw new TableException(s"Table '${tablePath.mkString(".")}' was not found.")
    }
  }

  private[flink] def scanInternal(tablePath: Array[String]): Option[CatalogQueryOperation] = {
    JavaScalaConversionUtil.toScala(catalogManager.resolveTable(tablePath: _*))
      .map(t => new CatalogQueryOperation(t.getTablePath, t.getTableSchema))
  }

  override def listTables(): Array[String] = {
    val currentCatalogName = catalogManager.getCurrentCatalog
    val currentCatalog = catalogManager.getCatalog(currentCatalogName)
    JavaScalaConversionUtil.toScala(currentCatalog) match {
      case Some(catalog) => catalog.listTables(catalogManager.getCurrentDatabase).asScala.toArray
      case None =>
        throw new TableException(s"The current catalog ($currentCatalogName) does not exist.")
    }
  }

  override def listUserDefinedFunctions(): Array[String] = {
    functionCatalog.getUserDefinedFunctions.toArray
  }

  override def explain(table: Table): String

  override def getCompletionHints(statement: String, position: Int): Array[String] = {
    val planner = getFlinkPlanner
    planner.getCompletionHints(statement, position)
  }

  override def sqlQuery(query: String): Table = {
    val planner = getFlinkPlanner
    // parse the sql query
    val parsed = planner.parse(query)
    if (null != parsed && parsed.getKind.belongsTo(SqlKind.QUERY)) {
      // validate the sql query
      val validated = planner.validate(parsed)
      // transform to a relational tree
      val relational = planner.rel(validated)
      new TableImpl(this, new PlannerQueryOperation(relational.rel))
    } else {
      throw new TableException(
        "Unsupported SQL query! sqlQuery() only accepts SQL queries of type " +
          "SELECT, UNION, INTERSECT, EXCEPT, VALUES, and ORDER_BY.")
    }
  }

  override def sqlUpdate(stmt: String): Unit = {
    sqlUpdate(stmt, this.queryConfig)
  }

  override def sqlUpdate(stmt: String, config: QueryConfig): Unit = {
    val planner = getFlinkPlanner
    // parse the sql query
    val parsed = planner.parse(stmt)
    parsed match {
      case insert: SqlInsert =>
        // validate the SQL query
        val query = insert.getSource
        val validatedQuery = planner.validate(query)

        // get query result as Table
        val queryResult = new TableImpl(this,
          new PlannerQueryOperation(planner.rel(validatedQuery).rel))

        // get name of sink table
        val targetTablePath = insert.getTargetTable.asInstanceOf[SqlIdentifier].names

        // insert query result into sink table
        insertInto(queryResult, config, targetTablePath.asScala:_*)
      case _ =>
        throw new TableException(
          "Unsupported SQL query! sqlUpdate() only accepts SQL statements of type INSERT.")
    }
  }

  /**
    * Writes a [[Table]] to a [[TableSink]].
    *
    * @param table The [[Table]] to write.
    * @param sink The [[TableSink]] to write the [[Table]] to.
    * @param conf The [[QueryConfig]] to use.
    * @tparam T The data type that the [[TableSink]] expects.
    */
  private[flink] def writeToSink[T](table: Table, sink: TableSink[T], conf: QueryConfig): Unit

  override def insertInto(
      table: Table,
      queryConfig: QueryConfig,
      sinkPath: String,
      sinkPathContinued: String*)
    : Unit = insertInto(table, queryConfig, sinkPath +: sinkPathContinued: _*)

  override def insertInto(
      table: Table,
      sinkPath: String,
      sinkPathContinued: String*)
    : Unit = insertInto(table, queryConfig, sinkPath +: sinkPathContinued: _*)

  /**
    * Writes the [[Table]] to a [[TableSink]] that was registered under the specified name.
    *
    * @param table The table to write to the TableSink.
    * @param sinkTablePath The name of the registered TableSink.
    * @param conf The query configuration to use.
    */
  private def insertInto(table: Table, conf: QueryConfig, sinkTablePath: String*): Unit = {

    // check that sink table exists
    if (null == sinkTablePath) {
      throw new TableException("Name of TableSink must not be null.")
    }
    if (sinkTablePath.isEmpty) {
      throw new TableException("Name of TableSink must not be empty.")
    }

    getTableSink(sinkTablePath: _*) match {

      case None =>
        throw new TableException(s"No table was registered under the name $sinkTablePath.")

      case Some(tableSink) =>
        // validate schema of source table and table sink
        val srcFieldTypes = table.getSchema.getFieldTypes
        val sinkFieldTypes = tableSink.getTableSchema.getFieldTypes

        if (srcFieldTypes.length != sinkFieldTypes.length ||
          srcFieldTypes.zip(sinkFieldTypes).exists { case (srcF, snkF) => srcF != snkF }) {

          val srcFieldNames = table.getSchema.getFieldNames
          val sinkFieldNames = tableSink.getTableSchema.getFieldNames

          // format table and table sink schema strings
          val srcSchema = srcFieldNames.zip(srcFieldTypes)
            .map { case (n, t) => s"$n: ${t.getTypeClass.getSimpleName}" }
            .mkString("[", ", ", "]")
          val sinkSchema = sinkFieldNames.zip(sinkFieldTypes)
            .map { case (n, t) => s"$n: ${t.getTypeClass.getSimpleName}" }
            .mkString("[", ", ", "]")

          throw new ValidationException(
            s"Field types of query result and registered TableSink " +
              s"$sinkTablePath do not match.\n" +
              s"Query result schema: $srcSchema\n" +
              s"TableSink schema:    $sinkSchema")
        }
        // emit the table to the configured table sink
        writeToSink(table, tableSink, conf)
    }
  }

  private def getTableSink(name: String*): Option[TableSink[_]] = {
    JavaScalaConversionUtil.toScala(catalogManager.resolveTable(name: _*)) match {
      case Some(s) if s.getExternalCatalogTable.isPresent =>

        Option(TableFactoryUtil.findAndCreateTableSink(s.getExternalCatalogTable.get()))

      case Some(s) if JavaScalaConversionUtil.toScala(s.getCatalogTable)
        .exists(_.isInstanceOf[ConnectorCatalogTable[_, _]]) =>

        JavaScalaConversionUtil
          .toScala(s.getCatalogTable.get().asInstanceOf[ConnectorCatalogTable[_, _]].getTableSink)

      case Some(s) if JavaScalaConversionUtil.toScala(s.getCatalogTable)
        .exists(_.isInstanceOf[CatalogTable]) =>

        val sinkProperties = s.getCatalogTable.get().asInstanceOf[CatalogTable].toProperties
        Option(TableFactoryService.find(classOf[TableSinkFactory[_]], sinkProperties)
          .createTableSink(sinkProperties))

      case _ => None
    }
  }

  protected def getCatalogTable(name: String*): Option[CatalogBaseTable] = {
    JavaScalaConversionUtil.toScala(catalogManager.resolveTable(name: _*))
      .flatMap(t => JavaScalaConversionUtil.toScala(t.getCatalogTable))
  }

  /** Returns the [[FlinkRelBuilder]] of this TableEnvironment. */
  private[flink] def getRelBuilder: FlinkRelBuilder = {
    val currentCatalogName = catalogManager.getCurrentCatalog
    val currentDatabase = catalogManager.getCurrentDatabase

    planningConfigurationBuilder.createRelBuilder(currentCatalogName, currentDatabase)
  }

  /** Returns the Calcite [[org.apache.calcite.plan.RelOptPlanner]] of this TableEnvironment. */
  private def getPlanner: RelOptPlanner = {
    planningConfigurationBuilder.getPlanner
  }

  private[flink] def getFunctionCatalog: FunctionCatalog = {
    functionCatalog
  }

  private[flink] def getParserConfig: SqlParser.Config = planningConfigurationBuilder
    .getSqlParserConfig

  /** Returns the Calcite [[FrameworkConfig]] of this TableEnvironment. */
  @VisibleForTesting
  private[flink] def getFlinkPlanner: FlinkPlannerImpl = {
    val currentCatalogName = catalogManager.getCurrentCatalog
    val currentDatabase = catalogManager.getCurrentDatabase

    planningConfigurationBuilder.createFlinkPlanner(currentCatalogName, currentDatabase)
  }
}
