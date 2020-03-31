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

package org.apache.flink.table.api.internal

import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api._
import org.apache.flink.table.calcite.{CalciteParser, FlinkPlannerImpl, FlinkRelBuilder}
import org.apache.flink.table.catalog._
import org.apache.flink.table.catalog.exceptions.{TableNotExistException => _, _}
import org.apache.flink.table.delegation.Parser
import org.apache.flink.table.expressions._
import org.apache.flink.table.expressions.resolver.lookups.TableReferenceLookup
import org.apache.flink.table.factories.{TableFactoryUtil, TableSinkFactoryContextImpl}
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction, UserDefinedAggregateFunction, _}
import org.apache.flink.table.module.{Module, ModuleManager}
import org.apache.flink.table.operations.ddl._
import org.apache.flink.table.operations.utils.OperationTreeBuilder
import org.apache.flink.table.operations.{CatalogQueryOperation, TableSourceQueryOperation, _}
import org.apache.flink.table.planner.{ParserImpl, PlanningConfigurationBuilder}
import org.apache.flink.table.sinks.{OverwritableTableSink, PartitionableTableSink, TableSink, TableSinkUtils}
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.util.JavaScalaConversionUtil

import org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.FrameworkConfig

import _root_.java.util.function.{Function => JFunction, Supplier => JSupplier}
import _root_.java.util.{Optional, HashMap => JHashMap, Map => JMap}

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._
import _root_.scala.util.Try

/**
  * The abstract base class for the implementation of batch TableEnvironment.
  *
  * @param config The configuration of the TableEnvironment
  */
abstract class TableEnvImpl(
    val config: TableConfig,
    private val catalogManager: CatalogManager,
    private val moduleManager: ModuleManager)
  extends TableEnvironment {

  // Table API/SQL function catalog
  private[flink] val functionCatalog: FunctionCatalog =
    new FunctionCatalog(config, catalogManager, moduleManager)

  // temporary utility until we don't use planner expressions anymore
  functionCatalog.setPlannerTypeInferenceUtil(PlannerTypeInferenceUtilImpl.INSTANCE)

  // temporary bridge between API and planner
  private[flink] val expressionBridge: ExpressionBridge[PlannerExpression] =
    new ExpressionBridge[PlannerExpression](PlannerExpressionConverter.INSTANCE)

  private def tableLookup: TableReferenceLookup = {
    new TableReferenceLookup {
      override def lookupTable(name: String): Optional[TableReferenceExpression] = {
        JavaScalaConversionUtil
          .toJava(
            // The TableLookup is used during resolution of expressions and it actually might not
            // be an identifier of a table. It might be a reference to some other object such as
            // column, local reference etc. This method should return empty optional in such cases
            // to fallback for other identifiers resolution.
            Try({
              val unresolvedIdentifier = UnresolvedIdentifier.of(name)
              scanInternal(unresolvedIdentifier)
                .map(t => ApiExpressionUtils.tableRef(name, t))
            })
              .toOption
              .flatten)
      }
    }
  }

  private[flink] val operationTreeBuilder = OperationTreeBuilder.create(
    config,
    functionCatalog.asLookup(new JFunction[String, UnresolvedIdentifier] {
      override def apply(t: String): UnresolvedIdentifier = parser.parseIdentifier(t)
    }),
    catalogManager.getDataTypeFactory,
    tableLookup,
    isStreamingMode)

  protected val planningConfigurationBuilder: PlanningConfigurationBuilder =
    new PlanningConfigurationBuilder(
      config,
      functionCatalog,
      asRootSchema(new CatalogManagerCalciteSchema(catalogManager, config, isStreamingMode)),
      expressionBridge)

  private val parser: Parser = new ParserImpl(
    catalogManager,
    new JSupplier[FlinkPlannerImpl] {
      override def get(): FlinkPlannerImpl = getFlinkPlanner
    },
    // we do not cache the parser in order to use the most up to
    // date configuration. Users might change parser configuration in TableConfig in between
    // parsing statements
    new JSupplier[CalciteParser] {
      override def get(): CalciteParser = planningConfigurationBuilder.createCalciteParser()
    }
  )

  def getConfig: TableConfig = config

  private val UNSUPPORTED_QUERY_IN_SQL_UPDATE_MSG =
    "Unsupported SQL query! sqlUpdate() only accepts a single SQL statement of type " +
      "INSERT, CREATE TABLE, DROP TABLE, ALTER TABLE, USE CATALOG, USE [CATALOG.]DATABASE, " +
      "CREATE DATABASE, DROP DATABASE, ALTER DATABASE"

  private def isStreamingMode: Boolean = this match {
    case _: BatchTableEnvImpl => false
    case _ => true
  }

  private def isBatchTable: Boolean = !isStreamingMode

  override def registerFunction(name: String, function: ScalarFunction): Unit = {
    functionCatalog.registerTempSystemScalarFunction(
      name,
      function)
  }

  override def createTemporarySystemFunction(
      name: String,
      functionClass: Class[_ <: UserDefinedFunction])
    : Unit = {
    val functionInstance = UserDefinedFunctionHelper.instantiateFunction(functionClass)
    createTemporarySystemFunction(name, functionInstance)
  }

  override def createTemporarySystemFunction(
      name: String,
      functionInstance: UserDefinedFunction)
    : Unit = {
    functionCatalog.registerTemporarySystemFunction(name, functionInstance, false)
  }

  override def dropTemporarySystemFunction(name: String): Boolean = {
    functionCatalog.dropTemporarySystemFunction(name, true)
  }

  override def createFunction(
      path: String,
      functionClass: Class[_ <: UserDefinedFunction])
    : Unit = {
    createFunction(path, functionClass, ignoreIfExists = false)
  }

  override def createFunction(
      path: String,
      functionClass: Class[_ <: UserDefinedFunction],
      ignoreIfExists: Boolean)
    : Unit = {
    val unresolvedIdentifier = parser.parseIdentifier(path)
    functionCatalog.registerCatalogFunction(unresolvedIdentifier, functionClass, ignoreIfExists)
  }

  override def dropFunction(path: String): Boolean = {
    val unresolvedIdentifier = parser.parseIdentifier(path)
    functionCatalog.dropCatalogFunction(unresolvedIdentifier, true)
  }

  override def createTemporaryFunction(
      path: String,
      functionClass: Class[_ <: UserDefinedFunction])
    : Unit = {
    val functionInstance = UserDefinedFunctionHelper.instantiateFunction(functionClass)
    createTemporaryFunction(path, functionInstance)
  }

  override def createTemporaryFunction(
      path: String,
      functionInstance: UserDefinedFunction)
    : Unit = {
    val unresolvedIdentifier = parser.parseIdentifier(path)
    functionCatalog.registerTemporaryCatalogFunction(unresolvedIdentifier, functionInstance, false)
  }

  override def dropTemporaryFunction(path: String): Boolean = {
    val unresolvedIdentifier = parser.parseIdentifier(path)
    functionCatalog.dropTemporaryCatalogFunction(unresolvedIdentifier, true)
  }

  /**
    * Registers a [[TableFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  private[flink] def registerTableFunctionInternal[T: TypeInformation](
      name: String,
      function: TableFunction[T])
    : Unit = {
    val resultTypeInfo = UserDefinedFunctionHelper
      .getReturnTypeOfTableFunction(
        function,
        implicitly[TypeInformation[T]])

    functionCatalog.registerTempSystemTableFunction(
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
    val resultTypeInfo: TypeInformation[T] = UserDefinedFunctionHelper
      .getReturnTypeOfAggregateFunction(
        function,
        implicitly[TypeInformation[T]])

    val accTypeInfo: TypeInformation[ACC] = UserDefinedFunctionHelper
      .getAccumulatorTypeOfAggregateFunction(
      function,
      implicitly[TypeInformation[ACC]])

    functionCatalog.registerTempSystemAggregateFunction(
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

  override def loadModule(moduleName: String, module: Module): Unit = {
    moduleManager.loadModule(moduleName, module)
  }

  override def unloadModule(moduleName: String): Unit = {
    moduleManager.unloadModule(moduleName)
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
    createTemporaryView(UnresolvedIdentifier.of(name), table)
  }

  protected def parseIdentifier(identifier: String): UnresolvedIdentifier = {
    val parser = planningConfigurationBuilder.createCalciteParser()
    UnresolvedIdentifier.of(parser.parseIdentifier(identifier).names: _*)
  }

  override def createTemporaryView(path: String, view: Table): Unit = {
    val identifier = parseIdentifier(path)
    createTemporaryView(identifier, view)
  }

  private def createTemporaryView(identifier: UnresolvedIdentifier, view: Table): Unit = {
    // check that table belongs to this table environment
    if (view.asInstanceOf[TableImpl].getTableEnvironment != this) {
      throw new TableException(
        "Only table API objects that belong to this TableEnvironment can be registered.")
    }

    val objectIdentifier = catalogManager.qualifyIdentifier(identifier)

    catalogManager.createTemporaryTable(
      new QueryOperationCatalogView(view.getQueryOperation),
      objectIdentifier,
      false)
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
    createTable(new TableSourceQueryOperation(source, isBatchTable))
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
    val unresolvedIdentifier = UnresolvedIdentifier.of(name)
    val objectIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier)
    // check if a table (source or sink) is registered
    getTemporaryTable(objectIdentifier) match {

      // check if a table (source or sink) is registered
      case Some(table: ConnectorCatalogTable[_, _]) =>
        if (table.getTableSource.isPresent) {
          // wrapper contains source
          throw new TableException(s"Table '$name' already exists. " +
            s"Please choose a different name.")
        } else {
          // wrapper contains only sink (not source)
          val sourceAndSink = ConnectorCatalogTable.sourceAndSink(
            tableSource,
            table.getTableSink.get,
            isBatchTable)
          catalogManager.createTemporaryTable(
            sourceAndSink,
            objectIdentifier,
            true)
        }

      // no table is registered
      case _ =>
        val source = ConnectorCatalogTable.source(tableSource, isBatchTable)
        catalogManager.createTemporaryTable(source, objectIdentifier, false)
    }
  }

  private def registerTableSinkInternal(
      name: String,
      tableSink: TableSink[_])
    : Unit = {
    val unresolvedIdentifier = UnresolvedIdentifier.of(name)
    val objectIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier)
    // check if a table (source or sink) is registered
    getTemporaryTable(objectIdentifier) match {

      // table source and/or sink is registered
      case Some(table: ConnectorCatalogTable[_, _]) =>
        if (table.getTableSink.isPresent) {
          // wrapper contains sink
          throw new TableException(s"Table '$name' already exists. " +
            s"Please choose a different name.")
        } else {
          // wrapper contains only source (not sink)
          val sourceAndSink = ConnectorCatalogTable.sourceAndSink(
            table.getTableSource.get,
            tableSink,
            isBatchTable)
          catalogManager.createTemporaryTable(
            sourceAndSink,
            objectIdentifier,
            true)
        }

      // no table is registered
      case _ =>
        val sink = ConnectorCatalogTable.sink(tableSink, isBatchTable)
        catalogManager.createTemporaryTable(sink, objectIdentifier, false)
    }
  }

  @throws[TableException]
  override def scan(tablePath: String*): Table = {
    val unresolvedIdentifier = UnresolvedIdentifier.of(tablePath: _*)
    scanInternal(unresolvedIdentifier) match {
      case Some(table) => createTable(table)
      case None => throw new TableException(s"Table '$unresolvedIdentifier' was not found.")
    }
  }

  override def from(path: String): Table = {
    val parser = planningConfigurationBuilder.createCalciteParser()
    val unresolvedIdentifier = UnresolvedIdentifier.of(parser.parseIdentifier(path).names: _*)
    scanInternal(unresolvedIdentifier) match {
      case Some(table) => createTable(table)
      case None => throw new TableException(s"Table '$unresolvedIdentifier' was not found.")
    }
  }

  private[flink] def scanInternal(identifier: UnresolvedIdentifier)
    : Option[CatalogQueryOperation] = {
    val objectIdentifier: ObjectIdentifier = catalogManager.qualifyIdentifier(identifier)

    JavaScalaConversionUtil.toScala(catalogManager.getTable(objectIdentifier))
      .map(t => new CatalogQueryOperation(objectIdentifier, t.getTable.getSchema))
  }

  override def listModules(): Array[String] = {
    moduleManager.listModules().asScala.toArray
  }

  override def listCatalogs(): Array[String] = {
    catalogManager.listCatalogs
      .asScala
      .toArray
      .sorted
  }

  override def listDatabases(): Array[String] = {
    catalogManager.getCatalog(catalogManager.getCurrentCatalog)
      .get()
      .listDatabases()
      .asScala.toArray
  }

  override def listTables(): Array[String] = {
    catalogManager.listTables().asScala
      .toArray
      .sorted
  }

  override def listTemporaryTables(): Array[String] = {
    catalogManager.listTemporaryTables().asScala
      .toArray
      .sorted
  }

  override def listTemporaryViews(): Array[String] = {
    catalogManager.listTemporaryViews().asScala
      .toArray
      .sorted
  }

  override def dropTemporaryTable(path: String): Boolean = {
    val parser = planningConfigurationBuilder.createCalciteParser()
    val unresolvedIdentifier = UnresolvedIdentifier.of(parser.parseIdentifier(path).names: _*)
    catalogManager.dropTemporaryTable(unresolvedIdentifier)
  }

  override def dropTemporaryView(path: String): Boolean = {
    val parser = planningConfigurationBuilder.createCalciteParser()
    val unresolvedIdentifier = UnresolvedIdentifier.of(parser.parseIdentifier(path).names: _*)
    catalogManager.dropTemporaryView(unresolvedIdentifier)
  }

  override def listUserDefinedFunctions(): Array[String] = functionCatalog.getUserDefinedFunctions

  override def listFunctions(): Array[String] = functionCatalog.getFunctions

  override def explain(table: Table): String

  override def getCompletionHints(statement: String, position: Int): Array[String] = {
    val planner = getFlinkPlanner
    planner.getCompletionHints(statement, position)
  }

  override def sqlQuery(query: String): Table = {
    val operations = parser.parse(query)

    if (operations.size != 1) throw new ValidationException(
      "Unsupported SQL query! sqlQuery() only accepts a single SQL query.")

    operations.get(0) match {
      case op: QueryOperation if !op.isInstanceOf[ModifyOperation] =>
        createTable(op)
      case _ => throw new ValidationException(
        "Unsupported SQL query! sqlQuery() only accepts a single SQL query of type " +
          "SELECT, UNION, INTERSECT, EXCEPT, VALUES, and ORDER_BY.")
    }
  }

  override def sqlUpdate(stmt: String): Unit = {
    val operations = parser.parse(stmt)

    if (operations.size != 1) throw new TableException(UNSUPPORTED_QUERY_IN_SQL_UPDATE_MSG)

    operations.get(0) match {
      case op: CatalogSinkModifyOperation =>
        insertInto(
          createTable(op.getChild),
          InsertOptions(op.getStaticPartitions, op.isOverwrite),
          op.getTableIdentifier)
      case createTableOperation: CreateTableOperation =>
        catalogManager.createTable(
          createTableOperation.getCatalogTable,
          createTableOperation.getTableIdentifier,
          createTableOperation.isIgnoreIfExists)
      case createDatabaseOperation: CreateDatabaseOperation =>
        val catalog = getCatalogOrThrowException(createDatabaseOperation.getCatalogName)
        val exMsg = getDDLOpExecuteErrorMsg(createDatabaseOperation.asSummaryString)
        try {
          catalog.createDatabase(
            createDatabaseOperation.getDatabaseName,
            createDatabaseOperation.getCatalogDatabase,
            createDatabaseOperation.isIgnoreIfExists)
        } catch {
          case ex: DatabaseAlreadyExistException => throw new ValidationException(exMsg, ex)
          case ex: Exception => throw new TableException(exMsg, ex)
        }
      case dropTableOperation: DropTableOperation =>
        catalogManager.dropTable(
          dropTableOperation.getTableIdentifier,
          dropTableOperation.isIfExists)
      case alterTableOperation: AlterTableOperation => {
        val catalog = getCatalogOrThrowException(
          alterTableOperation.getTableIdentifier.getCatalogName)
        val exMsg = getDDLOpExecuteErrorMsg(alterTableOperation.asSummaryString)
        try {
          alterTableOperation match {
            case alterTableRenameOp: AlterTableRenameOperation =>
              catalog.renameTable(
                alterTableRenameOp.getTableIdentifier.toObjectPath,
                alterTableRenameOp.getNewTableIdentifier.getObjectName,
                false)
            case alterTablePropertiesOp: AlterTablePropertiesOperation =>
              catalog.alterTable(
                alterTablePropertiesOp.getTableIdentifier.toObjectPath,
                alterTablePropertiesOp.getCatalogTable,
                false)
          }
        } catch {
          case ex: TableNotExistException => throw new ValidationException(exMsg, ex)
          case ex: Exception => throw new TableException(exMsg, ex)
        }
      }
      case dropDatabaseOperation: DropDatabaseOperation =>
        val catalog = getCatalogOrThrowException(dropDatabaseOperation.getCatalogName)
        val exMsg = getDDLOpExecuteErrorMsg(dropDatabaseOperation.asSummaryString)
        try {
          catalog.dropDatabase(
            dropDatabaseOperation.getDatabaseName,
            dropDatabaseOperation.isIfExists,
            dropDatabaseOperation.isCascade)
        } catch {
          case ex: DatabaseNotEmptyException => throw new ValidationException(exMsg, ex)
          case ex: DatabaseNotExistException => throw new ValidationException(exMsg, ex)
          case ex: Exception => throw new TableException(exMsg, ex)
        }
      case alterDatabaseOperation: AlterDatabaseOperation =>
        val catalog = getCatalogOrThrowException(alterDatabaseOperation.getCatalogName)
        val exMsg = getDDLOpExecuteErrorMsg(alterDatabaseOperation.asSummaryString)
        try {
          catalog.alterDatabase(
            alterDatabaseOperation.getDatabaseName,
            alterDatabaseOperation.getCatalogDatabase,
            false)
        } catch {
          case ex: DatabaseNotExistException => throw new ValidationException(exMsg, ex)
          case ex: Exception => throw new TableException(exMsg, ex)
        }
      case createFunctionOperation: CreateCatalogFunctionOperation =>
          createCatalogFunction(createFunctionOperation)
      case createTempSystemFunctionOperation: CreateTempSystemFunctionOperation =>
          createSystemFunction(createTempSystemFunctionOperation)
      case alterFunctionOperation: AlterCatalogFunctionOperation =>
          alterCatalogFunction(alterFunctionOperation)
      case dropFunctionOperation: DropCatalogFunctionOperation =>
          dropCatalogFunction(dropFunctionOperation)
      case dropTempSystemFunctionOperation: DropTempSystemFunctionOperation =>
          dropSystemFunction(dropTempSystemFunctionOperation)
      case useCatalogOperation: UseCatalogOperation =>
        catalogManager.setCurrentCatalog(useCatalogOperation.getCatalogName)
      case useDatabaseOperation: UseDatabaseOperation =>
        catalogManager.setCurrentCatalog(useDatabaseOperation.getCatalogName)
        catalogManager.setCurrentDatabase(useDatabaseOperation.getDatabaseName)
      case _ => throw new TableException(UNSUPPORTED_QUERY_IN_SQL_UPDATE_MSG)
    }
  }

  /** Get catalog from catalogName or throw a ValidationException if the catalog not exists. */
  private def getCatalogOrThrowException(catalogName: String): Catalog = {
    getCatalog(catalogName)
      .orElseThrow(
        new JSupplier[Throwable] {
          override def get() = new ValidationException(
            String.format("Catalog %s does not exist", catalogName))
        })
  }

  private def getDDLOpExecuteErrorMsg(action: String):String = {
    String.format("Could not execute %s", action)
  }

  protected def createTable(tableOperation: QueryOperation): TableImpl = {
    TableImpl.createTable(
      this,
      tableOperation,
      operationTreeBuilder,
      functionCatalog.asLookup(new JFunction[String, UnresolvedIdentifier] {
        override def apply(t: String): UnresolvedIdentifier = parser.parseIdentifier(t)
      }))
  }

  /**
    * Writes a [[Table]] to a [[TableSink]].
    *
    * @param table The [[Table]] to write.
    * @param sink The [[TableSink]] to write the [[Table]] to.
    * @tparam T The data type that the [[TableSink]] expects.
    */
  private[flink] def writeToSink[T](table: Table, sink: TableSink[T]): Unit

  override def insertInto(path: String, table: Table): Unit = {
    val parser = planningConfigurationBuilder.createCalciteParser()
    val unresolvedIdentifier = UnresolvedIdentifier.of(parser.parseIdentifier(path).names: _*)
    val objectIdentifier: ObjectIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier)
    insertInto(
      table,
      InsertOptions(new JHashMap[String, String](), overwrite = false),
      objectIdentifier)
  }

  override def insertInto(
        table: Table,
        sinkPath: String,
        sinkPathContinued: String*): Unit = {
    val unresolvedIdentifier = UnresolvedIdentifier.of(sinkPath +: sinkPathContinued: _*)
    val objectIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier)
    insertInto(
      table,
      InsertOptions(new JHashMap[String, String](), overwrite = false),
      objectIdentifier)
  }

  /** Insert options for executing sql insert. **/
  case class InsertOptions(staticPartitions: JMap[String, String], overwrite: Boolean)

  /**
    * Writes the [[Table]] to a [[TableSink]] that was registered under the specified name.
    *
    * @param table The table to write to the TableSink.
    * @param sinkIdentifier The name of the registered TableSink.
    */
  private def insertInto(
      table: Table,
      insertOptions: InsertOptions,
      sinkIdentifier: ObjectIdentifier): Unit = {

    getTableSink(sinkIdentifier) match {

      case None =>
        throw new TableException(s"No table was registered under the name $sinkIdentifier.")

      case Some(tableSink) =>
        // validate schema of source table and table sink
        TableSinkUtils.validateSink(
          insertOptions.staticPartitions,
          table.getQueryOperation,
          sinkIdentifier,
          tableSink)
        // set static partitions if it is a partitioned table sink
        tableSink match {
          case partitionableSink: PartitionableTableSink =>
            partitionableSink.setStaticPartition(insertOptions.staticPartitions)
          case _ =>
        }
        // set whether to overwrite if it's an OverwritableTableSink
        tableSink match {
          case overwritableTableSink: OverwritableTableSink =>
            overwritableTableSink.setOverwrite(insertOptions.overwrite)
          case _ =>
            require(!insertOptions.overwrite, "INSERT OVERWRITE requires " +
              s"${classOf[OverwritableTableSink].getSimpleName} but actually got " +
              tableSink.getClass.getName)
        }
        // emit the table to the configured table sink
        writeToSink(table, tableSink)
    }
  }

  private def getTableSink(objectIdentifier: ObjectIdentifier): Option[TableSink[_]] = {
    JavaScalaConversionUtil.toScala(catalogManager.getTable(objectIdentifier))
      .map(_.getTable) match {
      case Some(s) if s.isInstanceOf[ConnectorCatalogTable[_, _]] =>

        JavaScalaConversionUtil
          .toScala(s.asInstanceOf[ConnectorCatalogTable[_, _]].getTableSink)

      case Some(s) if s.isInstanceOf[CatalogTable] =>

        val catalog = catalogManager.getCatalog(objectIdentifier.getCatalogName)
        val catalogTable = s.asInstanceOf[CatalogTable]
        val context = new TableSinkFactoryContextImpl(
          objectIdentifier, catalogTable, config.getConfiguration)
        if (catalog.isPresent && catalog.get().getTableFactory.isPresent) {
          val sink = TableFactoryUtil.createTableSinkForCatalogTable(catalog.get(), context)
          if (sink.isPresent) {
            return Option(sink.get())
          }
        }
        Option(TableFactoryUtil.findAndCreateTableSink(context))

      case _ => None
    }
  }

  protected def getTemporaryTable(identifier: ObjectIdentifier): Option[CatalogBaseTable] = {
    JavaScalaConversionUtil.toScala(catalogManager.getTable(identifier))
      .filter(_.isTemporary)
      .map(_.getTable)
  }

  private def createCatalogFunction(createFunctionOperation: CreateCatalogFunctionOperation)= {
    val exMsg = getDDLOpExecuteErrorMsg(createFunctionOperation.asSummaryString)
    try {
      val function = createFunctionOperation.getCatalogFunction
      if (createFunctionOperation.isTemporary) {
        val exist = functionCatalog.hasTemporaryCatalogFunction(
          createFunctionOperation.getFunctionIdentifier);
        if (!exist) {
          val functionDefinition = FunctionDefinitionUtil.createFunctionDefinition(
            createFunctionOperation.getFunctionName, function.getClassName)
          registerCatalogFunctionInFunctionCatalog(
            createFunctionOperation.getFunctionIdentifier,
            functionDefinition)
        } else if (!createFunctionOperation.isIgnoreIfExists) {
          throw new ValidationException(
            String.format("Temporary catalog function %s is already defined",
            createFunctionOperation.getFunctionIdentifier.asSerializableString))
        }
      } else {
        val catalog = getCatalogOrThrowException(
          createFunctionOperation.getFunctionIdentifier.getCatalogName)
        catalog.createFunction(
          createFunctionOperation.getFunctionIdentifier.toObjectPath,
          createFunctionOperation.getCatalogFunction,
          createFunctionOperation.isIgnoreIfExists)
      }
    } catch {
      case ex: ValidationException => throw ex
      case ex: FunctionAlreadyExistException => throw new ValidationException(ex.getMessage, ex)
      case ex: Exception => throw new TableException(exMsg, ex)
    }
  }

  private def alterCatalogFunction(alterFunctionOperation: AlterCatalogFunctionOperation) = {
    val exMsg = getDDLOpExecuteErrorMsg(alterFunctionOperation.asSummaryString)
    try {
      val function = alterFunctionOperation.getCatalogFunction
      if (alterFunctionOperation.isTemporary) {
        throw new ValidationException("Alter temporary catalog function is not supported")
      } else {
        val catalog = getCatalogOrThrowException(
          alterFunctionOperation.getFunctionIdentifier.getCatalogName)
        catalog.alterFunction(
          alterFunctionOperation.getFunctionIdentifier.toObjectPath,
          alterFunctionOperation.getCatalogFunction,
          alterFunctionOperation.isIfExists)
      }
    } catch {
      case ex: ValidationException => throw ex
      case ex: FunctionNotExistException => throw new ValidationException(ex.getMessage, ex)
      case ex: Exception => throw new TableException(exMsg, ex)
    }
  }

  private def dropCatalogFunction(dropFunctionOperation: DropCatalogFunctionOperation) = {
    val exMsg = getDDLOpExecuteErrorMsg(dropFunctionOperation.asSummaryString)
    try {
      if (dropFunctionOperation.isTemporary)  {
          functionCatalog.dropTempCatalogFunction(
            dropFunctionOperation.getFunctionIdentifier, dropFunctionOperation.isIfExists)
      } else  {
        val catalog = getCatalogOrThrowException(
          dropFunctionOperation.getFunctionIdentifier.getCatalogName)
        catalog.dropFunction(
          dropFunctionOperation.getFunctionIdentifier.toObjectPath,
          dropFunctionOperation.isIfExists)
      }
    } catch {
      case ex: ValidationException => throw ex
      case ex: FunctionNotExistException => throw new ValidationException(ex.getMessage, ex)
      case ex: Exception => throw new TableException(exMsg, ex)
    }
  }

  private def createSystemFunction(
      createFunctionOperation: CreateTempSystemFunctionOperation): Unit = {
    val exMsg = getDDLOpExecuteErrorMsg(createFunctionOperation.asSummaryString)
    try {
      val exist = functionCatalog.hasTemporarySystemFunction(
        createFunctionOperation.getFunctionName)
      if (!exist) {
        val functionDefinition = FunctionDefinitionUtil.createFunctionDefinition(
          createFunctionOperation.getFunctionName, createFunctionOperation.getFunctionClass)
        registerSystemFunctionInFunctionCatalog(
          createFunctionOperation.getFunctionName,
          functionDefinition)
      } else if (!createFunctionOperation.isIgnoreIfExists) {
        throw new ValidationException(
          String.format("Temporary system function %s is already defined",
          createFunctionOperation.getFunctionName))
      }
    } catch {
      case e: ValidationException =>
        throw e
      case e: Exception =>
        throw new TableException(exMsg, e)
    }
  }

  private def dropSystemFunction(dropFunctionOperation: DropTempSystemFunctionOperation): Unit = {
    val exMsg = getDDLOpExecuteErrorMsg(dropFunctionOperation.asSummaryString)
    try {
      functionCatalog.dropTemporarySystemFunction(
        dropFunctionOperation.getFunctionName, dropFunctionOperation.isIfExists)
    } catch {
      case e: ValidationException =>
        throw e
      case e: Exception =>
        throw new TableException(exMsg, e)
    }
  }

  private def registerCatalogFunctionInFunctionCatalog[T, ACC](
      functionIdentifier: ObjectIdentifier,
      functionDefinition: FunctionDefinition): Unit = {

    if (functionDefinition.isInstanceOf[ScalarFunctionDefinition]) {
      val scalarFunctionDefinition = functionDefinition.asInstanceOf[ScalarFunctionDefinition]
      functionCatalog.registerTempCatalogScalarFunction(
        functionIdentifier,
        scalarFunctionDefinition.getScalarFunction)
    } else if (functionDefinition.isInstanceOf[AggregateFunctionDefinition]) {
      val aggregateFunctionDefinition = functionDefinition.asInstanceOf[AggregateFunctionDefinition]
      val aggregateFunction = aggregateFunctionDefinition
        .getAggregateFunction.asInstanceOf[AggregateFunction[T, ACC]]
      val typeInfo = UserDefinedFunctionHelper.getReturnTypeOfAggregateFunction(aggregateFunction)
      val accTypeInfo = UserDefinedFunctionHelper
        .getAccumulatorTypeOfAggregateFunction(aggregateFunction)
      functionCatalog.registerTempCatalogAggregateFunction(
        functionIdentifier,
        aggregateFunction,
        typeInfo,
        accTypeInfo)

    } else if (functionDefinition.isInstanceOf[TableFunctionDefinition]) {
      val tableFunctionDefinition = functionDefinition.asInstanceOf[TableFunctionDefinition]
      val tableFunction = tableFunctionDefinition.getTableFunction.asInstanceOf[TableFunction[T]]
      val typeInfo = UserDefinedFunctionHelper.getReturnTypeOfTableFunction(tableFunction)
      functionCatalog.registerTempCatalogTableFunction(
        functionIdentifier,
        tableFunction,
        typeInfo)

    }
  }

  private def registerSystemFunctionInFunctionCatalog[T, ACC](
      functionName: String,
      functionDefinition: FunctionDefinition): Unit = {
    if (functionDefinition.isInstanceOf[ScalarFunctionDefinition]) {
      val scalarFunctionDefinition = functionDefinition.asInstanceOf[ScalarFunctionDefinition]
      functionCatalog.registerTempSystemScalarFunction(
        functionName,
        scalarFunctionDefinition.getScalarFunction)
    } else if (functionDefinition.isInstanceOf[AggregateFunctionDefinition]) {
      val aggregateFunctionDefinition = functionDefinition.asInstanceOf[AggregateFunctionDefinition]
      val aggregateFunction = aggregateFunctionDefinition
        .getAggregateFunction.asInstanceOf[AggregateFunction[T, ACC]]
      val typeInfo = UserDefinedFunctionHelper.getReturnTypeOfAggregateFunction(aggregateFunction)
      val accTypeInfo = UserDefinedFunctionHelper
        .getAccumulatorTypeOfAggregateFunction(aggregateFunction)
      functionCatalog.registerTempSystemAggregateFunction(
        functionName,
        aggregateFunction,
        typeInfo,
        accTypeInfo)
    } else if (functionDefinition.isInstanceOf[TableFunctionDefinition]) {
      val tableFunctionDefinition = functionDefinition.asInstanceOf[TableFunctionDefinition]
      val tableFunction = tableFunctionDefinition.getTableFunction.asInstanceOf[TableFunction[T]]
      val typeInfo = UserDefinedFunctionHelper.getReturnTypeOfTableFunction(tableFunction)
      functionCatalog.registerTempSystemTableFunction(
        functionName,
        tableFunction,
        typeInfo)

    }
  }

  /** Returns the [[FlinkRelBuilder]] of this TableEnvironment. */
  private[flink] def getRelBuilder: FlinkRelBuilder = {
    val currentCatalogName = catalogManager.getCurrentCatalog
    val currentDatabase = catalogManager.getCurrentDatabase

    planningConfigurationBuilder.createRelBuilder(currentCatalogName, currentDatabase)
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
