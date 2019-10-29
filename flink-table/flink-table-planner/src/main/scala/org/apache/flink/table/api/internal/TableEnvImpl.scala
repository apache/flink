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
import org.apache.flink.table.delegation.Parser
import org.apache.flink.table.expressions._
import org.apache.flink.table.expressions.resolver.lookups.TableReferenceLookup
import org.apache.flink.table.factories.{TableFactoryService, TableFactoryUtil, TableSinkFactory}
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction, UserDefinedAggregateFunction, _}
import org.apache.flink.table.module.{Module, ModuleManager}
import org.apache.flink.table.operations.ddl.{CreateTableOperation, DropTableOperation}
import org.apache.flink.table.operations.utils.OperationTreeBuilder
import org.apache.flink.table.operations.{CatalogQueryOperation, TableSourceQueryOperation, _}
import org.apache.flink.table.planner.{ParserImpl, PlanningConfigurationBuilder}
import org.apache.flink.table.sinks.{OverwritableTableSink, PartitionableTableSink, TableSink, TableSinkUtils}
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.util.JavaScalaConversionUtil

import org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.FrameworkConfig

import _root_.java.util.function.{Supplier => JSupplier}
import _root_.java.util.{Optional, HashMap => JHashMap, Map => JMap}

import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.JavaConversions._

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
    new FunctionCatalog(catalogManager, moduleManager)

  // temporary utility until we don't use planner expressions anymore
  functionCatalog.setPlannerTypeInferenceUtil(PlannerTypeInferenceUtilImpl.INSTANCE)

  // temporary bridge between API and planner
  private[flink] val expressionBridge: ExpressionBridge[PlannerExpression] =
    new ExpressionBridge[PlannerExpression](functionCatalog, PlannerExpressionConverter.INSTANCE)

  private def tableLookup: TableReferenceLookup = {
    new TableReferenceLookup {
      override def lookupTable(name: String): Optional[TableReferenceExpression] = {
        JavaScalaConversionUtil
          .toJava(scanInternal(Array(name)).map(t => new TableReferenceExpression(name, t)))
      }
    }
  }

  private[flink] val operationTreeBuilder = OperationTreeBuilder.create(
    functionCatalog,
    tableLookup,
    isStreamingMode)

  protected val planningConfigurationBuilder: PlanningConfigurationBuilder =
    new PlanningConfigurationBuilder(
      config,
      functionCatalog,
      asRootSchema(new CatalogManagerCalciteSchema(catalogManager, isStreamingMode)),
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
    val resultTypeInfo: TypeInformation[T] = UserFunctionsTypeHelper
      .getReturnTypeOfAggregateFunction(
        function,
        implicitly[TypeInformation[T]])

    val accTypeInfo: TypeInformation[ACC] = UserFunctionsTypeHelper
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

    // check that table belongs to this table environment
    if (table.asInstanceOf[TableImpl].getTableEnvironment != this) {
      throw new TableException(
        "Only tables that belong to this TableEnvironment can be registered.")
    }

    val view = new QueryOperationCatalogView(table.getQueryOperation)
    catalogManager.createTemporaryTable(view, getTemporaryObjectIdentifier(name), false)
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
    // register
    getTemporaryTable(
      catalogManager.getBuiltInCatalogName,
      catalogManager.getBuiltInDatabaseName, name) match {

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
            getTemporaryObjectIdentifier(name),
            true)
        }

      // no table is registered
      case _ =>
        val source = ConnectorCatalogTable.source(tableSource, isBatchTable)
        catalogManager.createTemporaryTable(source, getTemporaryObjectIdentifier(name), false)
    }
  }

  private def registerTableSinkInternal(
    name: String,
    tableSink: TableSink[_])
  : Unit = {
    // check if a table (source or sink) is registered
    getTemporaryTable(
      catalogManager.getBuiltInCatalogName,
      catalogManager.getBuiltInDatabaseName,
      name) match {

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
            getTemporaryObjectIdentifier(name),
            true)
        }

      // no table is registered
      case _ =>
        val sink = ConnectorCatalogTable.sink(tableSink, isBatchTable)
        catalogManager.createTemporaryTable(sink, getTemporaryObjectIdentifier(name), false)
    }
  }

  private def getTemporaryObjectIdentifier(name: String): ObjectIdentifier = {
    catalogManager.qualifyIdentifier(
      UnresolvedIdentifier.of(
        catalogManager.getBuiltInCatalogName,
        catalogManager.getBuiltInDatabaseName,
        name
      ))
  }

  @throws[TableException]
  override def scan(tablePath: String*): Table = {
    scanInternal(tablePath.toArray) match {
      case Some(table) => createTable(table)
      case None => throw new TableException(s"Table '${tablePath.mkString(".")}' was not found.")
    }
  }

  private[flink] def scanInternal(tablePath: Array[String]): Option[CatalogQueryOperation] = {
    val unresolvedIdentifier = UnresolvedIdentifier.of(tablePath: _*)
    val objectIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier)
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

    if (operations.size != 1) throw new TableException(
      "Unsupported SQL query! sqlUpdate() only accepts a single SQL statement of type " +
        "INSERT, CREATE TABLE, DROP TABLE")

    operations.get(0) match {
      case op: CatalogSinkModifyOperation =>
        insertInto(
          createTable(op.getChild),
          InsertOptions(op.getStaticPartitions, op.isOverwrite),
          op.getTableIdentifier.getCatalogName,
          op.getTableIdentifier.getDatabaseName,
          op.getTableIdentifier.getObjectName)
      case createTableOperation: CreateTableOperation =>
        catalogManager.createTable(
          createTableOperation.getCatalogTable,
          createTableOperation.getTableIdentifier,
          createTableOperation.isIgnoreIfExists)
      case dropTableOperation: DropTableOperation =>
        catalogManager.dropTable(
          dropTableOperation.getTableIdentifier,
          dropTableOperation.isIfExists)
      case _ => throw new TableException(
        "Unsupported SQL query! sqlUpdate() only accepts a single SQL statements of " +
          "type INSERT, CREATE TABLE, DROP TABLE")
    }
  }

  protected def createTable(tableOperation: QueryOperation): TableImpl = {
    TableImpl.createTable(
      this,
      tableOperation,
      operationTreeBuilder,
      functionCatalog)
  }

  /**
    * Writes a [[Table]] to a [[TableSink]].
    *
    * @param table The [[Table]] to write.
    * @param sink The [[TableSink]] to write the [[Table]] to.
    * @tparam T The data type that the [[TableSink]] expects.
    */
  private[flink] def writeToSink[T](table: Table, sink: TableSink[T]): Unit

  override def insertInto(
      table: Table,
      path: String,
      pathContinued: String*): Unit = {
    insertInto(
      table,
      InsertOptions(new JHashMap[String, String](), false),
      path +: pathContinued: _*)
  }

  /** Insert options for executing sql insert. **/
  case class InsertOptions(staticPartitions: JMap[String, String], overwrite: Boolean)

  /**
    * Writes the [[Table]] to a [[TableSink]] that was registered under the specified name.
    *
    * @param table The table to write to the TableSink.
    * @param sinkTablePath The name of the registered TableSink.
    */
  private def insertInto(table: Table,
      insertOptions: InsertOptions,
      sinkTablePath: String*): Unit = {

    val unresolvedIdentifier = UnresolvedIdentifier.of(sinkTablePath: _*)
    val objectIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier)

    getTableSink(objectIdentifier) match {

      case None =>
        throw new TableException(s"No table was registered under the name $sinkTablePath.")

      case Some(tableSink) =>
        // validate schema of source table and table sink
        TableSinkUtils.validateSink(
          insertOptions.staticPartitions,
          table.getQueryOperation,
          objectIdentifier,
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
        if (catalog.isPresent && catalog.get().getTableFactory.isPresent) {
          val sink = TableFactoryUtil.createTableSinkForCatalogTable(
            catalog.get(),
            catalogTable,
            objectIdentifier.toObjectPath)
          if (sink.isPresent) {
            return Option(sink.get())
          }
        }
        val sinkProperties = catalogTable.toProperties
        Option(TableFactoryService.find(classOf[TableSinkFactory[_]], sinkProperties)
          .createTableSink(sinkProperties))

      case _ => None
    }
  }

  protected def getTemporaryTable(name: String*): Option[CatalogBaseTable] = {
    val unresolvedIdentifier = UnresolvedIdentifier.of(name: _*)
    val objectIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier)
    JavaScalaConversionUtil.toScala(catalogManager.getTable(objectIdentifier))
      .filter(_.isTemporary)
      .map(_.getTable)
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
