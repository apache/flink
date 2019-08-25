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
import org.apache.flink.sql.parser.ddl.{SqlCreateTable, SqlDropTable}
import org.apache.flink.sql.parser.dml.RichSqlInsert
import org.apache.flink.table.api._
import org.apache.flink.table.calcite.{FlinkPlannerImpl, FlinkRelBuilder}
import org.apache.flink.table.catalog._
import org.apache.flink.table.expressions._
import org.apache.flink.table.expressions.resolver.lookups.TableReferenceLookup
import org.apache.flink.table.factories.{TableFactoryService, TableFactoryUtil, TableSinkFactory}
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction, UserDefinedAggregateFunction, _}
import org.apache.flink.table.operations.ddl.CreateTableOperation
import org.apache.flink.table.operations.utils.OperationTreeBuilder
import org.apache.flink.table.operations.{CatalogQueryOperation, PlannerQueryOperation, TableSourceQueryOperation, _}
import org.apache.flink.table.planner.PlanningConfigurationBuilder
import org.apache.flink.table.sinks.{PartitionableTableSink, TableSink, TableSinkUtils}
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.sqlexec.SqlToOperationConverter
import org.apache.flink.table.util.JavaScalaConversionUtil

import org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema
import org.apache.calcite.sql._
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.FrameworkConfig

import _root_.java.util.{Optional, HashMap => JHashMap, Map => JMap}

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._

/**
  * The abstract base class for the implementation of batch TableEnvironment.
  *
  * @param config The configuration of the TableEnvironment
  */
abstract class TableEnvImpl(
    val config: TableConfig,
    private val catalogManager: CatalogManager)
  extends TableEnvironment {

  // Table API/SQL function catalog
  private[flink] val functionCatalog: FunctionCatalog = new FunctionCatalog(catalogManager)

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

  def getConfig: TableConfig = config

  private def isStreamingMode: Boolean = this match {
    case _: BatchTableEnvImpl => false
    case _ => true
  }

  private def isBatchTable: Boolean = !isStreamingMode

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
    if (table.asInstanceOf[TableImpl].getTableEnvironment != this) {
      throw new TableException(
        "Only tables that belong to this TableEnvironment can be registered.")
    }

    val view = new QueryOperationCatalogView(table.getQueryOperation)
    catalogManager.createTable(view, getTemporaryObjectIdentifier(name), false)
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
    getCatalogTable(
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
          catalogManager.alterTable(sourceAndSink, getTemporaryObjectIdentifier(name), false)
        }

      // no table is registered
      case _ =>
        val source = ConnectorCatalogTable.source(tableSource, isBatchTable)
        catalogManager.createTable(source, getTemporaryObjectIdentifier(name), false)
    }
  }

  private def registerTableSinkInternal(
    name: String,
    tableSink: TableSink[_])
  : Unit = {
    // check if a table (source or sink) is registered
    getCatalogTable(
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
          catalogManager.alterTable(sourceAndSink, getTemporaryObjectIdentifier(name), false)
        }

      // no table is registered
      case _ =>
        val sink = ConnectorCatalogTable.sink(tableSink, isBatchTable)
        catalogManager.createTable(sink, getTemporaryObjectIdentifier(name), false)
    }
  }

  private def getTemporaryObjectIdentifier(name: String): ObjectIdentifier = {
    catalogManager.qualifyIdentifier(
      catalogManager.getBuiltInCatalogName,
      catalogManager.getBuiltInDatabaseName,
      name
    )
  }

  @throws[TableException]
  override def scan(tablePath: String*): Table = {
    scanInternal(tablePath.toArray) match {
      case Some(table) => createTable(table)
      case None => throw new TableException(s"Table '${tablePath.mkString(".")}' was not found.")
    }
  }

  private[flink] def scanInternal(tablePath: Array[String]): Option[CatalogQueryOperation] = {
    val objectIdentifier = catalogManager.qualifyIdentifier(tablePath: _*)
    JavaScalaConversionUtil.toScala(catalogManager.getTable(objectIdentifier))
      .map(t => new CatalogQueryOperation(objectIdentifier, t.getSchema))
  }

  override def listCatalogs(): Array[String] = {
    catalogManager.getCatalogs.asScala.toArray
  }

  override def listDatabases(): Array[String] = {
    catalogManager.getCatalog(catalogManager.getCurrentCatalog)
      .get()
      .listDatabases()
      .asScala.toArray
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

  override def listUserDefinedFunctions(): Array[String] = functionCatalog.getUserDefinedFunctions

  override def listFunctions(): Array[String] = functionCatalog.getFunctions

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
      createTable(new PlannerQueryOperation(relational.rel))
    } else {
      throw new TableException(
        "Unsupported SQL query! sqlQuery() only accepts SQL queries of type " +
          "SELECT, UNION, INTERSECT, EXCEPT, VALUES, and ORDER_BY.")
    }
  }

  override def sqlUpdate(stmt: String): Unit = {
    val planner = getFlinkPlanner
    // parse the sql query
    val parsed = planner.parse(stmt)
    parsed match {
      case insert: RichSqlInsert =>
        // validate the insert
        val validatedInsert = planner.validate(insert).asInstanceOf[RichSqlInsert]
        // we do not validate the row type for sql insert now, so validate the source
        // separately.
        val validatedQuery = planner.validate(validatedInsert.getSource)

        val tableOperation = new PlannerQueryOperation(planner.rel(validatedQuery).rel)
        // get query result as Table
        val queryResult = createTable(tableOperation)

        // get name of sink table
        val targetTablePath = insert.getTargetTable.asInstanceOf[SqlIdentifier].names

        // insert query result into sink table
        insertInto(queryResult, InsertOptions(insert.getStaticPartitionKVs),
          targetTablePath.asScala:_*)
      case createTable: SqlCreateTable =>
        val operation = SqlToOperationConverter
          .convert(planner, createTable)
          .asInstanceOf[CreateTableOperation]
        val objectIdentifier = catalogManager.qualifyIdentifier(operation.getTablePath: _*)
        catalogManager.createTable(
          operation.getCatalogTable,
          objectIdentifier,
          operation.isIgnoreIfExists)
      case dropTable: SqlDropTable =>
        val objectIdentifier = catalogManager.qualifyIdentifier(dropTable.fullTableName(): _*)
        catalogManager.dropTable(objectIdentifier, dropTable.getIfExists)
      case _ =>
        throw new TableException(
          "Unsupported SQL query! sqlUpdate() only accepts SQL statements of " +
            "type INSERT, CREATE TABLE, DROP TABLE.")
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
      InsertOptions(new JHashMap[String, String]()),
      path +: pathContinued: _*)
  }

  /** Insert options for executing sql insert. **/
  case class InsertOptions(staticPartitions: JMap[String, String])

  /**
    * Writes the [[Table]] to a [[TableSink]] that was registered under the specified name.
    *
    * @param table The table to write to the TableSink.
    * @param sinkTablePath The name of the registered TableSink.
    */
  private def insertInto(table: Table,
      insertOptions: InsertOptions,
      sinkTablePath: String*): Unit = {

    val objectIdentifier = catalogManager.qualifyIdentifier(sinkTablePath: _*)

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
          case partitionableSink: PartitionableTableSink
            if partitionableSink.getPartitionFieldNames != null
              && partitionableSink.getPartitionFieldNames.nonEmpty =>
            partitionableSink.setStaticPartition(insertOptions.staticPartitions)
          case _ =>
        }
        // emit the table to the configured table sink
        writeToSink(table, tableSink)
    }
  }

  private def getTableSink(objectIdentifier: ObjectIdentifier): Option[TableSink[_]] = {
    JavaScalaConversionUtil.toScala(catalogManager.getTable(objectIdentifier)) match {
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

  protected def getCatalogTable(name: String*): Option[CatalogBaseTable] = {
    val objectIdentifier = catalogManager.qualifyIdentifier(name: _*)
    JavaScalaConversionUtil.toScala(catalogManager.getTable(objectIdentifier))
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
