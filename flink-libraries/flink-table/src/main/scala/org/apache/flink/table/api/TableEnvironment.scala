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

import org.apache.flink.annotation.{Experimental, Internal, VisibleForTesting}
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils._
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaStreamExecEnv}
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment => ScalaStreamExecEnv}
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.api.java.{BatchTableEnvironment => JavaBatchTableEnvironment, StreamTableEnvironment => JavaStreamTableEnv}
import org.apache.flink.table.api.scala.{BatchTableEnvironment => ScalaBatchTableEnvironment, StreamTableEnvironment => ScalaStreamTableEnv, _}
import org.apache.flink.table.api.types._
import org.apache.flink.table.calcite._
import org.apache.flink.table.catalog._
import org.apache.flink.table.codegen._
import org.apache.flink.table.descriptors.{ConnectorDescriptor, TableDescriptor}
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.expressions.{Alias, Expression, TimeAttribute, UnresolvedFieldReference}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.logical.{CatalogNode, LogicalNode, LogicalRelNode, SinkNode}
import org.apache.flink.table.plan.nodes.exec.ExecNode
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.plan.stats.{FlinkStatistic, TableStats}
import org.apache.flink.table.sinks._
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.temptable.FlinkTableServiceManager
import org.apache.flink.table.typeutils.TypeUtils
import org.apache.flink.table.validate.{BuiltInFunctionCatalog, ChainedFunctionCatalog, FunctionCatalog}

import org.apache.calcite.config.Lex
import org.apache.calcite.plan.{Contexts, RelOptPlanner}
import org.apache.calcite.rel.logical.LogicalTableModify
import org.apache.calcite.schema
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.util.ChainedSqlOperatorTable
import org.apache.calcite.sql.{SqlIdentifier, SqlInsert, SqlOperatorTable, _}
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.tools._
import org.apache.commons.lang3.StringUtils

import _root_.java.lang.reflect.Modifier
import _root_.java.util
import _root_.java.util.concurrent.atomic.AtomicInteger
import _root_.java.util.concurrent.atomic.AtomicBoolean

import _root_.scala.annotation.varargs
import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable
import _root_.scala.collection.mutable.ArrayBuffer

/**
  * The abstract base class for batch and stream TableEnvironments.
  *
  * @param config The configuration of the TableEnvironment
  */
abstract class TableEnvironment(
    private[flink] val execEnv: JavaStreamExecEnv,
    val config: TableConfig) extends AutoCloseable {

  protected val DEFAULT_JOB_NAME = "Flink Exec Table Job"

  protected val catalogManager: CatalogManager = new CatalogManager()
  private val currentSchema: SchemaPlus = catalogManager.getRootSchema

  private val typeFactory: FlinkTypeFactory = new FlinkTypeFactory(new FlinkTypeSystem)

  // Table API/SQL function catalog (built in, does not contain external functions)
  private val functionCatalog: FunctionCatalog = BuiltInFunctionCatalog.withBuiltIns()

  // Table API/SQL function catalog built in function catalog.
  private[flink] lazy val chainedFunctionCatalog: FunctionCatalog =
    new ChainedFunctionCatalog(Seq(functionCatalog))

  // the configuration to create a Calcite planner
  protected var frameworkConfig: FrameworkConfig = createFrameworkConfig

  // the builder for Calcite RelNodes, Calcite's representation of a relational expression tree.
  protected var relBuilder: FlinkRelBuilder = createRelBuilder

  // the planner instance used to optimize queries of this TableEnvironment
  private var planner: RelOptPlanner = createRelOptPlanner

  // reuse flink planner
  private var flinkPlanner: FlinkPlannerImpl = createFlinkPlanner

  // a counter for unique attribute names
  private[flink] val attrNameCntr: AtomicInteger = new AtomicInteger(0)

  // a counter for unique table names
  private[flink] val tableNameCntr: AtomicInteger = new AtomicInteger(0)

  private[flink] val tableNamePrefix = "_TempTable_"

  // sink nodes collection
  private[flink] val sinkNodes = new mutable.MutableList[SinkNode]

  private[flink] val transformations = new ArrayBuffer[StreamTransformation[_]]

  protected var userClassloader: ClassLoader = null

  // a manager for table service
  private[flink] val tableServiceManager = new FlinkTableServiceManager(this)

  private val closed: AtomicBoolean = new AtomicBoolean(false)

  // the configuration for SqlToRelConverter
  private[flink] lazy val sqlToRelConverterConfig: SqlToRelConverter.Config = {
    val calciteConfig = config.getCalciteConfig
    calciteConfig.getSqlToRelConverterConfig match {
      case Some(c) => c
      case None => getSqlToRelConverterConfig
    }
  }

  /** Returns the table config to define the runtime behavior of the Table API. */
  def getConfig: TableConfig = config

  /** Returns the [[QueryConfig]] depends on the concrete type of this TableEnvironment. */
  private[flink] def queryConfig: QueryConfig = this match {
    case _: BatchTableEnvironment => new BatchQueryConfig
    case _: StreamTableEnvironment => new StreamQueryConfig
    case _ => null
  }

  /**
    * Compile the sink [[org.apache.flink.table.plan.logical.LogicalNode]] to
    * [[org.apache.flink.streaming.api.transformations.StreamTransformation]].
    */
  protected def compile(): Unit = {
    if (config.getSubsectionOptimization) {
      // optimize rel node, and translate to node dag
      val nodeDag = optimizeAndTranslateNodeDag(true, sinkNodes: _*)
      // translate to transformation
      val sinkTransformations = translate(nodeDag)
      transformations.addAll(sinkTransformations)
    }
  }

  /**
    * Optimize the RelNode tree (or DAG), and translate the result to [[ExecNode]] tree (or DAG).
    */
  @VisibleForTesting
  private[flink] def optimizeAndTranslateNodeDag(
      dagOptimizeEnabled: Boolean, logicalNodes: LogicalNode*): Seq[ExecNode[_, _]]

  /**
    * Translates a [[ExecNode]] DAG into a [[StreamTransformation]] DAG.
    *
    * @param sinks The node DAG to translate.
    * @return The [[StreamTransformation]] DAG that corresponds to the node DAG.
    */
  protected def translate(sinks: Seq[ExecNode[_, _]]): Seq[StreamTransformation[_]]

  /**
    * Generate a [[StreamGraph]] from this table environment, this will also
    * clear [[LogicalNode]]s.
    * @return A [[StreamGraph]] describing the whole job.
    */
  def generateStreamGraph(): StreamGraph = generateStreamGraph(DEFAULT_JOB_NAME)

  /**
    * Generate a [[StreamGraph]] from this table environment, this will also
    * clear [[LogicalNode]]s.
    * @return A [[StreamGraph]] describing the whole job.
    */
  def generateStreamGraph(jobName: String): StreamGraph = {
    try {
      if (config.getSubsectionOptimization) {
        compile()
      }
      if (transformations.isEmpty) {
        throw new TableException("No table sinks have been created yet. " +
          "A program needs at least one sink that consumes data. ")
      }
      translateStreamGraph(transformations, Option.apply(jobName))
    } finally {
      sinkNodes.clear()
    }
  }

  /**
    * Translate a [[StreamGraph]] from Given streamingTransformations.
    * @return A [[StreamGraph]] describing the given job.
    */
  protected def translateStreamGraph(
      streamingTransformations: ArrayBuffer[StreamTransformation[_]],
      jobName: Option[String]): StreamGraph = ???

  /**
    * Returns the operator table for this environment including a custom Calcite configuration.
    */
  protected def getSqlOperatorTable: SqlOperatorTable = {
    val calciteConfig = config.getCalciteConfig

    calciteConfig.getSqlOperatorTable match {
      case None =>
        chainedFunctionCatalog.getSqlOperatorTable
      case Some(table) =>
        if (calciteConfig.replacesSqlOperatorTable) {
          table
        } else {
          ChainedSqlOperatorTable.of(chainedFunctionCatalog.getSqlOperatorTable, table)
        }
    }
  }

  /**
    * Returns the SQL parser config for this environment including a custom Calcite configuration.
    */
  protected def getSqlParserConfig: SqlParser.Config = {
    val calciteConfig = config.getCalciteConfig
    calciteConfig.getSqlParserConfig match {

      case None =>
        // we use Java lex because back ticks are easier than double quotes in programming
        // and cases are preserved
        SqlParser
          .configBuilder()
          .setLex(Lex.JAVA)
          .setIdentifierMaxLength(256)
          .build()

      case Some(sqlParserConfig) =>
        sqlParserConfig
    }
  }

  /**
    * Returns the SqlToRelConverter config.
    */
  @Internal
  protected def getSqlToRelConverterConfig: SqlToRelConverter.Config =
    SqlToRelConverter.configBuilder()
      .withTrimUnusedFields(false)
      .withConvertTableAccess(false)
      .build()

  def getCatalogManager: CatalogManager = {
    catalogManager
  }

  /**
    * Registers an [[ReadableCatalog]] under a unique name in the TableEnvironment's schema.
    * All tables registered in the [[ReadableCatalog]] can be accessed.
    *
    * @param name            The name under which the catalog will be registered
    * @param catalog         The catalog to register
    */
  def registerCatalog(name: String, catalog: ReadableCatalog): Unit = {
    registerCatalogInternal(name, catalog)
  }

  /**
    * Registers an [[ReadableCatalog]] under a unique name in the TableEnvironment's schema.
    * All tables registered in the [[ReadableCatalog]] can be accessed.
    *
    * @param name            The name under which the catalog will be registered
    * @param catalog         The catalog to register
    *
    */
  @throws[CatalogAlreadyExistException]
  def registerCatalogInternal(
    name: String,
    catalog: ReadableCatalog): Unit = {

    catalogManager.registerCatalog(name, catalog)
  }

  /**
    * Get a registered catalog.
    *
    * @param catalogName
    * @return ReadableCatalog
    */
  @throws[CatalogNotExistException]
  def getCatalog(catalogName: String): ReadableCatalog = {
    catalogManager.getCatalog(catalogName)
  }

  /**
    * Get the default registered catalog.
    *
    * @return ReadableWritableCatalog
    */
  @throws[CatalogNotExistException]
  def getDefaultCatalog(): ReadableWritableCatalog = {
    catalogManager.getCatalog(getDefaultCatalogName()).asInstanceOf[ReadableWritableCatalog]
  }

  /**
    * Get the default catalog name.
    */
  def getDefaultCatalogName(): String = {
    catalogManager.getDefaultCatalogName
  }

  /**
    * Get the default database name.
    */
  def getDefaultDatabaseName(): String = {
    catalogManager.getDefaultDatabaseName
  }

  /**
    * Set a default catalog.
    *
    * @param name            Name of the catalog
    */
  def setDefaultCatalog(name: String): Unit = {
    catalogManager.setDefaultCatalog(name)
  }

  /**
    * Set a default catalog and database.
    *
    * @param catalogName     Name of the catalog
    * @param dbName          Name of the database
    */
  def setDefaultDatabase(catalogName: String, dbName: String): Unit = {
    catalogManager.setDefaultDatabase(catalogName, dbName)
  }

  /**
    * Set the default database. If a catalog is not specified, the database is resolved relative
    * to the current default catalog.
    * Note! This method does not support setting default catalog only.
    *
    * @param dbPath         Name or path of the database
    */
  @varargs
  def setDefaultDatabase(dbPath: String*): Unit = {
    if (dbPath(0) == null || dbPath.length < 1 || dbPath.length > 2 || dbPath(0).isEmpty ||
      (dbPath.length == 2 && dbPath(1).isEmpty)) {
      throw new IllegalArgumentException(String.format("Invalid database path %s", dbPath))
    }

    val catalogName = if (dbPath.length == 1) getDefaultCatalogName() else dbPath(0)
    val dbName = if (dbPath.length == 1) dbPath(0) else dbPath(1)

    catalogManager.setDefaultDatabase(catalogName, dbName)
  }

  /**
    * Get a table catalogs.
    *
    * @param name The name of the table.
    * @return The table registered either internally or externally, None otherwise.
    */
  def getTable(name: String): Option[org.apache.calcite.schema.Table] = {
    getTable(name.split('.'))
  }

  /**
    * Get a table from catalogs.
    *
    * @param paths The paths of the table.
    * @return The table registered either internally or externally, None otherwise.
    */
  def getTable(paths: Array[String]): Option[org.apache.calcite.schema.Table] = {
    val names = catalogManager.resolveTableName(paths : _*)
    val catalogName = names(0)
    val dbName = names(1)
    val tableName = names(2)

    val catalogSchema = catalogManager.getRootSchema.getSubSchema(catalogName)

    if (catalogSchema == null) {
      Option.empty
    } else {
      val dbSchema = catalogSchema.getSubSchema(dbName)

      if (dbSchema == null) {
        Option.empty
      } else {
        Option(dbSchema.getTable(tableName))
      }
    }
  }

  /**
    * Registers a [[ScalarFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  def registerFunction(name: String, function: ScalarFunction): Unit = {
    // check if class could be instantiated
    checkForInstantiation(function.getClass)

    // register in Table API
    functionCatalog.registerFunction(name, function.getClass)

    // register in SQL API
    functionCatalog.registerSqlFunction(
      createScalarSqlFunction(name, name, function, typeFactory)
    )
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
  def registerFunction[T, ACC](
      name: String,
      f: AggregateFunction[T, ACC])
  : Unit = {
    implicit val typeInfo: TypeInformation[T] = TypeExtractor
      .createTypeInfo(f, classOf[AggregateFunction[T, ACC]], f.getClass, 0)
      .asInstanceOf[TypeInformation[T]]

    implicit val accTypeInfo: TypeInformation[ACC] = TypeExtractor
      .createTypeInfo(f, classOf[AggregateFunction[T, ACC]], f.getClass, 1)
      .asInstanceOf[TypeInformation[ACC]]

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
  def registerFunction[T](name: String, tf: TableFunction[T]): Unit = {
    implicit val typeInfo: TypeInformation[T] =
      UserDefinedFunctionUtils.getImplicitResultTypeInfo(tf)
    registerTableFunctionInternal(name, tf)
  }

  /**
    * Registers a [[TableFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  private[flink] def registerTableFunctionInternal[T: TypeInformation](
      name: String, function: TableFunction[T]): Unit = {
    // check if class not Scala object
    checkNotSingleton(function.getClass)
    // check if class could be instantiated
    checkForInstantiation(function.getClass)
    val implicitResultType: DataType =
      // we may use arguments types to infer later on.
      if (UserDefinedFunctionUtils.getResultTypeIgnoreException(function) != null) {
      function.getResultType(null, null)
    } else {
      implicitly[TypeInformation[T]]
    }

    // register in Table API
    functionCatalog.registerFunction(name, function.getClass)

    // register in SQL API
    val sqlFunctions =
      createTableSqlFunction(name, name, function, implicitResultType, typeFactory)
    functionCatalog.registerSqlFunction(sqlFunctions)
  }

  /**
    * Registers an [[AggregateFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  private[flink] def registerAggregateFunctionInternal[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: AggregateFunction[T, ACC]): Unit = {
    // check if class not Scala object
    checkNotSingleton(function.getClass)
    // check if class could be instantiated
    checkForInstantiation(function.getClass)

    val resultType = getResultTypeOfAggregateFunction(function, implicitly[TypeInformation[T]])
    val accType = getAccumulatorTypeOfAggregateFunction(function, implicitly[TypeInformation[ACC]])

    // register in Table API
    functionCatalog.registerFunction(name, function.getClass)

    // register in SQL API
    val sqlFunctions = createAggregateSqlFunction(
      name,
      name,
      function,
      resultType,
      accType,
      typeFactory)

    functionCatalog.registerSqlFunction(sqlFunctions)
  }

  /**
    * Registers a [[Table]] under a unique name in the TableEnvironment's catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name The name under which the table will be registered.
    * @param table The table to register.
    */
  def registerTable(name: String, table: Table): Unit = {
    // check that table belongs to this table environment
    if (table.tableEnv != this) {
      throw new TableException(
        "Only tables that belong to this TableEnvironment can be registered.")
    }

    checkValidTableName(name)
    val tableTable = new RelTable(table.getRelNode)
    registerTableInternal(name, tableTable, replace = false)
  }

  /**
    * Registers a [[CatalogTable]] under a unique name in the TableEnvironment's default catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name The name under which the table will be registered.
    * @param catalogTable The table to register.
    */
  def registerTable(name: String, catalogTable: CatalogTable): Unit = {
    val catalog = getDefaultCatalog()
    val path = new ObjectPath(getDefaultDatabaseName(), name)
    catalog.createTable(path, catalogTable, false)
  }

  /**
    * Registers or replace a [[Table]] under a unique name in the TableEnvironment's catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name The name under which the table will be registered.
    * @param table The table to register.
    */
  def registerOrReplaceTable(name: String, table: Table): Unit = {
    // check that table belongs to this table environment
    if (table.tableEnv != this) {
      throw new TableException(
        "Only tables that belong to this TableEnvironment can be registered.")
    }

    checkValidTableName(name)
    val tableTable = new RelTable(table.getRelNode)
    registerTableInternal(name, tableTable, replace = true)
  }

  /**
    * Registers or replace a [[CatalogTable]] under a unique name in TableEnvironment's default
    * catalog. Registered tables can be referenced in SQL queries.
    *
    * @param name The name under which the table will be registered.
    * @param catalogTable The table to register.
    */
  def registerOrReplaceTable(name: String, catalogTable: CatalogTable): Unit = {
    val catalog = getDefaultCatalog()
    val path = new ObjectPath(getDefaultDatabaseName(),name)

    catalog.dropTable(path, true)
    catalog.createTable(path, catalogTable, false)
  }

  /**
    * Registers a Calcite [[AbstractTable]] in the TableEnvironment's catalog.
    *
    * @param name The name under which the table will be registered.
    * @param table The table to register in the catalog
    * @throws TableAlreadyExistException if another table is registered under the provided name.
    */
  @throws[TableAlreadyExistException]
  private[flink] def registerTableInternal(name: String, table: AbstractTable): Unit = {
    catalogManager.getCatalog(catalogManager.getDefaultCatalogName)
      .asInstanceOf[ReadableWritableCatalog]
      .createTable(
        new ObjectPath(catalogManager.getDefaultDatabaseName, name),
        createFlinkTempTable(table),
        false
      )
  }

  /**
    * Replaces a registered Table with another Table under the same name.
    * We use this method to replace a [[org.apache.flink.table.plan.schema.DataStreamTable]]
    * with a [[org.apache.calcite.schema.TranslatableTable]].
    *
    * @param name Name of the table to replace.
    * @param table The table that replaces the previous table.
    */
  protected def replaceRegisteredTable(name: String, table: AbstractTable): Unit = {
    catalogManager.getCatalog(catalogManager.getDefaultCatalogName)
      .asInstanceOf[ReadableWritableCatalog]
      .alterTable(
        new ObjectPath(catalogManager.getDefaultDatabaseName, name),
        createFlinkTempTable(table),
        false
      )
  }

  private def createFlinkTempTable(table: AbstractTable): FlinkTempTable = {
    val currentMillis = System.currentTimeMillis()

    return new FlinkTempTable(
      table,
      null,
      null,
      new util.HashMap[String, String](),
      null,
      null,
      null,
      new util.LinkedHashSet[String](),
      false,
      null,
      null,
      -1,
      currentMillis,
      currentMillis
    )
  }

  /**
    * Registers an external [[TableSource]] in this [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  def registerTableSource(name: String, tableSource: TableSource): Unit = {
    checkValidTableName(name)
    registerTableSourceInternal(name, tableSource, FlinkStatistic.UNKNOWN, false)
  }

  /**
    * Registers or replace an external [[TableSource]] in this [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  def registerOrReplaceTableSource(name: String,
                          tableSource: TableSource): Unit = {
    checkValidTableName(name)
    registerTableSourceInternal(name, tableSource, FlinkStatistic.UNKNOWN, true)
  }

  /**
    * Registers an internal [[TableSource]] in this [[TableEnvironment]]'s catalog without
    * name checking. Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    * @param replace     Whether to replace this [[TableSource]]
    */
  protected def registerTableSourceInternal(name: String,
                                            tableSource: TableSource,
                                            tableStats: FlinkStatistic,
                                            replace: Boolean): Unit

  /**
    * Gets the statistics of a table.
    * Note: this function returns current statistics of the table directly, does not trigger
    *       statistics gather operation.
    *
    * @param tableName The table name under which the table is registered in [[TableEnvironment]].
    *                  tableName must be a single name(e.g. "MyTable") associated with a table.
    * @return Statistics of a table if the statistics is available, else return null.
    */
  @Experimental
  def getTableStats(tableName: String): TableStats = {
    require(tableName != null && tableName.nonEmpty, "tableName must not be null or empty.")
    getTableStats(Array(tableName))
  }

  /**
    * Gets the statistics of a table.
    * Note: this function returns current statistics of the table directly, does not trigger
    *       statistics gather operation.
    *
    * @param tablePath The table name under which the table is registered in [[TableEnvironment]].
    *                  tablePath can be a single name(e.g. Array("MyTable")) associated with a
    *                  table , or can be a nest names (e.g. Array("MyCatalog", "MyDb", "MyTable"))
    *                  associated with a table registered as member of a [[ReadableCatalog]].
    * @return Statistics of a table if the statistics is available, else return null.
    */
  @Experimental
  def getTableStats(tablePath: Array[String]): TableStats = {
    val tableOpt = getTable(tablePath)
    if (tableOpt.isEmpty) {
      throw new TableException(s"Table '${tablePath.mkString(".")}' was not found.")
    }
    val table = tableOpt.get
    val tableName = tablePath.last
    val stats = if (tablePath.length == 1) {
      table match {
        case t: FlinkTable =>
          // call statistic instead of getStatistics of FlinkTable to fetch the original statistics.
          val statistics = t.getStatistic
          if (statistics == null) {
            None
          } else {
            Option(statistics.getTableStats)
          }
        // Only source table extends FlinkTable now
        case sourceSinkTable: TableSourceSinkTable[_] if sourceSinkTable.isSourceTable =>
          val statistics = sourceSinkTable.tableSourceTable.get.getStatistic
          if (statistics == null) {
            None
          } else {
            Option(statistics.getTableStats)
          }
        case _ => None
      }
    } else {
      // table in catalogs
      val path = catalogManager.resolveTableName(tablePath.toList)
      val catalog = getCatalog(path(0))

      Option(catalog.getTable(new ObjectPath(path(1), path(2))).getTableStats)
    }
    stats.orNull
  }

  /**
    *  Alters the statistics of a table.
    *
    * @param tablePath The table name under which the table is registered in [[TableEnvironment]].
    *                  tablePath can be a single name(e.g. Array("MyTable")) associated with a
    *                  table , or can be a nest names (e.g. Array("MyCatalog", "MyDb", "MyTable"))
    *                  associated with a table registered as member of a [[ReadableCatalog]].
    * @param tableStats The [[TableStats]] to update.
    */
  def alterTableStats(tablePath: Array[String], tableStats: TableStats): Unit = {
    alterTableStats(tablePath, Option(tableStats))
  }

  /**
    *  Alters the statistics of a table.
    *
    * @param tableName The table name under which the table is registered in [[TableEnvironment]].
    *                  tableName must be a single name(e.g. "MyTable") associated with a table.
    * @param tableStats The [[TableStats]] to update.
    */
  @Experimental
  def alterTableStats(tableName: String, tableStats: Option[TableStats]): Unit = {
    require(tableName != null && tableName.nonEmpty, "tableName must not be null or empty.")
    alterTableStats(Array(tableName), tableStats)
  }

  /**
    *  Alters the statistics of a table.
    *
    * @param tablePath The table name under which the table is registered in [[TableEnvironment]].
    *                  tablePath can be a single name(e.g. Array("MyTable")) associated with a
    *                  table , or can be a nest names (e.g. Array("MyCatalog", "MyDb", "MyTable"))
    *                  associated with a table registered as member of a [[ReadableCatalog]].
    * @param tableStats The [[TableStats]] to update.
    */
  @Experimental
  def alterTableStats(tablePath: Array[String], tableStats: Option[TableStats]): Unit = {
    val tableOpt = getTable(tablePath)
    if (tableOpt.isEmpty) {
      throw new TableException(s"Table '${tablePath.mkString(".")}' was not found.")
    }

    val table = tableOpt.get
    val tableName = tablePath.last
    if (tablePath.length == 1) {
      // table in calcite root schema
      val statistic = table match {
        // call statistic instead of getStatistics of TableSourceTable
        // to fetch the original statistics.
        case t: TableSourceSinkTable[_] if t.isSourceTable => t.tableSourceTable.get.statistic
        case t: FlinkTable => t.getStatistic
        case _ => throw new TableException(
          s"alter TableStats operation is not supported for ${table.getClass}.")
      }
      val oldStatistic = if (statistic == null) FlinkStatistic.UNKNOWN else statistic
      val newStatistic = FlinkStatistic.builder.statistic(oldStatistic).
                         tableStats(tableStats.orNull).build()
      val newTable = table.asInstanceOf[FlinkTable].copy(newStatistic)
      replaceRegisteredTable(tableName, newTable)
    } else {
      // table in catalogs
      val path = catalogManager.resolveTableName(tablePath.toList)
      val catalog = getCatalog(path(0))

      // TODO: [BLINK-18570617] re-enable alter catalog table stats in TableEnvironment
//      catalog match {
//        case c: ReadableWritableCatalog =>
//          c.alterTableStats(tableName, tableStats, ignoreIfNotExists = false)
//        case _ => throw new TableException(
//          s"alterTableStats operation is not supported for ${catalog.getClass}.")
//      }

      throw new TableException(
        s"catalogs haven't supportted alterTableStats operation yet.")
    }
  }

  /**
    * Alter skew info to a table, optimizer will try to choose better plan on skewed data.
    * 1. pick the skewed values to join separately
    * 2. prefer to choose add local-combine aggregate before global aggregate which group by
    * skewed data
    *
    * TODO: Add skewInfo on a specified set of columns later. Now only support to specify skewInfo
    * on a singleKey, it is not enough to determines whether a specified set of columns from a
    * specified relational expression is skew or not.
    *
    * @param tableName table name to alter.
    * @param skewInfo statistics of skewedColNames and skewedColValues.
    */
  def alterSkewInfo(
                     tableName: String,
                     skewInfo: util.Map[String, util.List[AnyRef]]): Unit = {
    require(tableName != null && tableName.nonEmpty, "tableName must not be null or empty.")
    alterSkewInfo(Array(tableName), skewInfo)
  }

  private def alterSkewInfo(
                             tablePath: Array[String],
                             skewInfo: util.Map[String, util.List[AnyRef]]): Unit = {
    val tableOpt = getTable(tablePath)
    if (tableOpt.isEmpty) {
      throw new TableException(s"Table '${tablePath.mkString(".")}' was not found.")
    }

    val table = tableOpt.get
    val tableName = tablePath.last
    if (tablePath.length == 1) {
      // table in calcite root schema
      val statistic = table match {
        // call statistic instead of getStatistics of TableSourceTable
        // to fetch the original statistics.
        case t: TableSourceSinkTable[_] if t.isSourceTable => t.tableSourceTable.get.statistic
        case t: FlinkTable => t.getStatistic
        case _ => throw new TableException(
          s"alter SkewInfo operation is not supported for ${table.getClass}.")
      }
      val oldStatistic = if (statistic == null) FlinkStatistic.UNKNOWN else statistic
      val newStatistic = FlinkStatistic.builder.statistic(oldStatistic).skewInfo(skewInfo).build()
      val newTable = table.asInstanceOf[FlinkTable].copy(newStatistic)
      replaceRegisteredTable(tableName, newTable)
    } else {
      throw new TableException("alterSkewInfo operation is not supported for external catalog.")
    }
  }

  /**
    * Registers an external [[TableSink]] with given field names and types in this
    * [[TableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * @param name The name under which the [[TableSink]] is registered.
    * @param fieldNames The field names to register with the [[TableSink]].
    * @param fieldTypes The field types to register with the [[TableSink]].
    * @param tableSink The [[TableSink]] to register.
    */
  def registerTableSink(
      name: String,
      fieldNames: Array[String],
      fieldTypes: Array[DataType],
      tableSink: TableSink[_]): Unit = {
    registerTableSinkInternal(name, fieldNames, fieldTypes, tableSink, false)
  }

  /**
    * Registers or replace an external [[TableSink]] with given field names and types in this
    * [[TableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * @param name The name under which the [[TableSink]] is registered.
    * @param fieldNames The field names to register with the [[TableSink]].
    * @param fieldTypes The field types to register with the [[TableSink]].
    * @param tableSink The [[TableSink]] to register.
    */
  def registerOrReplaceTableSink(
                         name: String,
                         fieldNames: Array[String],
                         fieldTypes: Array[DataType],
                         tableSink: TableSink[_]): Unit = {
    registerTableSinkInternal(name, fieldNames, fieldTypes, tableSink, true)
  }

  /**
    * Registers or replace an external [[TableSink]] with given field names and types in this
    * [[TableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * @param name The name under which the [[TableSink]] is registered.
    * @param fieldNames The field names to register with the [[TableSink]].
    * @param fieldTypes The field types to register with the [[TableSink]].
    * @param tableSink The [[TableSink]] to register.
    * @param replace Whether replace this [[TableSink]].
    */
  protected def registerTableSinkInternal(
       name: String,
       fieldNames: Array[String],
       fieldTypes: Array[DataType],
       tableSink: TableSink[_],
       replace: Boolean): Unit

  /**
    * Registers an external [[TableSink]] with already configured field names and field types in
    * this [[TableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * @param name The name under which the [[TableSink]] is registered.
    * @param configuredSink The configured [[TableSink]] to register.
    */
  def registerTableSink(name: String, configuredSink: TableSink[_]): Unit = {
    registerTableSinkInternal(name, configuredSink, false)
  }

  /**
    * Registers or replace an external [[TableSink]] with already configured field names and
    * field types in this [[TableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * @param name The name under which the [[TableSink]] is registered.
    * @param configuredSink The configured [[TableSink]] to register.
    */
  def registerOrReplaceTableSink(name: String, configuredSink: TableSink[_]): Unit = {
    registerTableSinkInternal(name, configuredSink, true)
  }

  /**
    * Registers an external [[TableSink]] with already configured field names and field types in
    * this [[TableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * @param name The name under which the [[TableSink]] is registered.
    * @param configuredSink The configured [[TableSink]] to register.
    */
  protected def registerTableSinkInternal(name: String,
                                          configuredSink: TableSink[_],
                                          replace: Boolean): Unit


  private[flink] def getStateTableNameForWrite(name: String): String = {
    s"__W_$name"
  }

  private[flink] def collect[T](
      table: Table,
      sink: CollectTableSink[T],
      jobName: Option[String]): Seq[T] = {
    throw new TableException(s"collect is not supported.")
  }

  /**
    * Scans a registered table and returns the resulting [[Table]].
    *
    * A table to scan must be registered in the catalog. It can be either directly
    * registered as DataStream, DataSet, or Table or as member of an [[ReadableCatalog]].
    *
    * Examples:
    *
    * - Scanning a directly registered table
    * {{{
    *   val tab: Table = tableEnv.scan("tableName")
    * }}}
    *
    * - Scanning a table from a registered catalog
    * {{{
    *   val tab: Table = tableEnv.scan("catalogName", "dbName", "tableName")
    * }}}
    *
    * @param tablePath The path of the table to scan.
    * @throws TableException if no table is found using the given table path.
    * @return The resulting [[Table]].
    */
  @throws[TableException]
  @varargs
  def scan(tablePath: String*): Table = {
    scanInternal(catalogManager.resolveTableName(tablePath.toArray : _*)) match {
      case Some(table) => table
      case None => throw new TableException(s"Table '${tablePath.mkString(".")}' was not found.")
    }
  }

  private[flink] def scanInternal(tablePath: Array[String]): Option[Table] = {
    val tableOpt = getTable(tablePath)
    if (tableOpt.nonEmpty) {
      Some(new Table(this, CatalogNode(tablePath, tableOpt.get.getRowType(typeFactory))))
    } else {
      None
    }
  }

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
  def connect(connectorDescriptor: ConnectorDescriptor): TableDescriptor

  private def getSchema(schemaPath: Array[String]): SchemaPlus = {
    var schema = currentSchema
    for (schemaName <- schemaPath) {
      schema = schema.getSubSchema(schemaName)
      if (schema == null) {
        return schema
      }
    }
    schema
  }

  /**
    * Gets the names of all catalogs registered in this environment.
    *
    * @return A list of the names of all registered catalogs.
    */
  def listCatalogs(): Array[String] = {
    catalogManager.getCatalogs.asScala.toArray
  }

  /**
    * Gets the names of all databases registered in the default catalog.
    *
    * @return A list of the names of all registered databases.
    */
  def listDatabases(): Array[String] = {
    catalogManager.getDefaultCatalog.listDatabases().asScala.toArray
  }

  /**
    * Gets the names of all tables registered in the default database.
    *
    * @return A list of the names of all registered tables.
    */
  def listTables(): Array[String] = {
    catalogManager.getDefaultCatalog.listTables(catalogManager.getDefaultDatabaseName)
      .map(op => op.getObjectName)
      .toArray
  }

  /**
    * Gets the names of all functions registered in this environment.
    */
  def listUserDefinedFunctions(): Array[String] = {
    chainedFunctionCatalog.getSqlOperatorTable.getOperatorList.map(e => e.getName).toArray
  }

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    */
  def explain(table: Table): String

  /**
    * Explain the whole plan only when subsection optimization is supported, and returns the AST
    * of the specified Table API and SQL queries and the execution plan.
    *
    * @param extended Flag to include detailed optimizer estimates.
    */
  def explain(extended: Boolean = false): String

  /**
    * Evaluates a SQL query on registered tables and retrieves the result as a [[Table]].
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[Table]] is automatically registered when its [[toString]] method is called, for example
    * when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[Table]] as follows:
    *
    * {{{
    *   val table: Table = ...
    *   // the table is not registered to the table environment
    *   tEnv.sqlQuery(s"SELECT * FROM $table")
    * }}}
    *
    * @param query The SQL query to evaluate.
    * @return The result of the query as Table
    */
  def sqlQuery(query: String): Table = {
    // parse the sql query
    val parsed = flinkPlanner.parse(query)
    if (null != parsed && parsed.getKind.belongsTo(SqlKind.QUERY)) {
      // validate the sql query
      val validated = flinkPlanner.validate(parsed)
      // transform to a relational tree
      val relational = flinkPlanner.rel(validated)
      new Table(this, LogicalRelNode(relational.project()))
    } else {
      throw new TableException(
        "Unsupported SQL query! sqlQuery() only accepts SQL queries of type " +
          "SELECT, UNION, INTERSECT, EXCEPT, VALUES, and ORDER_BY.")
    }
  }

  /**
    * Returns specific FlinkCostFactory of TableEnvironment's subclass.
    */
  protected def getFlinkCostFactory: FlinkCostFactory

  /**
    * Evaluates a SQL statement such as INSERT, UPDATE or DELETE; or a DDL statement;
    * NOTE: Currently only SQL INSERT statements are supported.
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[Table]] is automatically registered when its [[toString]] method is called, for example
    * when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[Table]] as follows:
    *
    * {{{
    *   // register the table sink into which the result is inserted.
    *   tEnv.registerTableSink("sinkTable", fieldNames, fieldsTypes, tableSink)
    *   val sourceTable: Table = ...
    *   // sourceTable is not registered to the table environment
    *   tEnv.sqlUpdate(s"INSERT INTO sinkTable SELECT * FROM $sourceTable")
    * }}}
    *
    * @param stmt The SQL statement to evaluate.
    */
  def sqlUpdate(stmt: String): Unit = {
    // parse the sql query
    val parsed = flinkPlanner.parse(stmt)
    parsed match {
      case insert: SqlInsert =>
        if (insert.getTargetTable.isInstanceOf[SqlIdentifier] &&
            insert.getTargetTable.asInstanceOf[SqlIdentifier].toString.equals("console") &&
            getTable("console").isEmpty) {
          val source = flinkPlanner.validate(insert.getSource)
          val queryResult = new Table(this, LogicalRelNode(flinkPlanner.rel(source).rel))
          val schema = queryResult.getSchema
          val printTableSink = new PrintTableSink(getConfig.getTimeZone).configure(
            schema.getColumnNames, schema.getTypes.asInstanceOf[Array[DataType]])
          writeToSink(queryResult, printTableSink, "console")
          return
        }
        // validate the insert sql
        val validated = flinkPlanner.validate(insert)
        // transform to a relational tree
        val relational:LogicalTableModify = flinkPlanner.rel(validated).rel
            .asInstanceOf[LogicalTableModify]

        // get query result as Table
        val queryResult = new Table(this, LogicalRelNode(relational.getInput(0)))

        // get name of sink table
        val targetTable = relational.getTable

        // set emit configs
        val emit = insert.getEmit
        if (emit != null && this.isInstanceOf[StreamTableEnvironment]) {
          if (emit.getBeforeDelayValue >= 0) {
            getConfig.withEarlyFireInterval(Time.milliseconds(emit.getBeforeDelayValue))
          }
          if (emit.getAfterDelayValue >= 0) {
            getConfig.withLateFireInterval(Time.milliseconds(emit.getAfterDelayValue))
          }
        }

        // insert query result into sink table
        insertInto(
            queryResult,
            targetTable.unwrap(classOf[schema.Table]),
            StringUtils.join(targetTable.getQualifiedName, ","))
      case _ =>
        throw new TableException(
          "Unsupported SQL query! sqlUpdate() only accepts SQL statements of type INSERT.")
    }
  }

  /**
    * Evaluates a SQL statement such as INSERT, UPDATE or DELETE; or a DDL statement;
    * NOTE: Currently only SQL INSERT statements are supported.
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[Table]] is automatically registered when its [[toString]] method is called, for example
    * when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[Table]] as follows:
    *
    * {{{
    *   // register the table sink into which the result is inserted.
    *   tEnv.registerTableSink("sinkTable", fieldNames, fieldsTypes, tableSink)
    *   val sourceTable: Table = ...
    *   // sourceTable is not registered to the table environment
    *   tEnv.sqlUpdate(s"INSERT INTO sinkTable SELECT * FROM $sourceTable")
    * }}}
    *
    * @param stmt The SQL statement to evaluate.
    * @param config The [[QueryConfig]] to use.
    */
  def sqlUpdate(stmt: String, config: QueryConfig): Unit = {
    config.overrideTableConfig(getConfig)
    sqlUpdate(stmt)
  }

  /**
    * Writes a [[Table]] to a [[TableSink]].
    *
    * @param table The [[Table]] to write.
    * @param sink The [[TableSink]] to write the [[Table]] to.
    * @tparam T The data type that the [[TableSink]] expects.
    */
  private[table] def writeToSink[T](
      table: Table,
      sink: TableSink[T],
      sinkName: String = null): Unit

  /**
    * Triggers the program execution.
    */
  def execute(): JobExecutionResult = execute(DEFAULT_JOB_NAME)

  /**
    * Triggers the program execution with jobName.
    */
  def execute(jobName: String): JobExecutionResult

  /**
    * Writes the [[Table]] to a [[TableSink]] that was registered under the specified name.
    *
    * @param table The table to write to the TableSink.
    * @param sinkTableName The name of the registered TableSink.
    */
  private[flink] def insertInto(table: Table, sinkTableName: String): Unit = {

    // check that sink table exists
    if (null == sinkTableName || sinkTableName.isEmpty) {
      throw new TableException(TableErrors.INST.sqlInvalidSinkTblName())
    }

    if (!catalogManager.getDefaultCatalog()
      .tableExists(new ObjectPath(catalogManager.getDefaultDatabaseName, sinkTableName))) {
      throw new TableException(TableErrors.INST.sqlTableNotRegistered(sinkTableName))
    }
    val targetTable = getTable(sinkTableName).get

    insertInto(table, targetTable, sinkTableName)
  }

  private def insertInto(
      sourceTable: Table,
      targetTable: schema.Table,
      targetTableName: String) = {
    val tableSink = targetTable match {
      case s: CatalogCalciteTable => s.tableSink
      case s: TableSinkTable[_] => s.tableSink
      case s: TableSourceSinkTable[_] if s.tableSinkTable.isDefined =>
        s.tableSinkTable.get.tableSink
      case _ =>
        throw new TableException(TableErrors.INST.sqlNotTableSinkError(targetTableName))

    }

    // validate schema of source table and table sink
    val srcFieldTypes = sourceTable.getSchema.getTypes
    val sinkFieldTypes = tableSink.getFieldTypes.map(_.toInternalType)

    val srcFieldNames = sourceTable.getSchema.getColumnNames
    val sinkFieldNames = tableSink.getFieldNames

    val srcNameTypes = srcFieldNames.zip(srcFieldTypes)
    val sinkNameTypes = sinkFieldNames.zip(sinkFieldTypes)

    def typeMatch(t1: InternalType, t2: InternalType): Boolean = {
      t1 == t2 ||
        (t1.isInstanceOf[DateType] && t2.isInstanceOf[DateType]) ||
        (t1.isInstanceOf[TimestampType] && t2.isInstanceOf[TimestampType])

    }

    if (srcFieldTypes.length != sinkFieldTypes.length) {
      // format table and table sink schema strings
      val srcSchema = srcNameTypes
        .map { case (n, t) => s"$n: ${TypeUtils.getExternalClassForType(t)}" }
        .mkString("[", ", ", "]")

      val sinkSchema = sinkNameTypes
        .map { case (n, t) => s"$n: ${TypeUtils.getExternalClassForType(t)}" }
        .mkString("[", ", ", "]")

      throw new ValidationException(
        TableErrors.INST.sqlInsertIntoMismatchedFieldLen(
          targetTableName, srcSchema, sinkSchema))
    } else if (srcFieldTypes.zip(sinkFieldTypes)
      .exists {
        case (_: GenericType[_], _: GenericType[_]) => false
        case (srcF, snkF) => !typeMatch(srcF, snkF)
      }
    ) {
      val diffNameTypes = srcNameTypes.zip(sinkNameTypes)
        .filter {
          case ((_, srcType), (_, sinkType)) => !typeMatch(srcType, sinkType)
        }
      val srcDiffMsg = diffNameTypes
        .map(_._1)
        .map { case (n, t) => s"$n: ${TypeUtils.getExternalClassForType(t)}" }
        .mkString("[", ", ", "]")
      val sinkDiffMsg = diffNameTypes
        .map(_._2)
        .map { case (n, t) => s"$n: ${TypeUtils.getExternalClassForType(t)}" }
        .mkString("[", ", ", "]")

      throw new ValidationException(
        TableErrors.INST.sqlInsertIntoMismatchedFieldTypes(
          targetTableName, srcDiffMsg, sinkDiffMsg))
    }

    // emit the table to the configured table sink
    writeToSink(sourceTable, tableSink, targetTableName)
  }

  /**
    * Registers a Calcite [[AbstractTable]] in the TableEnvironment's catalog.
    *
    * @param name The name under which the table will be registered.
    * @param table The table to register in the catalog.
    * @param replace Whether to replace the registered table.
    * @throws TableException if another table is registered under the provided name.
    */
  @throws[TableException]
  protected def registerTableInternal(name: String,
                                      table: AbstractTable,
                                      replace: Boolean): Unit = {
    if (replace) {
      catalogManager.getCatalog(catalogManager.getDefaultCatalogName)
        .asInstanceOf[ReadableWritableCatalog]
        .dropTable(new ObjectPath(catalogManager.getDefaultDatabaseName, name), true)
    }
    catalogManager.getCatalog(catalogManager.getDefaultCatalogName)
      .asInstanceOf[ReadableWritableCatalog]
      .createTable(
        new ObjectPath(catalogManager.getDefaultDatabaseName, name),
        createFlinkTempTable(table),
        false
      )
  }

  /**
    * Checks if the chosen table name is valid.
    *
    * @param name The table name to check.
    */
  protected def checkValidTableName(name: String): Unit = {}

  /**
    * Close the table environment. This method will clean up the internal state and background
    * services. Users should invoke this method if possible to avoid resource leak.
    */
  def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      tableServiceManager.close()
    }
  }

  /** Returns a unique temporary attribute name. */
  private[flink] def createUniqueAttributeName(prefix: String): String = {
    prefix + attrNameCntr.getAndIncrement()
  }

  /** Returns a unique temporary attribute name. */
  private[flink] def createUniqueAttributeName(): String = {
    "TMP_" + attrNameCntr.getAndIncrement()
  }

  /** Returns a unique table name according to the internal naming pattern. */
  private[flink] def createUniqueTableName(): String = {
    var res = tableNamePrefix + tableNameCntr.getAndIncrement()
    while (getTable(res).nonEmpty) {
      res = tableNamePrefix + tableNameCntr.getAndIncrement()
    }
    res
  }

  /** Returns the [[FlinkRelBuilder]] of this TableEnvironment. */
  private[flink] def getRelBuilder: FlinkRelBuilder = {
    relBuilder
  }

  /** Returns the Calcite [[org.apache.calcite.plan.RelOptPlanner]] of this TableEnvironment. */
  private[flink] def getPlanner: RelOptPlanner = {
    planner
  }

  /** Returns the [[FlinkTypeFactory]] of this TableEnvironment. */
  private[flink] def getTypeFactory: FlinkTypeFactory = {
    typeFactory
  }

  /** Returns the chained [[FunctionCatalog]]. */
  private[flink] def getFunctionCatalog: FunctionCatalog = {
    chainedFunctionCatalog
  }

  private def createFrameworkConfig: FrameworkConfig = {
    Frameworks
      .newConfigBuilder
      .defaultSchema(currentSchema)
      .parserConfig(getSqlParserConfig)
      .costFactory(getFlinkCostFactory)
      .operatorTable(getSqlOperatorTable)
      // set the executor to evaluate constant expressions
      .executor(new ExpressionReducer(config))
      .context(FlinkChainContext.chain(Contexts.of(config), Contexts.of(chainedFunctionCatalog)))
      .build
  }

  /** Returns the Calcite [[FrameworkConfig]] of this TableEnvironment. */
  private[flink] def getFrameworkConfig: FrameworkConfig = {
    frameworkConfig
  }

  protected def createRelBuilder: FlinkRelBuilder = {
    FlinkRelBuilder.create(
      frameworkConfig, config, getTypeFactory, catalogManager = catalogManager)
  }

  private def createRelOptPlanner: RelOptPlanner = {
    relBuilder.getPlanner
  }

  private def createFlinkPlanner: FlinkPlannerImpl = {
    new FlinkPlannerImpl(
      getFrameworkConfig,
      getPlanner,
      getTypeFactory,
      sqlToRelConverterConfig,
      relBuilder.getCluster,
      catalogManager)
  }

   /**
    * Reference input fields by name:
    * All fields in the schema definition are referenced by name
    * (and possibly renamed using an alias (as). In this mode, fields can be reordered and
    * projected out. Moreover, we can define proctime and rowtime attributes at arbitrary
    * positions using arbitrary names (except those that exist in the result schema). This mode
    * can be used for any input type, including POJOs.
    *
    * Reference input fields by position:
    * In this mode, fields are simply renamed. Event-time attributes can
    * replace the field on their position in the input data (if it is of correct type) or be
    * appended at the end. Proctime attributes must be appended at the end. This mode can only be
    * used if the input type has a defined field order (tuple, case class, Row) and no of fields
    * references a field of the input type.
    */
  protected def isReferenceByPosition(ct: RowType, fields: Array[Expression]): Boolean = {

    val inputNames = ct.getFieldNames

    // Use the by-position mode if no of the fields exists in the input.
    // This prevents confusing cases like ('f2, 'f0, 'myName) for a Tuple3 where fields are renamed
    // by position but the user might assume reordering instead of renaming.
    fields.forall {
      case UnresolvedFieldReference(name) => !inputNames.contains(name)
      case Alias(_, _, _) => false
      case _ => true
    }
  }

  /**
    * Returns field names and field positions for a given [[TypeInformation]].
    *
    * @param inputType The DataType extract the field names and positions from.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  protected[flink] def getFieldInfo(inputType: DataType):
  (Array[String], Array[Int]) = {

    (TableEnvironment.getFieldNames(inputType), TableEnvironment.getFieldIndices(inputType))
  }

  /**
    * Returns field names and field positions for a given [[TypeInformation]] and [[Array]] of
    * [[Expression]]. It does not handle time attributes but considers them in indices.
    *
    * @param inputType The [[DataType]] against which the [[Expression]]s are evaluated.
    * @param exprs     The expressions that define the field names.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  protected[flink] def getFieldInfo[A](
      inputType: DataType,
      exprs: Array[Expression])
    : (Array[String], Array[Int]) = {

    TableEnvironment.validateType(inputType)

    def referenceByName(name: String, ct: RowType): Option[Int] = {
      val inputIdx = ct.getFieldIndex(name)
      if (inputIdx < 0) {
        throw new TableException(s"$name is not a field of type $ct. " +
                s"Expected: ${ct.getFieldNames.mkString(", ")}. " +
            s"Make sure there is no field in physical data type referred " +
            s"if you want to refer field by position.")
      } else {
        Some(inputIdx)
      }
    }

    val indexedNames: Array[(Int, String)] = inputType.toInternalType match {

      case t: RowType =>

        val isRefByPos = isReferenceByPosition(t, exprs)
        exprs.zipWithIndex flatMap {
          case (UnresolvedFieldReference(name: String), idx) =>
            if (isRefByPos) {
              Some((idx, name))
            } else {
              referenceByName(name, t).map((_, name))
            }
          case (Alias(UnresolvedFieldReference(origName), name: String, _), _) =>
            if (isRefByPos) {
              throw new TableException(
                s"Alias '$name' is not allowed if other fields are referenced by position.")
            } else {
              referenceByName(origName, t).map((_, name))
            }
          case (_: TimeAttribute, _) =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }

      case _: InternalType => // atomic or other custom type information
        var referenced = false
        exprs flatMap {
          case _: TimeAttribute =>
            None
          case UnresolvedFieldReference(_) if referenced =>
            // only accept the first field for an atomic type
            throw new TableException("Only the first field can reference an atomic type.")
          case UnresolvedFieldReference(name: String) =>
            referenced = true
            // first field reference is mapped to atomic type
            Some((0, name))
          case _ => throw new TableException(
            "Field reference expression expected.")
        }
    }

    val (fieldIndexes, fieldNames) = indexedNames.unzip

    if (fieldNames.contains("*")) {
      throw new TableException("Field name can not be '*'.")
    }

    (fieldNames, fieldIndexes)
  }

  def setUserClassLoader(userClassLoader: ClassLoader): Unit = {
    this.userClassloader = userClassLoader
  }
}

/**
  * Object to instantiate a [[TableEnvironment]] depending on the batch or stream execution
  * environment.
  */
object TableEnvironment {

  /**
    * The key for external catalog
    */
  val DEFAULT_SCHEMA: String = "hive"

  /**
    * Returns a [[BatchTableEnvironment]] for a Java [[JavaStreamExecEnv]].
    *
    * @param executionEnvironment The Java batch ExecutionEnvironment.
    */
  def getBatchTableEnvironment(
      executionEnvironment: JavaStreamExecEnv): JavaBatchTableEnvironment = {
    new JavaBatchTableEnvironment(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[BatchTableEnvironment]] for a Java [[JavaStreamExecEnv]] and a given
    * [[TableConfig]].
    *
    * @param executionEnvironment The Java batch ExecutionEnvironment.
    * @param tableConfig          The TableConfig for the new TableEnvironment.
    */
  def getBatchTableEnvironment(
      executionEnvironment: JavaStreamExecEnv,
      tableConfig: TableConfig): JavaBatchTableEnvironment = {
    new JavaBatchTableEnvironment(executionEnvironment, tableConfig)
  }

  /**
    * Returns a [[ScalaBatchTableEnvironment]] for a Scala stream [[ScalaStreamExecEnv]].
    *
    * @param executionEnvironment The Scala StreamExecutionEnvironment.
    */
  def getBatchTableEnvironment(
      executionEnvironment: ScalaStreamExecEnv): ScalaBatchTableEnvironment = {
    new ScalaBatchTableEnvironment(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[ScalaBatchTableEnvironment]] for a Scala stream [[ScalaStreamExecEnv]].
    *
    * @param executionEnvironment The Scala StreamExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    */
  def getBatchTableEnvironment(
      executionEnvironment: ScalaStreamExecEnv,
      tableConfig: TableConfig): ScalaBatchTableEnvironment = {

    new ScalaBatchTableEnvironment(executionEnvironment, tableConfig)
  }

  /**
    * Returns a [[JavaStreamTableEnv]] for a Java [[JavaStreamExecEnv]].
    *
    * @param executionEnvironment The Java StreamExecutionEnvironment.
    */
  def getTableEnvironment(executionEnvironment: JavaStreamExecEnv): JavaStreamTableEnv = {
    new JavaStreamTableEnv(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[JavaStreamTableEnv]] for a Java [[JavaStreamExecEnv]] and a given [[TableConfig]].
    *
    * @param executionEnvironment The Java StreamExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    */
  def getTableEnvironment(
    executionEnvironment: JavaStreamExecEnv,
    tableConfig: TableConfig): JavaStreamTableEnv = {

    new JavaStreamTableEnv(executionEnvironment, tableConfig)
  }

  /**
    * Returns a [[ScalaStreamTableEnv]] for a Scala stream [[ScalaStreamExecEnv]].
    *
    * @param executionEnvironment The Scala StreamExecutionEnvironment.
    */
  def getTableEnvironment(executionEnvironment: ScalaStreamExecEnv): ScalaStreamTableEnv = {
    new ScalaStreamTableEnv(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[ScalaStreamTableEnv]] for a Scala stream [[ScalaStreamExecEnv]].
    *
    * @param executionEnvironment The Scala StreamExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    */
  def getTableEnvironment(
    executionEnvironment: ScalaStreamExecEnv,
    tableConfig: TableConfig): ScalaStreamTableEnv = {

    new ScalaStreamTableEnv(executionEnvironment, tableConfig)
  }

  /**
    * Validate if class represented by the typeInfo is static and globally accessible
    * @param t type to check
    * @throws TableException if type does not meet these criteria
    */
  def validateType(t: DataType): Unit = {
    val clazz = TypeUtils.getExternalClassForType(t)
    if ((clazz.isMemberClass && !Modifier.isStatic(clazz.getModifiers)) ||
      !Modifier.isPublic(clazz.getModifiers) ||
      clazz.getCanonicalName == null) {
      throw new TableException(s"Class '$clazz' described in type information '$t' must be " +
        s"static and globally accessible.")
    }
  }

  /**
    * Return rowType of tableSink. [[UpsertStreamTableSink]] and [[RetractStreamTableSink]] should
    * return recordType, others return outputType.
    * @param tableSink
    * @tparam A
    * @return
    */
  def getRowTypeForTableSink[A](tableSink: TableSink[A]): DataType = {
    tableSink match {
      case u: UpsertStreamTableSink[A] => u.getRecordType
      case r: RetractStreamTableSink[A] => r.getRecordType
      case _ => tableSink.getOutputType
    }
  }

   /**
    * Returns field names for a given [[TypeInformation]].
    *
    * @param inputType The DataType extract the field names.
    * @return An array holding the field names
    */
  def getFieldNames(inputType: DataType): Array[String] = {
    validateType(inputType)

    val fieldNames: Array[String] = inputType.toInternalType match {
      case t: RowType => t.getFieldNames
      case _: InternalType => Array("f0")
    }

    if (fieldNames.contains("*")) {
      throw new TableException("Field name can not be '*'.")
    }

    fieldNames
  }

  /**
    * Returns field indexes for a given [[TypeInformation]].
    *
    * @param inputType The DataType extract the field positions from.
    * @return An array holding the field positions
    */
  def getFieldIndices(inputType: DataType): Array[Int] = {
    getFieldNames(inputType).indices.toArray
  }

  /**
    * Returns field types for a given [[TypeInformation]].
    *
    * @param inputType The DataType to extract field types from.
    * @return An array holding the field types.
    */
  def getFieldTypes(inputType: DataType): Array[InternalType] = {
    validateType(inputType)

    inputType.toInternalType match {
      case ct: RowType =>
        0.until(ct.getArity).map(i => ct.getInternalTypeAt(i).toInternalType).toArray
      case t: InternalType => Array(t)
    }
  }

}
