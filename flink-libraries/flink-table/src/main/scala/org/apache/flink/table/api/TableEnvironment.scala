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

import _root_.java.lang.reflect.Modifier
import _root_.java.util.concurrent.atomic.AtomicInteger

import com.google.common.collect.ImmutableList
import org.apache.calcite.config.Lex
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan.RelOptPlanner.CannotPlanException
import org.apache.calcite.plan.hep.{HepMatchOrder, HepPlanner, HepProgramBuilder}
import org.apache.calcite.plan.{RelOptPlanner, RelOptUtil, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql._
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.util.ChainedSqlOperatorTable
import org.apache.calcite.tools._
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{RowTypeInfo, _}
import org.apache.flink.api.java.{ExecutionEnvironment => JavaBatchExecEnv}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.scala.{ExecutionEnvironment => ScalaBatchExecEnv}
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaStreamExecEnv}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment => ScalaStreamExecEnv}
import org.apache.flink.table.api.java.{BatchTableEnvironment => JavaBatchTableEnv, StreamTableEnvironment => JavaStreamTableEnv}
import org.apache.flink.table.api.scala.{BatchTableEnvironment => ScalaBatchTableEnv, StreamTableEnvironment => ScalaStreamTableEnv}
import org.apache.flink.table.calcite.{FlinkPlannerImpl, FlinkRelBuilder, FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.catalog.{ExternalCatalog, ExternalCatalogSchema}
import org.apache.flink.table.codegen.{ExpressionReducer, FunctionCodeGenerator, GeneratedFunction}
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.plan.cost.DataSetCostFactory
import org.apache.flink.table.plan.logical.{CatalogNode, LogicalRelNode}
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.apache.flink.table.plan.schema.{RelTable, RowSchema, TableSinkTable}
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.table.validate.FunctionCatalog
import org.apache.flink.types.Row

import _root_.scala.annotation.varargs
import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable

/**
  * The abstract base class for batch and stream TableEnvironments.
  *
  * @param config The configuration of the TableEnvironment
  */
abstract class TableEnvironment(val config: TableConfig) {

  // the catalog to hold all registered and translated tables
  // we disable caching here to prevent side effects
  private val internalSchema: CalciteSchema = CalciteSchema.createRootSchema(true, false)
  private val rootSchema: SchemaPlus = internalSchema.plus()

  // Table API/SQL function catalog
  private[flink] val functionCatalog: FunctionCatalog = FunctionCatalog.withBuiltIns

  // the configuration to create a Calcite planner
  private lazy val frameworkConfig: FrameworkConfig = Frameworks
    .newConfigBuilder
    .defaultSchema(rootSchema)
    .parserConfig(getSqlParserConfig)
    .costFactory(new DataSetCostFactory)
    .typeSystem(new FlinkTypeSystem)
    .operatorTable(getSqlOperatorTable)
    // set the executor to evaluate constant expressions
    .executor(new ExpressionReducer(config))
    .build

  // the builder for Calcite RelNodes, Calcite's representation of a relational expression tree.
  protected lazy val relBuilder: FlinkRelBuilder = FlinkRelBuilder.create(frameworkConfig)

  // the planner instance used to optimize queries of this TableEnvironment
  private lazy val planner: RelOptPlanner = relBuilder.getPlanner

  private lazy val typeFactory: FlinkTypeFactory = relBuilder.getTypeFactory

  // a counter for unique attribute names
  private[flink] val attrNameCntr: AtomicInteger = new AtomicInteger(0)

  // registered external catalog names -> catalog
  private val externalCatalogs = new mutable.HashMap[String, ExternalCatalog]

  /** Returns the table config to define the runtime behavior of the Table API. */
  def getConfig: TableConfig = config

  /** Returns the [[QueryConfig]] depends on the concrete type of this TableEnvironment. */
  private[flink] def queryConfig: QueryConfig = this match {
    case _: BatchTableEnvironment => new BatchQueryConfig
    case _: StreamTableEnvironment => new StreamQueryConfig
    case _ => null
  }

  /**
    * Returns the operator table for this environment including a custom Calcite configuration.
    */
  protected def getSqlOperatorTable: SqlOperatorTable = {
    val calciteConfig = config.getCalciteConfig
    calciteConfig.getSqlOperatorTable match {

      case None =>
        functionCatalog.getSqlOperatorTable

      case Some(table) =>
        if (calciteConfig.replacesSqlOperatorTable) {
          table
        } else {
          ChainedSqlOperatorTable.of(functionCatalog.getSqlOperatorTable, table)
        }
    }
  }

  /**
    * Returns the normalization rule set for this environment
    * including a custom RuleSet configuration.
    */
  protected def getNormRuleSet: RuleSet = {
    val calciteConfig = config.getCalciteConfig
    calciteConfig.getNormRuleSet match {

      case None =>
        getBuiltInNormRuleSet

      case Some(ruleSet) =>
        if (calciteConfig.replacesNormRuleSet) {
          ruleSet
        } else {
          RuleSets.ofList((getBuiltInNormRuleSet.asScala ++ ruleSet.asScala).asJava)
        }
    }
  }

  /**
    * Returns the logical optimization rule set for this environment
    * including a custom RuleSet configuration.
    */
  protected def getLogicalOptRuleSet: RuleSet = {
    val calciteConfig = config.getCalciteConfig
    calciteConfig.getLogicalOptRuleSet match {

      case None =>
        getBuiltInLogicalOptRuleSet

      case Some(ruleSet) =>
        if (calciteConfig.replacesLogicalOptRuleSet) {
          ruleSet
        } else {
          RuleSets.ofList((getBuiltInLogicalOptRuleSet.asScala ++ ruleSet.asScala).asJava)
        }
    }
  }

  /**
    * Returns the physical optimization rule set for this environment
    * including a custom RuleSet configuration.
    */
  protected def getPhysicalOptRuleSet: RuleSet = {
    val calciteConfig = config.getCalciteConfig
    calciteConfig.getPhysicalOptRuleSet match {

      case None =>
        getBuiltInPhysicalOptRuleSet

      case Some(ruleSet) =>
        if (calciteConfig.replacesPhysicalOptRuleSet) {
          ruleSet
        } else {
          RuleSets.ofList((getBuiltInPhysicalOptRuleSet.asScala ++ ruleSet.asScala).asJava)
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
          .build()

      case Some(sqlParserConfig) =>
        sqlParserConfig
    }
  }

  /**
    * Returns the built-in normalization rules that are defined by the environment.
    */
  protected def getBuiltInNormRuleSet: RuleSet

  /**
    * Returns the built-in logical optimization rules that are defined by the environment.
    */
  protected def getBuiltInLogicalOptRuleSet: RuleSet = {
    FlinkRuleSets.LOGICAL_OPT_RULES
  }

  /**
    * Returns the built-in physical optimization rules that are defined by the environment.
    */
  protected def getBuiltInPhysicalOptRuleSet: RuleSet

  /**
    * run HEP planner
    */
  protected def runHepPlanner(
    hepMatchOrder: HepMatchOrder,
    ruleSet: RuleSet,
    input: RelNode,
    targetTraits: RelTraitSet): RelNode = {
    val builder = new HepProgramBuilder
    builder.addMatchOrder(hepMatchOrder)

    val it = ruleSet.iterator()
    while (it.hasNext) {
      builder.addRuleInstance(it.next())
    }

    val planner = new HepPlanner(builder.build, frameworkConfig.getContext)
    planner.setRoot(input)
    if (input.getTraitSet != targetTraits) {
      planner.changeTraits(input, targetTraits.simplify)
    }
    planner.findBestExp
  }

  /**
    * run VOLCANO planner
    */
  protected def runVolcanoPlanner(
    ruleSet: RuleSet,
    input: RelNode,
    targetTraits: RelTraitSet): RelNode = {
    val optProgram = Programs.ofRules(ruleSet)

    val output = try {
      optProgram.run(getPlanner, input, targetTraits,
        ImmutableList.of(), ImmutableList.of())
    } catch {
      case e: CannotPlanException =>
        throw new TableException(
          s"Cannot generate a valid execution plan for the given query: \n\n" +
            s"${RelOptUtil.toString(input)}\n" +
            s"This exception indicates that the query uses an unsupported SQL feature.\n" +
            s"Please check the documentation for the set of currently supported SQL features.")
      case t: TableException =>
        throw new TableException(
          s"Cannot generate a valid execution plan for the given query: \n\n" +
            s"${RelOptUtil.toString(input)}\n" +
            s"${t.msg}\n" +
            s"Please check the documentation for the set of currently supported SQL features.")
      case a: AssertionError =>
        // keep original exception stack for caller
        throw a
    }
    output
  }

  /**
    * Creates a table from a table source.
    *
    * @param source table source used as table
    */
  def fromTableSource(source: TableSource[_]): Table = {
    val name = createUniqueTableName()
    registerTableSourceInternal(name, source)
    scan(name)
  }

  /**
    * Registers an [[ExternalCatalog]] under a unique name in the TableEnvironment's schema.
    * All tables registered in the [[ExternalCatalog]] can be accessed.
    *
    * @param name            The name under which the externalCatalog will be registered
    * @param externalCatalog The externalCatalog to register
    */
  def registerExternalCatalog(name: String, externalCatalog: ExternalCatalog): Unit = {
    if (rootSchema.getSubSchema(name) != null) {
      throw new ExternalCatalogAlreadyExistException(name)
    }
    this.externalCatalogs.put(name, externalCatalog)
    // create an external catalog Calcite schema, register it on the root schema
    ExternalCatalogSchema.registerCatalog(this, rootSchema, name, externalCatalog)
  }

  /**
    * Gets a registered [[ExternalCatalog]] by name.
    *
    * @param name The name to look up the [[ExternalCatalog]]
    * @return The [[ExternalCatalog]]
    */
  def getRegisteredExternalCatalog(name: String): ExternalCatalog = {
    this.externalCatalogs.get(name) match {
      case Some(catalog) => catalog
      case None => throw new ExternalCatalogNotExistException(name)
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
    * Registers a [[TableFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  private[flink] def registerTableFunctionInternal[T: TypeInformation](
    name: String, function: TableFunction[T]): Unit = {
    // check if class not Scala object
    checkNotSingleton(function.getClass)
    // check if class could be instantiated
    checkForInstantiation(function.getClass)

    val typeInfo: TypeInformation[_] = if (function.getResultType != null) {
      function.getResultType
    } else {
      implicitly[TypeInformation[T]]
    }

    // register in Table API
    functionCatalog.registerFunction(name, function.getClass)

    // register in SQL API
    val sqlFunction = createTableSqlFunction(name, name, function, typeInfo, typeFactory)
    functionCatalog.registerSqlFunction(sqlFunction)
  }

  /**
    * Registers an [[AggregateFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  private[flink] def registerAggregateFunctionInternal[T: TypeInformation, ACC: TypeInformation](
      name: String, function: AggregateFunction[T, ACC]): Unit = {
    // check if class not Scala object
    checkNotSingleton(function.getClass)
    // check if class could be instantiated
    checkForInstantiation(function.getClass)

    val resultTypeInfo: TypeInformation[_] = getResultTypeOfAggregateFunction(
      function,
      implicitly[TypeInformation[T]])

    val accTypeInfo: TypeInformation[_] = getAccumulatorTypeOfAggregateFunction(
      function,
      implicitly[TypeInformation[ACC]])

    // register in Table API
    functionCatalog.registerFunction(name, function.getClass)

    // register in SQL API
    val sqlFunctions = createAggregateSqlFunction(
      name,
      name,
      function,
      resultTypeInfo,
      accTypeInfo,
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
    registerTableInternal(name, tableTable)
  }

  /**
    * Registers an external [[TableSource]] in this [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  def registerTableSource(name: String, tableSource: TableSource[_]): Unit = {
    checkValidTableName(name)
    registerTableSourceInternal(name, tableSource)
  }

  /**
    * Registers an internal [[TableSource]] in this [[TableEnvironment]]'s catalog without
    * name checking. Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  protected def registerTableSourceInternal(name: String, tableSource: TableSource[_]): Unit

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
      fieldTypes: Array[TypeInformation[_]],
      tableSink: TableSink[_]): Unit

  /**
    * Replaces a registered Table with another Table under the same name.
    * We use this method to replace a [[org.apache.flink.table.plan.schema.DataStreamTable]]
    * with a [[org.apache.calcite.schema.TranslatableTable]].
    *
    * @param name Name of the table to replace.
    * @param table The table that replaces the previous table.
    */
  protected def replaceRegisteredTable(name: String, table: AbstractTable): Unit = {

    if (isRegistered(name)) {
      rootSchema.add(name, table)
    } else {
      throw new TableException(s"Table \'$name\' is not registered.")
    }
  }

  /**
    * Scans a registered table and returns the resulting [[Table]].
    *
    * A table to scan must be registered in the TableEnvironment. It can be either directly
    * registered as DataStream, DataSet, or Table or as member of an [[ExternalCatalog]].
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
    scanInternal(tablePath.toArray) match {
      case Some(table) => table
      case None => throw new TableException(s"Table '${tablePath.mkString(".")}' was not found.")
    }
  }

  private[flink] def scanInternal(tablePath: Array[String]): Option[Table] = {
    require(tablePath != null && !tablePath.isEmpty, "tablePath must not be null or empty.")
    val schemaPaths = tablePath.slice(0, tablePath.length - 1)
    val schema = getSchema(schemaPaths)
    if (schema != null) {
      val tableName = tablePath(tablePath.length - 1)
      val table = schema.getTable(tableName)
      if (table != null) {
        return Some(new Table(this, CatalogNode(tablePath, table.getRowType(typeFactory))))
      }
    }
    None
  }

  private def getSchema(schemaPath: Array[String]): SchemaPlus = {
    var schema = rootSchema
    for (schemaName <- schemaPath) {
      schema = schema.getSubSchema(schemaName)
      if (schema == null) {
        return schema
      }
    }
    schema
  }

  /**
    * Gets the names of all tables registered in this environment.
    *
    * @return A list of the names of all registered tables.
    */
  def listTables(): Array[String] = {
    rootSchema.getTableNames.asScala.toArray
  }

  /**
    * Gets the names of all functions registered in this environment.
    */
  def listUserDefinedFunctions(): Array[String] = {
    functionCatalog.getUserDefinedFunctions.toArray
  }

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    */
  def explain(table: Table): String

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
    *   tEnv.sql(s"SELECT * FROM $table")
    * }}}
    *
    * @deprecated Use sqlQuery() instead.
    * @param query The SQL query to evaluate.
    * @return The result of the query as Table.
    */
  @Deprecated
  @deprecated("Please use sqlQuery() instead.")
  def sql(query: String): Table = {
    sqlQuery(query)
  }

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
    val planner = new FlinkPlannerImpl(getFrameworkConfig, getPlanner, getTypeFactory)
    // parse the sql query
    val parsed = planner.parse(query)
    if (null != parsed && parsed.getKind.belongsTo(SqlKind.QUERY)) {
      // validate the sql query
      val validated = planner.validate(parsed)
      // transform to a relational tree
      val relational = planner.rel(validated)
      new Table(this, LogicalRelNode(relational.rel))
    } else {
      throw new TableException(
        "Unsupported SQL query! sqlQuery() only accepts SQL queries of type " +
          "SELECT, UNION, INTERSECT, EXCEPT, VALUES, and ORDER_BY.")
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
    */
  def sqlUpdate(stmt: String): Unit = {
    sqlUpdate(stmt, this.queryConfig)
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
    val planner = new FlinkPlannerImpl(getFrameworkConfig, getPlanner, getTypeFactory)
    // parse the sql query
    val parsed = planner.parse(stmt)
    parsed match {
      case insert: SqlInsert =>
        // validate the SQL query
        val query = insert.getSource
        planner.validate(query)

        // get query result as Table
        val queryResult = new Table(this, LogicalRelNode(planner.rel(query).rel))

        // get name of sink table
        val targetTableName = insert.getTargetTable.asInstanceOf[SqlIdentifier].names.get(0)

        // insert query result into sink table
        insertInto(queryResult, targetTableName, config)
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

  /**
    * Writes the [[Table]] to a [[TableSink]] that was registered under the specified name.
    *
    * @param table The table to write to the TableSink.
    * @param sinkTableName The name of the registered TableSink.
    * @param conf The query configuration to use.
    */
  private[flink] def insertInto(table: Table, sinkTableName: String, conf: QueryConfig): Unit = {

    // check that sink table exists
    if (null == sinkTableName) throw TableException("Name of TableSink must not be null.")
    if (sinkTableName.isEmpty) throw TableException("Name of TableSink must not be empty.")
    if (!isRegistered(sinkTableName)) {
      throw TableException(s"No table was registered under the name $sinkTableName.")
    }

    getTable(sinkTableName) match {
      case s: TableSinkTable[_] =>
        val tableSink = s.tableSink
        // validate schema of source table and table sink
        val srcFieldTypes = table.getSchema.getTypes
        val sinkFieldTypes = tableSink.getFieldTypes

        if (srcFieldTypes.length != sinkFieldTypes.length ||
          srcFieldTypes.zip(sinkFieldTypes).exists{case (srcF, snkF) => srcF != snkF}) {

          val srcFieldNames = table.getSchema.getColumnNames
          val sinkFieldNames = tableSink.getFieldNames

          // format table and table sink schema strings
          val srcSchema = srcFieldNames.zip(srcFieldTypes)
            .map{case (n, t) => s"$n: ${t.getTypeClass.getSimpleName}"}
            .mkString("[", ", ", "]")
          val sinkSchema = sinkFieldNames.zip(sinkFieldTypes)
            .map{case (n, t) => s"$n: ${t.getTypeClass.getSimpleName}"}
            .mkString("[", ", ", "]")

          throw ValidationException(
            s"Field types of query result and registered TableSink $sinkTableName do not match.\n" +
              s"Query result schema: $srcSchema\n" +
              s"TableSink schema:    $sinkSchema")
        }
        // emit the table to the configured table sink
        writeToSink(table, tableSink, conf)
      case _ =>
        throw TableException(s"The table registered as $sinkTableName is not a TableSink. " +
          s"You can only emit query results to a registered TableSink.")
    }
  }

  /**
    * Registers a Calcite [[AbstractTable]] in the TableEnvironment's catalog.
    *
    * @param name The name under which the table will be registered.
    * @param table The table to register in the catalog
    * @throws TableException if another table is registered under the provided name.
    */
  @throws[TableException]
  protected def registerTableInternal(name: String, table: AbstractTable): Unit = {

    if (isRegistered(name)) {
      throw new TableException(s"Table \'$name\' already exists. " +
        s"Please, choose a different name.")
    } else {
      rootSchema.add(name, table)
    }
  }

  /** Returns a unique table name according to the internal naming pattern. */
  protected def createUniqueTableName(): String

  /**
    * Checks if the chosen table name is valid.
    *
    * @param name The table name to check.
    */
  protected def checkValidTableName(name: String): Unit

  /**
    * Checks if a table is registered under the given name.
    *
    * @param name The table name to check.
    * @return true, if a table is registered under the name, false otherwise.
    */
  protected[flink] def isRegistered(name: String): Boolean = {
    rootSchema.getTableNames.contains(name)
  }

  private def getTable(name: String): org.apache.calcite.schema.Table = {
    rootSchema.getTable(name)
  }

  protected def getRowType(name: String): RelDataType = {
    rootSchema.getTable(name).getRowType(typeFactory)
  }

  /** Returns a unique temporary attribute name. */
  private[flink] def createUniqueAttributeName(): String = {
    "TMP_" + attrNameCntr.getAndIncrement()
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

  private[flink] def getFunctionCatalog: FunctionCatalog = {
    functionCatalog
  }

  /** Returns the Calcite [[FrameworkConfig]] of this TableEnvironment. */
  private[flink] def getFrameworkConfig: FrameworkConfig = {
    frameworkConfig
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
  protected def isReferenceByPosition(ct: CompositeType[_], fields: Array[Expression]): Boolean = {
    if (!ct.isInstanceOf[TupleTypeInfoBase[_]]) {
      return false
    }

    val inputNames = ct.getFieldNames

    // Use the by-position mode if no of the fields exists in the input.
    // This prevents confusing cases like ('f2, 'f0, 'myName) for a Tuple3 where fields are renamed
    // by position but the user might assume reordering instead of renaming.
    fields.forall {
      case UnresolvedFieldReference(name) => !inputNames.contains(name)
      case _ => true
    }
  }

  /**
    * Returns field names and field positions for a given [[TypeInformation]].
    *
    * @param inputType The TypeInformation extract the field names and positions from.
    * @tparam A The type of the TypeInformation.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  protected[flink] def getFieldInfo[A](inputType: TypeInformation[A]):
  (Array[String], Array[Int]) = {

    if (inputType.isInstanceOf[GenericTypeInfo[A]] && inputType.getTypeClass == classOf[Row]) {
      throw new TableException(
        "An input of GenericTypeInfo<Row> cannot be converted to Table. " +
          "Please specify the type of the input with a RowTypeInfo.")
    } else {
      (TableEnvironment.getFieldNames(inputType), TableEnvironment.getFieldIndices(inputType))
    }
  }

  /**
    * Returns field names and field positions for a given [[TypeInformation]] and [[Array]] of
    * [[Expression]]. It does not handle time attributes but considers them in indices.
    *
    * @param inputType The [[TypeInformation]] against which the [[Expression]]s are evaluated.
    * @param exprs     The expressions that define the field names.
    * @tparam A The type of the TypeInformation.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  protected def getFieldInfo[A](
      inputType: TypeInformation[A],
      exprs: Array[Expression])
    : (Array[String], Array[Int]) = {

    TableEnvironment.validateType(inputType)

    def referenceByName(name: String, ct: CompositeType[_]): Option[Int] = {
      val inputIdx = ct.getFieldIndex(name)
      if (inputIdx < 0) {
        throw new TableException(s"$name is not a field of type $ct. " +
                s"Expected: ${ct.getFieldNames.mkString(", ")}")
      } else {
        Some(inputIdx)
      }
    }

    val indexedNames: Array[(Int, String)] = inputType match {

      case g: GenericTypeInfo[A] if g.getTypeClass == classOf[Row] =>
        throw new TableException(
          "An input of GenericTypeInfo<Row> cannot be converted to Table. " +
            "Please specify the type of the input with a RowTypeInfo.")

      case t: TupleTypeInfoBase[A] if t.isInstanceOf[TupleTypeInfo[A]] ||
        t.isInstanceOf[CaseClassTypeInfo[A]] || t.isInstanceOf[RowTypeInfo] =>

        // determine schema definition mode (by position or by name)
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

      case p: PojoTypeInfo[A] =>
        exprs flatMap {
          case (UnresolvedFieldReference(name: String)) =>
            referenceByName(name, p).map((_, name))
          case Alias(UnresolvedFieldReference(origName), name: String, _) =>
            referenceByName(origName, p).map((_, name))
          case _: TimeAttribute =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }

      case _: TypeInformation[_] => // atomic or other custom type information
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

  protected def generateRowConverterFunction[OUT](
      inputTypeInfo: TypeInformation[Row],
      schema: RowSchema,
      requestedTypeInfo: TypeInformation[OUT],
      functionName: String)
    : Option[GeneratedFunction[MapFunction[Row, OUT], OUT]] = {

    // validate that at least the field types of physical and logical type match
    // we do that here to make sure that plan translation was correct
    if (schema.typeInfo != inputTypeInfo) {
      throw TableException(
        s"The field types of physical and logical row types do not match. " +
        s"Physical type is [${schema.typeInfo}], Logical type is [$inputTypeInfo]. " +
        s"This is a bug and should not happen. Please file an issue.")
    }

    // generic row needs no conversion
    if (requestedTypeInfo.isInstanceOf[GenericTypeInfo[_]] &&
        requestedTypeInfo.getTypeClass == classOf[Row]) {
      return None
    }

    val fieldTypes = schema.fieldTypeInfos
    val fieldNames = schema.fieldNames

    // check for valid type info
    if (requestedTypeInfo.getArity != fieldTypes.length) {
      throw new TableException(
        s"Arity [${fieldTypes.length}] of result [$fieldTypes] does not match " +
        s"the number[${requestedTypeInfo.getArity}] of requested type [$requestedTypeInfo].")
    }

    // check requested types

    def validateFieldType(fieldType: TypeInformation[_]): Unit = fieldType match {
      case _: TimeIndicatorTypeInfo =>
        throw new TableException("The time indicator type is an internal type only.")
      case _ => // ok
    }

    requestedTypeInfo match {
      // POJO type requested
      case pt: PojoTypeInfo[_] =>
        fieldNames.zip(fieldTypes) foreach {
          case (fName, fType) =>
            val pojoIdx = pt.getFieldIndex(fName)
            if (pojoIdx < 0) {
              throw new TableException(s"POJO does not define field name: $fName")
            }
            val requestedTypeInfo = pt.getTypeAt(pojoIdx)
            validateFieldType(requestedTypeInfo)
            if (fType != requestedTypeInfo) {
              throw new TableException(s"Result field does not match requested type. " +
                s"Requested: $requestedTypeInfo; Actual: $fType")
            }
        }

      // Tuple/Case class/Row type requested
      case tt: TupleTypeInfoBase[_] =>
        fieldTypes.zipWithIndex foreach {
          case (fieldTypeInfo, i) =>
            val requestedTypeInfo = tt.getTypeAt(i)
            validateFieldType(requestedTypeInfo)
            if (fieldTypeInfo != requestedTypeInfo) {
              throw new TableException(s"Result field does not match requested type. " +
                s"Requested: $requestedTypeInfo; Actual: $fieldTypeInfo")
            }
        }

      // atomic type requested
      case t: TypeInformation[_] =>
        if (fieldTypes.size != 1) {
          throw new TableException(s"Requested result type is an atomic type but " +
            s"result[$fieldTypes] has more or less than a single field.")
        }
        val requestedTypeInfo = fieldTypes.head
        validateFieldType(requestedTypeInfo)
        if (requestedTypeInfo != t) {
          throw new TableException(s"Result field does not match requested type. " +
            s"Requested: $t; Actual: $requestedTypeInfo")
        }

      case _ =>
        throw new TableException(s"Unsupported result type: $requestedTypeInfo")
    }

    // code generate MapFunction
    val generator = new FunctionCodeGenerator(
      config,
      false,
      inputTypeInfo,
      None,
      None)

    val conversion = generator.generateConverterResultExpression(
      requestedTypeInfo,
      fieldNames)

    val body =
      s"""
         |${conversion.code}
         |return ${conversion.resultTerm};
         |""".stripMargin

    val generated = generator.generateFunction(
      functionName,
      classOf[MapFunction[Row, OUT]],
      body,
      requestedTypeInfo)

    Some(generated)
  }
}

/**
  * Object to instantiate a [[TableEnvironment]] depending on the batch or stream execution
  * environment.
  */
object TableEnvironment {

  /**
    * Returns a [[JavaBatchTableEnv]] for a Java [[JavaBatchExecEnv]].
    *
    * @param executionEnvironment The Java batch ExecutionEnvironment.
    */
  def getTableEnvironment(executionEnvironment: JavaBatchExecEnv): JavaBatchTableEnv = {
    new JavaBatchTableEnv(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[JavaBatchTableEnv]] for a Java [[JavaBatchExecEnv]] and a given [[TableConfig]].
    *
    * @param executionEnvironment The Java batch ExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    */
  def getTableEnvironment(
    executionEnvironment: JavaBatchExecEnv,
    tableConfig: TableConfig): JavaBatchTableEnv = {

    new JavaBatchTableEnv(executionEnvironment, tableConfig)
  }

  /**
    * Returns a [[ScalaBatchTableEnv]] for a Scala [[ScalaBatchExecEnv]].
    *
    * @param executionEnvironment The Scala batch ExecutionEnvironment.
    */
  def getTableEnvironment(executionEnvironment: ScalaBatchExecEnv): ScalaBatchTableEnv = {
    new ScalaBatchTableEnv(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[ScalaBatchTableEnv]] for a Scala [[ScalaBatchExecEnv]] and a given
    * [[TableConfig]].
    *
    * @param executionEnvironment The Scala batch ExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    */
  def getTableEnvironment(
    executionEnvironment: ScalaBatchExecEnv,
    tableConfig: TableConfig): ScalaBatchTableEnv = {

    new ScalaBatchTableEnv(executionEnvironment, tableConfig)
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
    * Returns field names for a given [[TypeInformation]].
    *
    * @param inputType The TypeInformation extract the field names.
    * @tparam A The type of the TypeInformation.
    * @return An array holding the field names
    */
  def getFieldNames[A](inputType: TypeInformation[A]): Array[String] = {
    validateType(inputType)

    val fieldNames: Array[String] = inputType match {
      case t: CompositeType[_] => t.getFieldNames
      case _: TypeInformation[_] => Array("f0")
    }

    if (fieldNames.contains("*")) {
      throw new TableException("Field name can not be '*'.")
    }

    fieldNames
  }

  /**
    * Validate if class represented by the typeInfo is static and globally accessible
    * @param typeInfo type to check
    * @throws TableException if type does not meet these criteria
    */
  def validateType(typeInfo: TypeInformation[_]): Unit = {
    val clazz = typeInfo.getTypeClass
    if ((clazz.isMemberClass && !Modifier.isStatic(clazz.getModifiers)) ||
      !Modifier.isPublic(clazz.getModifiers) ||
      clazz.getCanonicalName == null) {
      throw TableException(s"Class '$clazz' described in type information '$typeInfo' must be " +
        s"static and globally accessible.")
    }
  }

  /**
    * Returns field indexes for a given [[TypeInformation]].
    *
    * @param inputType The TypeInformation extract the field positions from.
    * @return An array holding the field positions
    */
  def getFieldIndices(inputType: TypeInformation[_]): Array[Int] = {
    getFieldNames(inputType).indices.toArray
  }

  /**
    * Returns field types for a given [[TypeInformation]].
    *
    * @param inputType The TypeInformation to extract field types from.
    * @return An array holding the field types.
    */
  def getFieldTypes(inputType: TypeInformation[_]): Array[TypeInformation[_]] = {
    validateType(inputType)

    inputType match {
      case ct: CompositeType[_] => 0.until(ct.getArity).map(i => ct.getTypeAt(i)).toArray
      case t: TypeInformation[_] => Array(t.asInstanceOf[TypeInformation[_]])
    }
  }

}
