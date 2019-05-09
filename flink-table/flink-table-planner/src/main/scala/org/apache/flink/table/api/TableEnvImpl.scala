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
import org.apache.calcite.plan._
import org.apache.calcite.plan.hep.{HepMatchOrder, HepPlanner, HepProgram, HepProgramBuilder}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.schema
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql._
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.util.ChainedSqlOperatorTable
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.tools._
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{RowTypeInfo, _}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.table.calcite._
import org.apache.flink.table.catalog.{ExternalCatalog, ExternalCatalogSchema}
import org.apache.flink.table.codegen.{ExpressionReducer, FunctionCodeGenerator, GeneratedFunction}
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction, UserDefinedAggregateFunction}
import org.apache.flink.table.operations.{CatalogTableOperation, OperationTreeBuilder, PlannerTableOperation}
import org.apache.flink.table.plan.TableOperationConverter
import org.apache.flink.table.plan.cost.DataSetCostFactory
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.apache.flink.table.plan.schema.{RelTable, RowSchema, TableSourceSinkTable}
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.table.validate.FunctionCatalog
import org.apache.flink.types.Row

import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable

/**
  * The abstract base class for the implementation of batch and stream TableEnvironments.
  *
  * @param config The configuration of the TableEnvironment
  */
abstract class TableEnvImpl(val config: TableConfig) extends TableEnvironment {

  // the catalog to hold all registered and translated tables
  // we disable caching here to prevent side effects
  private val internalSchema: CalciteSchema = CalciteSchema.createRootSchema(false, false)
  private val rootSchema: SchemaPlus = internalSchema.plus()

  // Table API/SQL function catalog
  private[flink] val functionCatalog: FunctionCatalog = new FunctionCatalog()

  // the configuration to create a Calcite planner
  private lazy val frameworkConfig: FrameworkConfig = Frameworks
    .newConfigBuilder
    .defaultSchema(rootSchema)
    .parserConfig(getSqlParserConfig)
    .costFactory(new DataSetCostFactory)
    .typeSystem(new FlinkTypeSystem)
    .operatorTable(getSqlOperatorTable)
    .sqlToRelConverterConfig(getSqlToRelConverterConfig)
    // the converter is needed when calling temporal table functions from SQL, because
    // they reference a history table represented with a tree of table operations
    .context(Contexts.of(
      new TableOperationConverter.ToRelConverterSupplier(expressionBridge)
    ))
    // set the executor to evaluate constant expressions
    .executor(new ExpressionReducer(config))
    .build

  // temporary bridge between API and planner
  private[flink] val expressionBridge: ExpressionBridge[PlannerExpression] =
    new ExpressionBridge[PlannerExpression](functionCatalog, PlannerExpressionConverter.INSTANCE)

  // the builder for Calcite RelNodes, Calcite's representation of a relational expression tree.
  protected lazy val relBuilder: FlinkRelBuilder = FlinkRelBuilder
    .create(frameworkConfig, expressionBridge)

  // the planner instance used to optimize queries of this TableEnvironment
  private lazy val planner: RelOptPlanner = relBuilder.getPlanner

  private lazy val typeFactory: FlinkTypeFactory = relBuilder.getTypeFactory

  // a counter for unique attribute names
  private[flink] val attrNameCntr: AtomicInteger = new AtomicInteger(0)

  // registered external catalog names -> catalog
  private val externalCatalogs = new mutable.HashMap[String, ExternalCatalog]

  private[flink] val operationTreeBuilder = new OperationTreeBuilder(this)

  protected def calciteConfig: CalciteConfig = config.getPlannerConfig
    .unwrap(classOf[CalciteConfig])
    .orElse(CalciteConfig.DEFAULT)

  def getConfig: TableConfig = config

  private[flink] def queryConfig: QueryConfig = this match {
    case _: BatchTableEnvImpl => new BatchQueryConfig
    case _: StreamTableEnvImpl => new StreamQueryConfig
    case _ => null
  }

  /**
    * Returns the SqlToRelConverter config.
    */
  protected def getSqlToRelConverterConfig: SqlToRelConverter.Config = {
    calciteConfig.sqlToRelConverterConfig match {

      case None =>
        SqlToRelConverter.configBuilder()
          .withTrimUnusedFields(false)
          .withConvertTableAccess(false)
          .withInSubQueryThreshold(Integer.MAX_VALUE)
          .withRelBuilderFactory(new FlinkRelBuilderFactory(expressionBridge))
          .build()

      case Some(c) => c
    }
  }

  /**
    * Returns the operator table for this environment including a custom Calcite configuration.
    */
  protected def getSqlOperatorTable: SqlOperatorTable = {
    calciteConfig.sqlOperatorTable match {

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
    calciteConfig.normRuleSet match {

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
    calciteConfig.logicalOptRuleSet match {

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
    calciteConfig.physicalOptRuleSet match {

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
    calciteConfig.sqlParserConfig match {

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

  protected def optimizeConvertSubQueries(relNode: RelNode): RelNode = {
    runHepPlannerSequentially(
      HepMatchOrder.BOTTOM_UP,
      FlinkRuleSets.TABLE_SUBQUERY_RULES,
      relNode,
      relNode.getTraitSet)
  }

  protected def optimizeExpandPlan(relNode: RelNode): RelNode = {
    val result = runHepPlannerSimultaneously(
      HepMatchOrder.TOP_DOWN,
      FlinkRuleSets.EXPAND_PLAN_RULES,
      relNode,
      relNode.getTraitSet)

    runHepPlannerSequentially(
      HepMatchOrder.TOP_DOWN,
      FlinkRuleSets.POST_EXPAND_CLEAN_UP_RULES,
      result,
      result.getTraitSet)
  }

  protected def optimizeNormalizeLogicalPlan(relNode: RelNode): RelNode = {
    val normRuleSet = getNormRuleSet
    if (normRuleSet.iterator().hasNext) {
      runHepPlannerSequentially(HepMatchOrder.BOTTOM_UP, normRuleSet, relNode, relNode.getTraitSet)
    } else {
      relNode
    }
  }

  protected def optimizeLogicalPlan(relNode: RelNode): RelNode = {
    val logicalOptRuleSet = getLogicalOptRuleSet
    val logicalOutputProps = relNode.getTraitSet.replace(FlinkConventions.LOGICAL).simplify()
    if (logicalOptRuleSet.iterator().hasNext) {
      runVolcanoPlanner(logicalOptRuleSet, relNode, logicalOutputProps)
    } else {
      relNode
    }
  }

  protected def optimizePhysicalPlan(relNode: RelNode, convention: Convention): RelNode = {
    val physicalOptRuleSet = getPhysicalOptRuleSet
    val physicalOutputProps = relNode.getTraitSet.replace(convention).simplify()
    if (physicalOptRuleSet.iterator().hasNext) {
      runVolcanoPlanner(physicalOptRuleSet, relNode, physicalOutputProps)
    } else {
      relNode
    }
  }

  /**
    * run HEP planner with rules applied one by one. First apply one rule to all of the nodes
    * and only then apply the next rule. If a rule creates a new node preceding rules will not
    * be applied to the newly created node.
    */
  protected def runHepPlannerSequentially(
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
    runHepPlanner(builder.build(), input, targetTraits)
  }

  /**
    * run HEP planner with rules applied simultaneously. Apply all of the rules to the given
    * node before going to the next one. If a rule creates a new node all of the rules will
    * be applied to this new node.
    */
  protected def runHepPlannerSimultaneously(
    hepMatchOrder: HepMatchOrder,
    ruleSet: RuleSet,
    input: RelNode,
    targetTraits: RelTraitSet): RelNode = {

    val builder = new HepProgramBuilder
    builder.addMatchOrder(hepMatchOrder)

    builder.addRuleCollection(ruleSet.asScala.toList.asJava)
    runHepPlanner(builder.build(), input, targetTraits)
  }

  /**
    * run HEP planner
    */
  protected def runHepPlanner(
    hepProgram: HepProgram,
    input: RelNode,
    targetTraits: RelTraitSet): RelNode = {

    val planner = new HepPlanner(hepProgram, frameworkConfig.getContext)
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
            s"${t.getMessage}\n" +
            s"Please check the documentation for the set of currently supported SQL features.")
      case a: AssertionError =>
        // keep original exception stack for caller
        throw a
    }
    output
  }

  override def fromTableSource(source: TableSource[_]): Table = {
    val name = createUniqueTableName()
    registerTableSourceInternal(name, source)
    scan(name)
  }

  override def registerExternalCatalog(name: String, externalCatalog: ExternalCatalog): Unit = {
    if (rootSchema.getSubSchema(name) != null) {
      throw new ExternalCatalogAlreadyExistException(name)
    }
    this.externalCatalogs.put(name, externalCatalog)
    // create an external catalog Calcite schema, register it on the root schema
    ExternalCatalogSchema.registerCatalog(this, rootSchema, name, externalCatalog)
  }

  override def getRegisteredExternalCatalog(name: String): ExternalCatalog = {
    this.externalCatalogs.get(name) match {
      case Some(catalog) => catalog
      case None => throw new ExternalCatalogNotExistException(name)
    }
  }

  override def registerFunction(name: String, function: ScalarFunction): Unit = {
    // check if class could be instantiated
    checkForInstantiation(function.getClass)

    functionCatalog.registerScalarFunction(
      name,
      function,
      typeFactory)
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

    functionCatalog.registerTableFunction(
      name,
      function,
      typeInfo,
      typeFactory)
  }

  /**
    * Registers an [[AggregateFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  private[flink] def registerAggregateFunctionInternal[T: TypeInformation, ACC: TypeInformation](
      name: String, function: UserDefinedAggregateFunction[T, ACC]): Unit = {
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

    functionCatalog.registerAggregateFunction(
      name,
      function,
      resultTypeInfo,
      accTypeInfo,
      typeFactory)
  }

  override def registerTable(name: String, table: Table): Unit = {

    // check that table belongs to this table environment
    if (table.asInstanceOf[TableImpl].tableEnv != this) {
      throw new TableException(
        "Only tables that belong to this TableEnvironment can be registered.")
    }

    checkValidTableName(name)
    val tableTable = new RelTable(table.asInstanceOf[TableImpl].getRelNode)
    registerTableInternal(name, tableTable)
  }

  override def registerTableSource(name: String, tableSource: TableSource[_]): Unit = {
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

  override def registerTableSink(
      name: String,
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]],
      tableSink: TableSink[_]): Unit

  override def registerTableSink(name: String, configuredSink: TableSink[_]): Unit

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

  @throws[TableException]
  override def scan(tablePath: String*): Table = {
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
        return Some(new TableImpl(this,
          new CatalogTableOperation(tablePath.toList.asJava, extractTableSchema(table))))
      }
    }
    None
  }

  private def extractTableSchema(table: schema.Table): TableSchema = {
    val relDataType = table.getRowType(typeFactory)
    val fieldNames = relDataType.getFieldNames
    val fieldTypes = relDataType.getFieldList.asScala
      .map(field => FlinkTypeFactory.toTypeInfo(field.getType))
    new TableSchema(fieldNames.asScala.toArray, fieldTypes.toArray)
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

  override def listTables(): Array[String] = {
    rootSchema.getTableNames.asScala.toArray
  }

  override def listUserDefinedFunctions(): Array[String] = {
    functionCatalog.getUserDefinedFunctions.toArray
  }

  override def explain(table: Table): String

  override def getCompletionHints(statement: String, position: Int): Array[String] = {
    val planner = new FlinkPlannerImpl(
      getFrameworkConfig,
      getPlanner,
      getTypeFactory)
    planner.getCompletionHints(statement, position)
  }

  override def sqlQuery(query: String): Table = {
    val planner = new FlinkPlannerImpl(getFrameworkConfig, getPlanner, getTypeFactory)
    // parse the sql query
    val parsed = planner.parse(query)
    if (null != parsed && parsed.getKind.belongsTo(SqlKind.QUERY)) {
      // validate the sql query
      val validated = planner.validate(parsed)
      // transform to a relational tree
      val relational = planner.rel(validated)
      new TableImpl(this, new PlannerTableOperation(relational.rel))
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
    val planner = new FlinkPlannerImpl(getFrameworkConfig, getPlanner, getTypeFactory)
    // parse the sql query
    val parsed = planner.parse(stmt)
    parsed match {
      case insert: SqlInsert =>
        // validate the SQL query
        val query = insert.getSource
        val validatedQuery = planner.validate(query)

        // get query result as Table
        val queryResult = new TableImpl(this,
          new PlannerTableOperation(planner.rel(validatedQuery).rel))

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
    if (null == sinkTableName) throw new TableException("Name of TableSink must not be null.")
    if (sinkTableName.isEmpty) throw new TableException("Name of TableSink must not be empty.")

    getTable(sinkTableName) match {

      case None =>
        throw new TableException(s"No table was registered under the name $sinkTableName.")

      case Some(s: TableSourceSinkTable[_, _]) if s.tableSinkTable.isDefined =>
        val tableSink = s.tableSinkTable.get.tableSink
        // validate schema of source table and table sink
        val srcFieldTypes = table.getSchema.getFieldTypes
        val sinkFieldTypes = tableSink.getFieldTypes

        if (srcFieldTypes.length != sinkFieldTypes.length ||
          srcFieldTypes.zip(sinkFieldTypes).exists { case (srcF, snkF) => srcF != snkF }) {

          val srcFieldNames = table.getSchema.getFieldNames
          val sinkFieldNames = tableSink.getFieldNames

          // format table and table sink schema strings
          val srcSchema = srcFieldNames.zip(srcFieldTypes)
            .map { case (n, t) => s"$n: ${t.getTypeClass.getSimpleName}" }
            .mkString("[", ", ", "]")
          val sinkSchema = sinkFieldNames.zip(sinkFieldTypes)
            .map { case (n, t) => s"$n: ${t.getTypeClass.getSimpleName}" }
            .mkString("[", ", ", "]")

          throw new ValidationException(
            s"Field types of query result and registered TableSink " +
              s"$sinkTableName do not match.\n" +
              s"Query result schema: $srcSchema\n" +
              s"TableSink schema:    $sinkSchema")
        }
        // emit the table to the configured table sink
        writeToSink(table, tableSink, conf)

      case Some(_) =>
        throw new TableException(s"The table registered as $sinkTableName is not a TableSink. " +
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

  /**
    * Get a table from either internal or external catalogs.
    *
    * @param name The name of the table.
    * @return The table registered either internally or externally, None otherwise.
    */
  protected def getTable(name: String): Option[org.apache.calcite.schema.Table] = {

    // recursively fetches a table from a schema.
    def getTableFromSchema(
        schema: SchemaPlus,
        path: List[String]): Option[org.apache.calcite.schema.Table] = {

      path match {
        case tableName :: Nil =>
          // look up table
          Option(schema.getTable(tableName))
        case subschemaName :: remain =>
          // look up subschema
          val subschema = Option(schema.getSubSchema(subschemaName))
          subschema match {
            case Some(s) =>
              // search for table in subschema
              getTableFromSchema(s, remain)
            case None =>
              // subschema does not exist
              None
          }
      }
    }

    val pathNames = name.split('.').toList
    getTableFromSchema(rootSchema, pathNames)
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
      (TableEnvImpl.getFieldNames(inputType), TableEnvImpl.getFieldIndices(inputType))
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

    TableEnvImpl.validateType(inputType)

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
          case (_: TimeAttribute, _) | (Alias(_: TimeAttribute, _, _), _) =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }

      case p: PojoTypeInfo[A] =>
        exprs flatMap {
          case UnresolvedFieldReference(name: String) =>
            referenceByName(name, p).map((_, name))
          case Alias(UnresolvedFieldReference(origName), name: String, _) =>
            referenceByName(origName, p).map((_, name))
          case _: TimeAttribute | Alias(_: TimeAttribute, _, _) =>
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
      throw new TableException(
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
  * Object to instantiate a [[TableEnvImpl]] depending on the batch or stream execution environment.
  */
object TableEnvImpl {

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
      throw new TableException(
        s"Class '$clazz' described in type information '$typeInfo' must be " +
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
