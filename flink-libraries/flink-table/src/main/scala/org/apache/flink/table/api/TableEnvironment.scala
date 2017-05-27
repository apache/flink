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
import org.apache.calcite.sql.SqlOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.util.ChainedSqlOperatorTable
import org.apache.calcite.tools._
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.java.{ExecutionEnvironment => JavaBatchExecEnv}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.scala.{ExecutionEnvironment => ScalaBatchExecEnv}
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaStreamExecEnv}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment => ScalaStreamExecEnv}
import org.apache.flink.table.api.java.{BatchTableEnvironment => JavaBatchTableEnv, StreamTableEnvironment => JavaStreamTableEnv}
import org.apache.flink.table.api.scala.{BatchTableEnvironment => ScalaBatchTableEnv, StreamTableEnvironment => ScalaStreamTableEnv}
import org.apache.flink.table.calcite.{FlinkPlannerImpl, FlinkRelBuilder, FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.catalog.{ExternalCatalog, ExternalCatalogSchema}
import org.apache.flink.table.codegen.{CodeGenerator, ExpressionReducer, GeneratedFunction}
import org.apache.flink.table.expressions.{Alias, Expression, UnresolvedFieldReference}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{checkForInstantiation, checkNotSingleton, createScalarSqlFunction, createTableSqlFunctions}
import org.apache.flink.table.functions.{ScalarFunction, TableFunction}
import org.apache.flink.table.plan.cost.DataSetCostFactory
import org.apache.flink.table.plan.logical.{CatalogNode, LogicalRelNode}
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.apache.flink.table.plan.schema.{RelTable, RowSchema}
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.{DefinedFieldNames, TableSource}
import org.apache.flink.table.validate.FunctionCatalog
import org.apache.flink.types.Row
import org.apache.flink.api.java.typeutils.RowTypeInfo

import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable.HashMap
import _root_.scala.annotation.varargs

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
  private val externalCatalogs = new HashMap[String, ExternalCatalog]

  /** Returns the table config to define the runtime behavior of the Table API. */
  def getConfig = config

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
        throw a.getCause
    }
    output
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
    // create an external catalog calicte schema, register it on the root schema
    ExternalCatalogSchema.registerCatalog(rootSchema, name, externalCatalog)
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
    functionCatalog.registerSqlFunction(createScalarSqlFunction(name, function, typeFactory))
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
    val sqlFunctions = createTableSqlFunctions(name, function, typeInfo, typeFactory)
    functionCatalog.registerSqlFunctions(sqlFunctions)
  }

  /**
    * Registers an [[AggregateFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  private[flink] def registerAggregateFunctionInternal[T: TypeInformation, ACC](
      name: String, function: AggregateFunction[T, ACC]): Unit = {
    // check if class not Scala object
    checkNotSingleton(function.getClass)
    // check if class could be instantiated
    checkForInstantiation(function.getClass)

    val typeInfo: TypeInformation[_] = implicitly[TypeInformation[T]]

    // register in Table API
    functionCatalog.registerFunction(name, function.getClass)

    // register in SQL API
    val sqlFunctions = createAggregateSqlFunction(name, function, typeInfo, typeFactory)
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
  def registerTableSource(name: String, tableSource: TableSource[_]): Unit

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
    scanInternal(tablePath.toArray)
  }

  @throws[TableException]
  private def scanInternal(tablePath: Array[String]): Table = {
    require(tablePath != null && !tablePath.isEmpty, "tablePath must not be null or empty.")
    val schemaPaths = tablePath.slice(0, tablePath.length - 1)
    val schema = getSchema(schemaPaths)
    if (schema != null) {
      val tableName = tablePath(tablePath.length - 1)
      val table = schema.getTable(tableName)
      if (table != null) {
        return new Table(this, CatalogNode(tablePath, table.getRowType(typeFactory)))
      }
    }
    throw new TableException(s"Table \'${tablePath.mkString(".")}\' was not found.")
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
    * Evaluates a SQL query on registered tables and retrieves the result as a [[Table]].
    *
    * All tables referenced by the query must be registered in the TableEnvironment. But
    * [[Table.toString]] will automatically register an unique table name and return the
    * table name. So it allows to call SQL directly on tables like this:
    *
    * {{{
    *   val table: Table = ...
    *   // the table is not registered to the table environment
    *   tEnv.sql(s"SELECT * FROM $table")
    * }}}
    *
    * @param query The SQL query to evaluate.
    * @return The result of the query as Table.
    */
  def sql(query: String): Table = {
    val planner = new FlinkPlannerImpl(getFrameworkConfig, getPlanner, getTypeFactory)
    // parse the sql query
    val parsed = planner.parse(query)
    // validate the sql query
    val validated = planner.validate(parsed)
    // transform to a relational tree
    val relational = planner.rel(validated)

    new Table(this, LogicalRelNode(relational.rel))
  }

  /**
    * Writes a [[Table]] to a [[TableSink]].
    *
    * @param table The [[Table]] to write.
    * @param sink The [[TableSink]] to write the [[Table]] to.
    * @tparam T The data type that the [[TableSink]] expects.
    */
  private[flink] def writeToSink[T](table: Table, sink: TableSink[T], conf: QueryConfig): Unit

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
  protected def isRegistered(name: String): Boolean = {
    rootSchema.getTableNames.contains(name)
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
  protected[flink] def getFieldInfo[A](
      inputType: TypeInformation[A],
      exprs: Array[Expression])
    : (Array[String], Array[Int]) = {

    TableEnvironment.validateType(inputType)

    val indexedNames: Array[(Int, String)] = inputType match {
      case g: GenericTypeInfo[A] if g.getTypeClass == classOf[Row] =>
        throw new TableException(
          "An input of GenericTypeInfo<Row> cannot be converted to Table. " +
            "Please specify the type of the input with a RowTypeInfo.")
      case a: AtomicType[_] =>
        exprs.zipWithIndex flatMap {
          case (UnresolvedFieldReference(name), idx) =>
            if (idx > 0) {
              throw new TableException("Table of atomic type can only have a single field.")
            }
            Some((0, name))
          case _ => throw new TableException("Field reference expression requested.")
        }
      case t: TupleTypeInfo[A] =>
        exprs.zipWithIndex flatMap {
          case (UnresolvedFieldReference(name), idx) =>
            Some((idx, name))
          case (Alias(UnresolvedFieldReference(origName), name, _), _) =>
            val idx = t.getFieldIndex(origName)
            if (idx < 0) {
              throw new TableException(s"$origName is not a field of type $t")
            }
            Some((idx, name))
          case (_: TimeAttribute, _) =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }
      case c: CaseClassTypeInfo[A] =>
        exprs.zipWithIndex flatMap {
          case (UnresolvedFieldReference(name), idx) =>
            Some((idx, name))
          case (Alias(UnresolvedFieldReference(origName), name, _), _) =>
            val idx = c.getFieldIndex(origName)
            if (idx < 0) {
              throw new TableException(s"$origName is not a field of type $c")
            }
            Some((idx, name))
          case (_: TimeAttribute, _) =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }
      case p: PojoTypeInfo[A] =>
        exprs flatMap {
          case (UnresolvedFieldReference(name)) =>
            val idx = p.getFieldIndex(name)
            if (idx < 0) {
              throw new TableException(s"$name is not a field of type $p")
            }
            Some((idx, name))
          case Alias(UnresolvedFieldReference(origName), name, _) =>
            val idx = p.getFieldIndex(origName)
            if (idx < 0) {
              throw new TableException(s"$origName is not a field of type $p")
            }
            Some((idx, name))
          case _: TimeAttribute =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }
      case r: RowTypeInfo => {
        exprs.zipWithIndex flatMap {
          case (UnresolvedFieldReference(name), idx) =>
            Some((idx, name))
          case (Alias(UnresolvedFieldReference(origName), name, _), _) =>
            val idx = r.getFieldIndex(origName)
            if (idx < 0) {
              throw new TableException(s"$origName is not a field of type $r")
            }
            Some((idx, name))
          case (_: TimeAttribute, _) =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }
        
      }
      case tpe => throw new TableException(
        s"Source of type $tpe cannot be converted into Table.")
    }

    val (fieldIndexes, fieldNames) = indexedNames.unzip

    if (fieldNames.contains("*")) {
      throw new TableException("Field name can not be '*'.")
    }

    (fieldNames.toArray, fieldIndexes.toArray)
  }

  protected def generateRowConverterFunction[OUT](
      inputTypeInfo: TypeInformation[Row],
      schema: RowSchema,
      requestedTypeInfo: TypeInformation[OUT],
      functionName: String):
    GeneratedFunction[MapFunction[Row, OUT], OUT] = {

    // validate that at least the field types of physical and logical type match
    // we do that here to make sure that plan translation was correct
    if (schema.physicalTypeInfo != inputTypeInfo) {
      throw TableException("The field types of physical and logical row types do not match." +
        "This is a bug and should not happen. Please file an issue.")
    }

    val fieldTypes = schema.physicalFieldTypeInfo
    val fieldNames = schema.physicalFieldNames

    // validate requested type
    if (requestedTypeInfo.getArity != fieldTypes.length) {
      throw new TableException("Arity of result does not match requested type.")
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
            if (fType != requestedTypeInfo) {
              throw new TableException(s"Result field does not match requested type. " +
                s"requested: $requestedTypeInfo; Actual: $fType")
            }
        }

      // Tuple/Case class/Row type requested
      case tt: TupleTypeInfoBase[_] =>
        fieldTypes.zipWithIndex foreach {
          case (fieldTypeInfo, i) =>
            val requestedTypeInfo = tt.getTypeAt(i)
            if (fieldTypeInfo != requestedTypeInfo) {
              throw new TableException(s"Result field does not match requested type. " +
                s"Requested: $requestedTypeInfo; Actual: $fieldTypeInfo")
            }
        }

      // Atomic type requested
      case at: AtomicType[_] =>
        if (fieldTypes.size != 1) {
          throw new TableException(s"Requested result type is an atomic type but " +
            s"result has more or less than a single field.")
        }
        val fieldTypeInfo = fieldTypes.head
        if (fieldTypeInfo != at) {
          throw new TableException(s"Result field does not match requested type. " +
            s"Requested: $at; Actual: $fieldTypeInfo")
        }

      case _ =>
        throw new TableException(s"Unsupported result type: $requestedTypeInfo")
    }

    // code generate MapFunction
    val generator = new CodeGenerator(
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

    generator.generateFunction(
      functionName,
      classOf[MapFunction[Row, OUT]],
      body,
      requestedTypeInfo)
  }
}

/**
  * Object to instantiate a [[TableEnvironment]] depending on the batch or stream execution
  * environment.
  */
object TableEnvironment {

  // default names that can be used in in TableSources etc.
  val DEFAULT_ROWTIME_ATTRIBUTE = "rowtime"
  val DEFAULT_PROCTIME_ATTRIBUTE = "proctime"

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
      case a: AtomicType[_] => Array("f0")
      case tpe =>
        throw new TableException(s"Currently only CompositeType and AtomicType are supported. " +
          s"Type $tpe lacks explicit field naming")
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
      case t: CompositeType[_] => 0.until(t.getArity).map(t.getTypeAt(_)).toArray
      case a: AtomicType[_] => Array(a.asInstanceOf[TypeInformation[_]])
      case tpe =>
        throw new TableException(s"Currently only CompositeType and AtomicType are supported.")
    }
  }

  /**
    * Returns field names for a given [[TableSource]].
    *
    * @param tableSource The TableSource to extract field names from.
    * @tparam A The type of the TableSource.
    * @return An array holding the field names.
    */
  def getFieldNames[A](tableSource: TableSource[A]): Array[String] = tableSource match {
      case d: DefinedFieldNames => d.getFieldNames
      case _ => TableEnvironment.getFieldNames(tableSource.getReturnType)
    }

  /**
    * Returns field indices for a given [[TableSource]].
    *
    * @param tableSource The TableSource to extract field indices from.
    * @tparam A The type of the TableSource.
    * @return An array holding the field indices.
    */
  def getFieldIndices[A](tableSource: TableSource[A]): Array[Int] = tableSource match {
    case d: DefinedFieldNames => d.getFieldIndices
    case _ => TableEnvironment.getFieldIndices(tableSource.getReturnType)
  }
}
