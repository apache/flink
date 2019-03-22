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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{RowTypeInfo, _}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.table.calcite.{FlinkContextImpl, FlinkPlannerImpl, FlinkRelBuilder, FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.optimize.Optimizer
import org.apache.flink.table.plan.schema.RelTable
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.TableSource
import org.apache.flink.types.Row

import org.apache.calcite.config.Lex
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.rel.RelNode
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql._
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.tools._

import _root_.java.lang.reflect.Modifier
import _root_.java.util.concurrent.atomic.AtomicInteger
import _root_.java.util.{Arrays => JArrays}

import _root_.scala.annotation.varargs
import _root_.scala.collection.JavaConverters._

/**
  * The abstract base class for batch and stream TableEnvironments.
  *
  * @param config The configuration of the TableEnvironment
  */
abstract class TableEnvironment(val config: TableConfig) {

  // the catalog to hold all registered and translated tables
  // we disable caching here to prevent side effects
  private val internalSchema: CalciteSchema = CalciteSchema.createRootSchema(false, false)
  private val rootSchema: SchemaPlus = internalSchema.plus()

  // the configuration to create a Calcite planner
  protected lazy val frameworkConfig: FrameworkConfig = Frameworks
    .newConfigBuilder
    .defaultSchema(rootSchema)
    .parserConfig(getSqlParserConfig)
    .costFactory(new FlinkCostFactory)
    .typeSystem(new FlinkTypeSystem)
    .sqlToRelConverterConfig(getSqlToRelConverterConfig)
    .operatorTable(FlinkSqlOperatorTable.instance())
    // TODO: introduce ExpressionReducer after codegen
    // set the executor to evaluate constant expressions
    // .executor(new ExpressionReducer(config))
    .context(new FlinkContextImpl(config))
    .build

  // the builder for Calcite RelNodes, Calcite's representation of a relational expression tree.
  protected lazy val relBuilder: FlinkRelBuilder = createRelBuilder

  // the planner instance used to optimize queries of this TableEnvironment
  private lazy val planner: RelOptPlanner = relBuilder.getPlanner

  private lazy val typeFactory: FlinkTypeFactory = relBuilder.getTypeFactory

  // a counter for unique attribute names
  private[flink] val attrNameCntr: AtomicInteger = new AtomicInteger(0)

  // a counter for unique table names
  private[flink] val tableNameCntr: AtomicInteger = new AtomicInteger(0)

  private[flink] val tableNamePrefix = "_TempTable_"

  /** Returns the table config to define the runtime behavior of the Table API. */
  def getConfig: TableConfig = config

  /** Returns the [[FlinkRelBuilder]] of this TableEnvironment. */
  private[flink] def getRelBuilder: FlinkRelBuilder = relBuilder

  /** Returns the Calcite [[org.apache.calcite.plan.RelOptPlanner]] of this TableEnvironment. */
  private[flink] def getPlanner: RelOptPlanner = planner

  /** Returns the [[FlinkTypeFactory]] of this TableEnvironment. */
  private[flink] def getTypeFactory: FlinkTypeFactory = typeFactory

  /** Returns the Calcite [[FrameworkConfig]] of this TableEnvironment. */
  private[flink] def getFrameworkConfig: FrameworkConfig = frameworkConfig

  /**
    * Returns the SqlToRelConverter config.
    *
    * `expand` is set as false, and each sub-query becomes a [[org.apache.calcite.rex.RexSubQuery]].
    */
  protected def getSqlToRelConverterConfig: SqlToRelConverter.Config = {
    SqlToRelConverter.configBuilder()
    .withTrimUnusedFields(false)
    .withConvertTableAccess(false)
    .withInSubQueryThreshold(Integer.MAX_VALUE)
    .withExpand(false)
    .build()
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

  /** Returns the [[QueryConfig]] depends on the concrete type of this TableEnvironment. */
  private[flink] def queryConfig: QueryConfig

  /** Returns specific [[FlinkRelBuilder]] depends on the concrete type of this TableEnvironment. */
  protected def createRelBuilder: FlinkRelBuilder

  /** Returns specific query [[Optimizer]] depends on the concrete type of this TableEnvironment. */
  protected def getOptimizer: Optimizer

  /**
    * Generates the optimized [[RelNode]] dag from the original relational nodes.
    *
    * @param roots The root nodes of the relational expression tree.
    * @return The optimized [[RelNode]] dag
    */
  private[flink] def optimize(roots: Seq[RelNode]): Seq[RelNode] = {
    val optRelNodes = getOptimizer.optimize(roots)
    require(optRelNodes.size == roots.size)
    optRelNodes
  }

  /**
    * Generates the optimized [[RelNode]] tree from the original relational tee.
    *
    * @param root The root nodes of the relational expression tree.
    * @return The optimized [[RelNode]] tree
    */
  private[flink] def optimize(root: RelNode): RelNode = optimize(Seq(root)).head

  /**
    * Registers a [[Table]] under a unique name in the TableEnvironment's catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name The name under which the table will be registered.
    * @param table The table to register.
    */
  def registerTable(name: String, table: Table): Unit = {

    // check that table belongs to this table environment
    if (table.asInstanceOf[TableImpl].tableEnv != this) {
      throw new TableException(
        "Only tables that belong to this TableEnvironment can be registered.")
    }

    checkValidTableName(name)
    val tableTable = new RelTable(table.asInstanceOf[TableImpl].getRelNode)
    registerTableInternal(name, tableTable)
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
    * registered as bounded or unbounded DataStream, or Table.
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
        val scan = relBuilder.scan(JArrays.asList(tablePath: _*)).build()
        return Some(new TableImpl(this, scan))
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
    * Returns completion hints for the given statement at the given cursor position.
    * The completion happens case insensitively.
    *
    * @param statement Partial or slightly incorrect SQL statement
    * @param position cursor position
    * @return completion hints that fit at the current cursor position
    */
  def getCompletionHints(statement: String, position: Int): Array[String] = {
    val planner = new FlinkPlannerImpl(
      getFrameworkConfig,
      getPlanner,
      getTypeFactory,
      relBuilder.getCluster)
    planner.getCompletionHints(statement, position)
  }

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    */
  def explain(table: Table): String

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    * @param extended Flag to include detailed optimizer estimates.
    */
  def explain(table: Table, extended: Boolean): String

  /**
    * Explain the whole plan, and returns the AST(s) of the specified Table API and SQL queries
    * and the execution plan.
    */
  def explain(): String

  /**
    * Explain the whole plan, and returns the AST(s) of the specified Table API and SQL queries
    * and the execution plan.
    *
    * @param extended Flag to include detailed optimizer estimates.
    */
  def explain(extended: Boolean): String

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
    val planner = new FlinkPlannerImpl(
      getFrameworkConfig,
      getPlanner,
      getTypeFactory,
      relBuilder.getCluster)
    // parse the sql query
    val parsed = planner.parse(query)
    if (null != parsed && parsed.getKind.belongsTo(SqlKind.QUERY)) {
      // validate the sql query
      val validated = planner.validate(parsed)
      // transform to a relational tree
      val relational = planner.rel(validated)
      new TableImpl(this, relational.project())
    } else {
      throw new TableException(
        "Unsupported SQL query! sqlQuery() only accepts SQL queries of type " +
          "SELECT, UNION, INTERSECT, EXCEPT, VALUES, and ORDER_BY.")
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
  private[flink] def registerTableInternal(name: String, table: AbstractTable): Unit = {
    if (isRegistered(name)) {
      throw new TableException(s"Table \'$name\' already exists. " +
              s"Please, choose a different name.")
    } else {
      rootSchema.add(name, table)
    }
  }

  /** Returns a unique table name according to the internal naming pattern. */
  private[flink] def createUniqueTableName(tableNamePrefix: Option[String] = None): String = {
    val prefix = tableNamePrefix.getOrElse(this.tableNamePrefix)
    var res = prefix + tableNameCntr.getAndIncrement()
    while (getTable(res).nonEmpty) {
      res = prefix + tableNameCntr.getAndIncrement()
    }
    res
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
  // TODO: we should support Expression fields after we introduce [Expression]
  protected def isReferenceByPosition(ct: CompositeType[_], fields: Array[String]): Boolean = {
    if (!ct.isInstanceOf[TupleTypeInfoBase[_]]) {
      return false
    }

    val inputNames = ct.getFieldNames

    // Use the by-position mode if no of the fields exists in the input.
    // This prevents confusing cases like ('f2, 'f0, 'myName) for a Tuple3 where fields are renamed
    // by position but the user might assume reordering instead of renaming.
    fields.forall(!inputNames.contains(_))
  }

  /**
    * Returns field names and field positions for a given [[TypeInformation]].
    *
    * @param inputType The TypeInformation extract the field names and positions from.
    * @tparam A The type of the TypeInformation.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  protected[flink] def getFieldInfo[A](
      inputType: TypeInformation[A]): (Array[String], Array[Int]) = {

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
    * field names. It does not handle time attributes.
    *
    * @param inputType The [[TypeInformation]] against which the field names are referenced.
    * @param fields The fields that define the field names.
    * @tparam A The type of the TypeInformation.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  // TODO: we should support Expression fields after we introduce [Expression]
  protected[flink] def getFieldInfo[A](
    inputType: TypeInformation[A],
    fields: Array[String]): (Array[String], Array[Int]) = {

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
        val isRefByPos = isReferenceByPosition(t, fields)

        fields.zipWithIndex flatMap { case (name, idx) =>
          if (isRefByPos) {
            Some((idx, name))
          } else {
            referenceByName(name, t).map((_, name))
          }
        }

      case p: PojoTypeInfo[A] =>
        fields flatMap { name =>
          referenceByName(name, p).map((_, name))
        }

      case _: TypeInformation[_] => // atomic or other custom type information
        if (fields.length > 1) {
          // only accept the first field for an atomic type
          throw new TableException("Only accept one field to reference an atomic type.")
        }
        // first field reference is mapped to atomic type
        Array((0, fields(0)))
    }

    val (fieldIndexes, fieldNames) = indexedNames.unzip

    if (fieldNames.contains("*")) {
      throw new TableException("Field name can not be '*'.")
    }

    (fieldNames, fieldIndexes)
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
    registerTableSourceInternal(name, tableSource, FlinkStatistic.UNKNOWN, replace = false)
  }

  /**
    * Registers or replace an external [[TableSource]] in this [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  def registerOrReplaceTableSource(name: String,
      tableSource: TableSource[_]): Unit = {
    checkValidTableName(name)
    registerTableSourceInternal(name, tableSource, FlinkStatistic.UNKNOWN, replace = true)
  }

  /**
    * Registers an internal [[TableSource]] in this [[TableEnvironment]]'s catalog without
    * name checking. Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    * @param replace     Whether to replace this [[TableSource]]
    */
  protected def registerTableSourceInternal(
      name: String,
      tableSource: TableSource[_],
      statistic: FlinkStatistic,
      replace: Boolean): Unit

}

/**
  * Object to instantiate a [[TableEnvironment]] depending on the batch or stream execution
  * environment.
  */
object TableEnvironment {

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
