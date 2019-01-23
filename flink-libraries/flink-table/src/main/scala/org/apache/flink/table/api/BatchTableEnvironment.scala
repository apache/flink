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

import _root_.java.util.concurrent.atomic.AtomicInteger

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql2rel.RelDecorrelator
import org.apache.calcite.tools.RuleSet
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.descriptors.{BatchTableDescriptor, ConnectorDescriptor}
import org.apache.flink.table.explain.PlanJsonParser
import org.apache.flink.table.expressions.{Expression, TimeAttribute}
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetRel
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.runtime.MapRunner
import org.apache.flink.table.sinks._
import org.apache.flink.table.sources.{BatchTableSource, TableSource}
import org.apache.flink.types.Row

/**
  * The abstract base class for batch TableEnvironments.
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
  * @param execEnv The [[ExecutionEnvironment]] which is wrapped in this [[BatchTableEnvironment]].
  * @param config The [[TableConfig]] of this [[BatchTableEnvironment]].
  */
abstract class BatchTableEnvironment(
    private[flink] val execEnv: ExecutionEnvironment,
    config: TableConfig)
  extends TableEnvironment(config) {

  // a counter for unique table names.
  private val nameCntr: AtomicInteger = new AtomicInteger(0)

  // the naming pattern for internally registered tables.
  private val internalNamePattern = "^_DataSetTable_[0-9]+$".r

  override def queryConfig: BatchQueryConfig = new BatchQueryConfig

  /**
    * Checks if the chosen table name is valid.
    *
    * @param name The table name to check.
    */
  override protected def checkValidTableName(name: String): Unit = {
    val m = internalNamePattern.findFirstIn(name)
    m match {
      case Some(_) =>
        throw new TableException(s"Illegal Table name. " +
          s"Please choose a name that does not contain the pattern $internalNamePattern")
      case None =>
    }
  }

  /** Returns a unique table name according to the internal naming pattern. */
  override protected def createUniqueTableName(): String =
    "_DataSetTable_" + nameCntr.getAndIncrement()

  /**
    * Registers an internal [[BatchTableSource]] in this [[TableEnvironment]]'s catalog without
    * name checking. Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  override protected def registerTableSourceInternal(
      name: String,
      tableSource: TableSource[_])
    : Unit = {

    tableSource match {

      // check for proper batch table source
      case batchTableSource: BatchTableSource[_] =>
        // check if a table (source or sink) is registered
        getTable(name) match {

          // table source and/or sink is registered
          case Some(table: TableSourceSinkTable[_, _]) => table.tableSourceTable match {

            // wrapper contains source
            case Some(_: TableSourceTable[_]) =>
              throw new TableException(s"Table '$name' already exists. " +
                s"Please choose a different name.")

            // wrapper contains only sink (not source)
            case _ =>
              val enrichedTable = new TableSourceSinkTable(
                Some(new BatchTableSourceTable(batchTableSource)),
                table.tableSinkTable)
              replaceRegisteredTable(name, enrichedTable)
          }

          // no table is registered
          case _ =>
            val newTable = new TableSourceSinkTable(
              Some(new BatchTableSourceTable(batchTableSource)),
              None)
            registerTableInternal(name, newTable)
        }

      // not a batch table source
      case _ =>
        throw new TableException("Only BatchTableSource can be registered in " +
            "BatchTableEnvironment.")
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
  def connect(connectorDescriptor: ConnectorDescriptor): BatchTableDescriptor = {
    new BatchTableDescriptor(this, connectorDescriptor)
  }

  /**
    * Registers an external [[TableSink]] with given field names and types in this
    * [[TableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * Example:
    *
    * {{{
    *   // create a table sink and its field names and types
    *   val fieldNames: Array[String] = Array("a", "b", "c")
    *   val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.INT, Types.LONG)
    *   val tableSink: BatchTableSink = new YourTableSinkImpl(...)
    *
    *   // register the table sink in the catalog
    *   tableEnv.registerTableSink("output_table", fieldNames, fieldsTypes, tableSink)
    *
    *   // use the registered sink
    *   tableEnv.sqlUpdate("INSERT INTO output_table SELECT a, b, c FROM sourceTable")
    * }}}
    *
    * @param name       The name under which the [[TableSink]] is registered.
    * @param fieldNames The field names to register with the [[TableSink]].
    * @param fieldTypes The field types to register with the [[TableSink]].
    * @param tableSink  The [[TableSink]] to register.
    */
  def registerTableSink(
      name: String,
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]],
      tableSink: TableSink[_]): Unit = {
    // validate
    checkValidTableName(name)
    if (fieldNames == null) throw new TableException("fieldNames must not be null.")
    if (fieldTypes == null) throw new TableException("fieldTypes must not be null.")
    if (fieldNames.length == 0) throw new TableException("fieldNames must not be empty.")
    if (fieldNames.length != fieldTypes.length) {
      throw new TableException("Same number of field names and types required.")
    }

    // configure and register
    val configuredSink = tableSink.configure(fieldNames, fieldTypes)
    registerTableSinkInternal(name, configuredSink)
  }

  /**
    * Registers an external [[TableSink]] with already configured field names and field types in
    * this [[TableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * @param name The name under which the [[TableSink]] is registered.
    * @param configuredSink The configured [[TableSink]] to register.
    */
  def registerTableSink(name: String, configuredSink: TableSink[_]): Unit = {
    registerTableSinkInternal(name, configuredSink)
  }

  private def registerTableSinkInternal(name: String, configuredSink: TableSink[_]): Unit = {
    // validate
    checkValidTableName(name)
    if (configuredSink.getFieldNames == null || configuredSink.getFieldTypes == null) {
      throw new TableException("Table sink is not configured.")
    }
    if (configuredSink.getFieldNames.length == 0) {
      throw new TableException("Field names must not be empty.")
    }
    if (configuredSink.getFieldNames.length != configuredSink.getFieldTypes.length) {
      throw new TableException("Same number of field names and types required.")
    }

    // register
    configuredSink match {

      // check for proper batch table sink
      case _: BatchTableSink[_] =>

        // check if a table (source or sink) is registered
        getTable(name) match {

          // table source and/or sink is registered
          case Some(table: TableSourceSinkTable[_, _]) => table.tableSinkTable match {

            // wrapper contains sink
            case Some(_: TableSinkTable[_]) =>
              throw new TableException(s"Table '$name' already exists. " +
                s"Please choose a different name.")

            // wrapper contains only source (not sink)
            case _ =>
              val enrichedTable = new TableSourceSinkTable(
                table.tableSourceTable,
                Some(new TableSinkTable(configuredSink)))
              replaceRegisteredTable(name, enrichedTable)
          }

          // no table is registered
          case _ =>
            val newTable = new TableSourceSinkTable(
              None,
              Some(new TableSinkTable(configuredSink)))
            registerTableInternal(name, newTable)
        }

      // not a batch table sink
      case _ =>
        throw new TableException("Only BatchTableSink can be registered in BatchTableEnvironment.")
    }
  }

  /**
    * Writes a [[Table]] to a [[TableSink]].
    *
    * Internally, the [[Table]] is translated into a [[DataSet]] and handed over to the
    * [[TableSink]] to write it.
    *
    * @param table The [[Table]] to write.
    * @param sink The [[TableSink]] to write the [[Table]] to.
    * @param queryConfig The configuration for the query to generate.
    * @tparam T The expected type of the [[DataSet]] which represents the [[Table]].
    */
  override private[flink] def writeToSink[T](
      table: Table,
      sink: TableSink[T],
      queryConfig: QueryConfig): Unit = {

    // Check the query configuration to be a batch one.
    val batchQueryConfig = queryConfig match {
      case batchConfig: BatchQueryConfig => batchConfig
      case _ =>
        throw new TableException("BatchQueryConfig required to configure batch query.")
    }

    sink match {
      case batchSink: BatchTableSink[T] =>
        val outputType = sink.getOutputType
        // translate the Table into a DataSet and provide the type that the TableSink expects.
        val result: DataSet[T] = translate(table, batchQueryConfig)(outputType)
        // Give the DataSet to the TableSink to emit it.
        batchSink.emitDataSet(result)
      case _ =>
        throw new TableException("BatchTableSink required to emit batch Table.")
    }
  }

  /**
    * Creates a final converter that maps the internal row type to external type.
    *
    * @param physicalTypeInfo the input of the sink
    * @param schema the input schema with correct field names (esp. for POJO field mapping)
    * @param requestedTypeInfo the output type of the sink
    * @param functionName name of the map function. Must not be unique but has to be a
    *                     valid Java class identifier.
    */
  protected def getConversionMapper[IN, OUT](
      physicalTypeInfo: TypeInformation[IN],
      schema: RowSchema,
      requestedTypeInfo: TypeInformation[OUT],
      functionName: String)
    : Option[MapFunction[IN, OUT]] = {

    val converterFunction = generateRowConverterFunction[OUT](
      physicalTypeInfo.asInstanceOf[TypeInformation[Row]],
      schema,
      requestedTypeInfo,
      functionName
    )

    // add a runner if we need conversion
    converterFunction.map { func =>
      new MapRunner[IN, OUT](
          func.name,
          func.code,
          func.returnType)
    }
  }

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    * @param extended Flag to include detailed optimizer estimates.
    */
  private[flink] def explain(table: Table, extended: Boolean): String = {
    val ast = table.getRelNode
    val optimizedPlan = optimize(ast)
    val dataSet = translate[Row](optimizedPlan, ast.getRowType, queryConfig) (
      new GenericTypeInfo (classOf[Row]))
    dataSet.output(new DiscardingOutputFormat[Row])
    val env = dataSet.getExecutionEnvironment
    val jasonSqlPlan = env.getExecutionPlan
    val sqlPlan = PlanJsonParser.getSqlExecutionPlan(jasonSqlPlan, extended)

    s"== Abstract Syntax Tree ==" +
        System.lineSeparator +
        s"${RelOptUtil.toString(ast)}" +
        System.lineSeparator +
        s"== Optimized Logical Plan ==" +
        System.lineSeparator +
        s"${RelOptUtil.toString(optimizedPlan)}" +
        System.lineSeparator +
        s"== Physical Execution Plan ==" +
        System.lineSeparator +
        s"$sqlPlan"
  }

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    */
  def explain(table: Table): String = explain(table: Table, extended = false)

  /**
    * Registers a [[DataSet]] as a table under a given name in the [[TableEnvironment]]'s catalog.
    *
    * @param name The name under which the table is registered in the catalog.
    * @param dataSet The [[DataSet]] to register as table in the catalog.
    * @tparam T the type of the [[DataSet]].
    */
  protected def registerDataSetInternal[T](name: String, dataSet: DataSet[T]): Unit = {

    val (fieldNames, fieldIndexes) = getFieldInfo[T](dataSet.getType)
    val dataSetTable = new DataSetTable[T](
      dataSet,
      fieldIndexes,
      fieldNames
    )
    registerTableInternal(name, dataSetTable)
  }

  /**
    * Registers a [[DataSet]] as a table under a given name with field names as specified by
    * field expressions in the [[TableEnvironment]]'s catalog.
    *
    * @param name The name under which the table is registered in the catalog.
    * @param dataSet The [[DataSet]] to register as table in the catalog.
    * @param fields The field expressions to define the field names of the table.
    * @tparam T The type of the [[DataSet]].
    */
  protected def registerDataSetInternal[T](
      name: String, dataSet: DataSet[T], fields: Array[Expression]): Unit = {

    val inputType = dataSet.getType

    val (fieldNames, fieldIndexes) = getFieldInfo[T](
      inputType,
      fields)

    if (fields.exists(_.isInstanceOf[TimeAttribute])) {
      throw new ValidationException(
        ".rowtime and .proctime time indicators are not allowed in a batch environment.")
    }

    val dataSetTable = new DataSetTable[T](dataSet, fieldIndexes, fieldNames)
    registerTableInternal(name, dataSetTable)
  }

  /**
    * Returns the built-in normalization rules that are defined by the environment.
    */
  protected def getBuiltInNormRuleSet: RuleSet = FlinkRuleSets.DATASET_NORM_RULES

  /**
    * Returns the built-in optimization rules that are defined by the environment.
    */
  protected def getBuiltInPhysicalOptRuleSet: RuleSet = FlinkRuleSets.DATASET_OPT_RULES

  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param relNode The original [[RelNode]] tree
    * @return The optimized [[RelNode]] tree
    */
  private[flink] def optimize(relNode: RelNode): RelNode = {
    val convSubQueryPlan = optimizeConvertSubQueries(relNode)
    val expandedPlan = optimizeExpandPlan(convSubQueryPlan)
    val decorPlan = RelDecorrelator.decorrelateQuery(expandedPlan)
    val normalizedPlan = optimizeNormalizeLogicalPlan(decorPlan)
    val logicalPlan = optimizeLogicalPlan(normalizedPlan)
    optimizePhysicalPlan(logicalPlan, FlinkConventions.DATASET)
  }

  /**
    * Translates a [[Table]] into a [[DataSet]].
    *
    * The transformation involves optimizing the relational expression tree as defined by
    * Table API calls and / or SQL queries and generating corresponding [[DataSet]] operators.
    *
    * @param table The root node of the relational expression tree.
    * @param queryConfig The configuration for the query to generate.
    * @param tpe   The [[TypeInformation]] of the resulting [[DataSet]].
    * @tparam A The type of the resulting [[DataSet]].
    * @return The [[DataSet]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](
      table: Table,
      queryConfig: BatchQueryConfig)(implicit tpe: TypeInformation[A]): DataSet[A] = {
    val relNode = table.getRelNode
    val dataSetPlan = optimize(relNode)
    translate(dataSetPlan, relNode.getRowType, queryConfig)
  }

  /**
    * Translates a logical [[RelNode]] into a [[DataSet]]. Converts to target type if necessary.
    *
    * @param logicalPlan The root node of the relational expression tree.
    * @param logicalType The row type of the result. Since the logicalPlan can lose the
    *                    field naming during optimization we pass the row type separately.
    * @param queryConfig The configuration for the query to generate.
    * @param tpe         The [[TypeInformation]] of the resulting [[DataSet]].
    * @tparam A The type of the resulting [[DataSet]].
    * @return The [[DataSet]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](
      logicalPlan: RelNode,
      logicalType: RelDataType,
      queryConfig: BatchQueryConfig)(implicit tpe: TypeInformation[A]): DataSet[A] = {
    TableEnvironment.validateType(tpe)

    logicalPlan match {
      case node: DataSetRel =>
        val plan = node.translateToPlan(this, queryConfig)
        val conversion =
          getConversionMapper(
            plan.getType,
            new RowSchema(logicalType),
            tpe,
            "DataSetSinkConversion")
        conversion match {
          case None => plan.asInstanceOf[DataSet[A]] // no conversion necessary
          case Some(mapFunction: MapFunction[Row, A]) =>
            plan.map(mapFunction)
              .returns(tpe)
              .name(s"to: ${tpe.getTypeClass.getSimpleName}")
              .asInstanceOf[DataSet[A]]
        }

      case _ =>
        throw new TableException("Cannot generate DataSet due to an invalid logical plan. " +
          "This is a bug and should not happen. Please file an issue.")
    }
  }
}
