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

package org.apache.flink.api.table

import java.util.concurrent.atomic.AtomicInteger

import org.apache.calcite.plan.RelOptPlanner.CannotPlanException
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.sql2rel.RelDecorrelator
import org.apache.calcite.tools.Programs
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.{ExecutionEnvironment, DataSet}
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.table.explain.PlanJsonParser
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.api.table.plan.PlanGenException
import org.apache.flink.api.table.plan.nodes.dataset.{DataSetRel, DataSetConvention}
import org.apache.flink.api.table.plan.rules.FlinkRuleSets
import org.apache.flink.api.table.plan.schema.{TableSourceTable, DataSetTable}
import org.apache.flink.api.table.sources.BatchTableSource

/**
  * The abstract base class for batch TableEnvironments.
  *
  * A TableEnvironment can be used to:
  * - convert a [[DataSet]] to a [[Table]]
  * - register a [[DataSet]] in the [[org.apache.flink.api.table.TableEnvironment]]'s catalog
  * - register a [[Table]] in the [[org.apache.flink.api.table.TableEnvironment]]'s catalog
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
  protected def createUniqueTableName(): String = "_DataSetTable_" + nameCntr.getAndIncrement()

  /**
    * Scans a registered table and returns the resulting [[Table]].
    *
    * The table to scan must be registered in the [[TableEnvironment]]'s catalog.
    *
    * @param tableName The name of the table to scan.
    * @throws TableException if no table is registered under the given name.
    * @return The scanned table.
    */
  @throws[TableException]
  def scan(tableName: String): Table = {

    if (isRegistered(tableName)) {
      relBuilder.scan(tableName)
      new Table(relBuilder.build(), this)
    }
    else {
      throw new TableException(s"Table \'$tableName\' was not found in the registry.")
    }
  }

  /**
    * Registers an external [[BatchTableSource]] in this [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name The name under which the [[BatchTableSource]] is registered.
    * @param tableSource The [[BatchTableSource]] to register.
    */
  def registerTableSource(name: String, tableSource: BatchTableSource[_]): Unit = {

    checkValidTableName(name)
    registerTableInternal(name, new TableSourceTable(tableSource))
  }

  /**
    * Evaluates a SQL query on registered tables and retrieves the result as a [[Table]].
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    *
    * @param query The SQL query to evaluate.
    * @return The result of the query as Table.
    */
  override def sql(query: String): Table = {

    val planner = new FlinkPlannerImpl(getFrameworkConfig, getPlanner)
    // parse the sql query
    val parsed = planner.parse(query)
    // validate the sql query
    val validated = planner.validate(parsed)
    // transform to a relational tree
    val relational = planner.rel(validated)

    new Table(relational.rel, this)
  }

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    * @param extended Flag to include detailed optimizer estimates.
    */
  private[flink] def explain(table: Table, extended: Boolean): String = {

    val ast = RelOptUtil.toString(table.relNode)
    val dataSet = translate[Row](table)(TypeExtractor.createTypeInfo(classOf[Row]))
    dataSet.output(new DiscardingOutputFormat[Row])
    val env = dataSet.getExecutionEnvironment
    val jasonSqlPlan = env.getExecutionPlan
    val sqlPlan = PlanJsonParser.getSqlExecutionPlan(jasonSqlPlan, extended)

    s"== Abstract Syntax Tree ==\n" +
    s"$ast\n" +
    s"== Physical Execution Plan ==\n" +
    s"$sqlPlan"

  }

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    */
  def explain(table: Table): String = explain(table: Table, false)

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
    name: String, dataSet: DataSet[T],
    fields: Array[Expression]): Unit = {

    val (fieldNames, fieldIndexes) = getFieldInfo[T](dataSet.getType, fields.toArray)
    val dataSetTable = new DataSetTable[T](
      dataSet,
      fieldIndexes.toArray,
      fieldNames.toArray
    )
    registerTableInternal(name, dataSetTable)
  }

  /**
    * Translates a [[Table]] into a [[DataSet]].
    *
    * The transformation involves optimizing the relational expression tree as defined by
    * Table API calls and / or SQL queries and generating corresponding [[DataSet]] operators.
    *
    * @param table The root node of the relational expression tree.
    * @param tpe The [[TypeInformation]] of the resulting [[DataSet]].
    * @tparam A The type of the resulting [[DataSet]].
    * @return The [[DataSet]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](table: Table)(implicit tpe: TypeInformation[A]): DataSet[A] = {

    val relNode = table.getRelNode

    // decorrelate
    val decorPlan = RelDecorrelator.decorrelateQuery(relNode)

    // optimize the logical Flink plan
    val optProgram = Programs.ofRules(FlinkRuleSets.DATASET_OPT_RULES)
    val flinkOutputProps = relNode.getTraitSet.replace(DataSetConvention.INSTANCE).simplify()

    val dataSetPlan = try {
      optProgram.run(getPlanner, decorPlan, flinkOutputProps)
    }
    catch {
      case e: CannotPlanException =>
        throw new PlanGenException(
          s"Cannot generate a valid execution plan for the given query: \n\n" +
            s"${RelOptUtil.toString(relNode)}\n" +
            "Please consider filing a bug report.", e)
      case a: AssertionError =>
        throw a.getCause
    }

    dataSetPlan match {
      case node: DataSetRel =>
        node.translateToPlan(
          this,
          Some(tpe.asInstanceOf[TypeInformation[Any]])
        ).asInstanceOf[DataSet[A]]
      case _ => ???
    }
  }

}
