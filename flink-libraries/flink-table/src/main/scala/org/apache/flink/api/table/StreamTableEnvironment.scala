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
import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql2rel.RelDecorrelator
import org.apache.calcite.tools.Programs

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.api.table.plan.logical.{CatalogNode, LogicalRelNode}
import org.apache.flink.api.table.plan.nodes.datastream.{DataStreamConvention, DataStreamRel}
import org.apache.flink.api.table.plan.rules.FlinkRuleSets
import org.apache.flink.api.table.sinks.{StreamTableSink, TableSink}
import org.apache.flink.api.table.plan.schema.{TableSourceTable, DataStreamTable}
import org.apache.flink.api.table.sources.StreamTableSource
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * The base class for stream TableEnvironments.
  *
  * A TableEnvironment can be used to:
  * - convert [[DataStream]] to a [[Table]]
  * - register a [[DataStream]] as a table in the catalog
  * - register a [[Table]] in the catalog
  * - scan a registered table to obtain a [[Table]]
  * - specify a SQL query on registered tables to obtain a [[Table]]
  * - convert a [[Table]] into a [[DataStream]]
  *
  * @param execEnv The [[StreamExecutionEnvironment]] which is wrapped in this
  *                [[StreamTableEnvironment]].
  * @param config The [[TableConfig]] of this [[StreamTableEnvironment]].
  */
abstract class StreamTableEnvironment(
    private[flink] val execEnv: StreamExecutionEnvironment,
    config: TableConfig)
  extends TableEnvironment(config) {

  // a counter for unique table names
  private val nameCntr: AtomicInteger = new AtomicInteger(0)

  // the naming pattern for internally registered tables.
  private val internalNamePattern = "^_DataStreamTable_[0-9]+$".r

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
  protected def createUniqueTableName(): String = "_DataStreamTable_" + nameCntr.getAndIncrement()

  /**
    * Ingests a registered table and returns the resulting [[Table]].
    *
    * The table to ingest must be registered in the [[TableEnvironment]]'s catalog.
    *
    * @param tableName The name of the table to ingest.
    * @throws ValidationException if no table is registered under the given name.
    * @return The ingested table.
    */
  @throws[ValidationException]
  def ingest(tableName: String): Table = {

    if (isRegistered(tableName)) {
      new Table(this, CatalogNode(tableName, getRowType(tableName)))
    }
    else {
      throw new ValidationException(s"Table \'$tableName\' was not found in the registry.")
    }
  }

  /**
    * Registers an external [[StreamTableSource]] in this [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[StreamTableSource]] is registered.
    * @param tableSource The [[org.apache.flink.api.table.sources.StreamTableSource]] to register.
    */
  def registerTableSource(name: String, tableSource: StreamTableSource[_]): Unit = {

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
    * Internally, the [[Table]] is translated into a [[DataStream]] and handed over to the
    * [[TableSink]] to write it.
    *
    * @param table The [[Table]] to write.
    * @param sink The [[TableSink]] to write the [[Table]] to.
    * @tparam T The expected type of the [[DataStream]] which represents the [[Table]].
    */
  override private[flink] def writeToSink[T](table: Table, sink: TableSink[T]): Unit = {

    sink match {
      case streamSink: StreamTableSink[T] =>
        val outputType = sink.getOutputType
        // translate the Table into a DataStream and provide the type that the TableSink expects.
        val result: DataStream[T] = translate(table)(outputType)
        // Give the DataSet to the TableSink to emit it.
        streamSink.emitDataStream(result)
      case _ =>
        throw new TableException("StreamTableSink required to emit streaming Table")
    }
  }

  /**
    * Registers a [[DataStream]] as a table under a given name in the [[TableEnvironment]]'s
    * catalog.
    *
    * @param name The name under which the table is registered in the catalog.
    * @param dataStream The [[DataStream]] to register as table in the catalog.
    * @tparam T the type of the [[DataStream]].
    */
  protected def registerDataStreamInternal[T](
    name: String,
    dataStream: DataStream[T]): Unit = {

    val (fieldNames, fieldIndexes) = getFieldInfo[T](dataStream.getType)
    val dataStreamTable = new DataStreamTable[T](
      dataStream,
      fieldIndexes,
      fieldNames
    )
    registerTableInternal(name, dataStreamTable)
  }

  /**
    * Registers a [[DataStream]] as a table under a given name with field names as specified by
    * field expressions in the [[TableEnvironment]]'s catalog.
    *
    * @param name The name under which the table is registered in the catalog.
    * @param dataStream The [[DataStream]] to register as table in the catalog.
    * @param fields The field expressions to define the field names of the table.
    * @tparam T The type of the [[DataStream]].
    */
  protected def registerDataStreamInternal[T](
    name: String,
    dataStream: DataStream[T],
    fields: Array[Expression]): Unit = {

    val (fieldNames, fieldIndexes) = getFieldInfo[T](dataStream.getType, fields.toArray)
    val dataStreamTable = new DataStreamTable[T](
      dataStream,
      fieldIndexes.toArray,
      fieldNames.toArray
    )
    registerTableInternal(name, dataStreamTable)
  }

  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param relNode The root node of the relational expression tree.
    * @return The optimized [[RelNode]] tree
    */
  private[flink] def optimize(relNode: RelNode): RelNode = {
    // decorrelate
    val decorPlan = RelDecorrelator.decorrelateQuery(relNode)

    // optimize the logical Flink plan
    val optProgram = Programs.ofRules(FlinkRuleSets.DATASTREAM_OPT_RULES)
    val flinkOutputProps = relNode.getTraitSet.replace(DataStreamConvention.INSTANCE).simplify()

    val dataStreamPlan = try {
      optProgram.run(getPlanner, decorPlan, flinkOutputProps)
    }
    catch {
      case e: CannotPlanException =>
        throw new TableException(
          s"Cannot generate a valid execution plan for the given query: \n\n" +
            s"${RelOptUtil.toString(relNode)}\n" +
            s"This exception indicates that the query uses an unsupported SQL feature.\n" +
            s"Please check the documentation for the set of currently supported SQL features.")
    }
    dataStreamPlan
  }


  /**
    * Translates a [[Table]] into a [[DataStream]].
    *
    * The transformation involves optimizing the relational expression tree as defined by
    * Table API calls and / or SQL queries and generating corresponding [[DataStream]] operators.
    *
    * @param table The root node of the relational expression tree.
    * @param tpe The [[TypeInformation]] of the resulting [[DataStream]].
    * @tparam A The type of the resulting [[DataStream]].
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](table: Table)(implicit tpe: TypeInformation[A]): DataStream[A] = {

    validateType(tpe)

   val dataStreamPlan = optimize(table.getRelNode)

    dataStreamPlan match {
      case node: DataStreamRel =>
        node.translateToPlan(
          this,
          Some(tpe.asInstanceOf[TypeInformation[Any]])
        ).asInstanceOf[DataStream[A]]
      case _ => ???
    }

  }

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    */
   def explain(table: Table): String = {

    val ast = RelOptUtil.toString(table.getRelNode)

    s"== Abstract Syntax Tree ==" +
      System.lineSeparator +
      ast

  }

}
