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

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.plan.`trait`.FlinkRelDistributionTraitDef
import org.apache.flink.table.plan.nodes.calcite.LogicalSink
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.optimize.{BatchOptimizer, Optimizer}
import org.apache.flink.table.plan.reuse.DeadlockBreakupProcessor
import org.apache.flink.table.plan.schema.{BatchTableSourceTable, TableSourceSinkTable, TableSourceTable}
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.plan.util.FlinkRelOptUtil
import org.apache.flink.table.sinks._
import org.apache.flink.table.sources._

import org.apache.calcite.plan.{ConventionTraitDef, RelTrait, RelTraitDef}
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode}
import org.apache.calcite.sql.SqlExplainLevel

import _root_.scala.collection.JavaConversions._

/**
  *  A session to construct between [[Table]] and [[DataStream]], its main function is:
  *
  *  1. Get a table from [[DataStream]], or through registering a [[TableSource]];
  *  2. Transform current already construct table to [[DataStream]];
  *  3. Add [[TableSink]] to the [[Table]].
  * @param config The [[TableConfig]] of this [[BatchTableEnvironment]].
  */
class BatchTableEnvironment(
    val streamEnv: StreamExecutionEnvironment,
    config: TableConfig)
  extends TableEnvironment(config) {

  // prefix for unique table names.
  override private[flink] val tableNamePrefix = "_DataStreamTable_"

  // the naming pattern for internally registered tables.
  private val internalNamePattern = "^_DataStreamTable_[0-9]+$".r

  override def queryConfig: QueryConfig = new BatchQueryConfig

  override protected def getTraitDefs: Array[RelTraitDef[_ <: RelTrait]] = {
    Array(
      ConventionTraitDef.INSTANCE,
      FlinkRelDistributionTraitDef.INSTANCE,
      RelCollationTraitDef.INSTANCE)
  }

  override protected def getOptimizer: Optimizer = new BatchOptimizer(this)

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

  override private[flink] def translateNodeDag(rels: Seq[RelNode]): Seq[ExecNode[_, _]] = {
    val nodeDag = super.translateNodeDag(rels)
    // breakup deadlock
    new DeadlockBreakupProcessor().process(nodeDag)
  }

  /**
    * Merge global job parameters and table config parameters,
    * and set the merged result to GlobalJobParameters
    */
  private def mergeParameters(): Unit = {
    if (streamEnv != null && streamEnv.getConfig != null) {
      val parameters = new Configuration()
      if (config != null && config.getConf != null) {
        parameters.addAll(config.getConf)
      }

      if (streamEnv.getConfig.getGlobalJobParameters != null) {
        streamEnv.getConfig.getGlobalJobParameters.toMap.foreach {
          kv => parameters.setString(kv._1, kv._2)
        }
      }

      streamEnv.getConfig.setGlobalJobParameters(parameters)
    }
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
  override private[table] def writeToSink[T](
      table: Table,
      sink: TableSink[T],
      sinkName: String): Unit = {
    mergeParameters()

    val sinkNode = LogicalSink.create(table.asInstanceOf[TableImpl].getRelNode, sink, sinkName)
    val optimizedPlan = optimize(sinkNode)
    val optimizedNodes = translateNodeDag(Seq(optimizedPlan))
    require(optimizedNodes.size() == 1)
    translateToPlan(optimizedNodes.head)
  }

  /**
    * Translates a [[BatchExecNode]] plan into a [[StreamTransformation]].
    * Converts to target type if necessary.
    *
    * @param node        The plan to translate.
    * @return The [[StreamTransformation]] that corresponds to the given node.
    */
  private def translateToPlan(node: ExecNode[_, _]): StreamTransformation[_] = {
    node match {
      case node: BatchExecNode[_] => node.translateToPlan(this)
      case _ =>
        throw new TableException("Cannot generate BoundedStream due to an invalid logical plan. " +
                                   "This is a bug and should not happen. Please file an issue.")
    }
  }

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    */
  def explain(table: Table): String = explain(table, extended = false)

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table    The table for which the AST and execution plan will be returned.
    * @param extended Flag to include detailed optimizer estimates.
    */
  def explain(table: Table, extended: Boolean): String = {
    val ast = table.asInstanceOf[TableImpl].getRelNode
    val optimizedNode = optimize(ast)

    val explainLevel = if (extended) {
      SqlExplainLevel.ALL_ATTRIBUTES
    } else {
      SqlExplainLevel.EXPPLAN_ATTRIBUTES
    }

    s"== Abstract Syntax Tree ==" +
      System.lineSeparator +
      s"${FlinkRelOptUtil.toString(ast)}" +
      System.lineSeparator +
      s"== Optimized Logical Plan ==" +
      System.lineSeparator +
      s"${FlinkRelOptUtil.toString(optimizedNode, explainLevel)}" +
      System.lineSeparator
    // TODO show Physical Execution Plan
  }

  /**
    * Explain the whole plan, and returns the AST(s) of the specified Table API and SQL queries
    * and the execution plan.
    */
  def explain(): String = explain(extended = false)

  /**
    * Explain the whole plan, and returns the AST(s) of the specified Table API and SQL queries
    * and the execution plan.
    *
    * @param extended Flag to include detailed optimizer estimates.
    */
  def explain(extended: Boolean): String = {
    // TODO implements this method when supports multi-sinks
    throw new TableException("Unsupported now")
  }

  /**
    * Registers an internal [[BatchTableSource]] in this [[TableEnvironment]]'s catalog without
    * name checking. Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    * @param replace     Whether to replace the registered table.
    */
  override protected def registerTableSourceInternal(
      name: String,
      tableSource: TableSource[_],
      statistic: FlinkStatistic,
      replace: Boolean = false): Unit = {

    tableSource match {

      // check for proper batch table source
      case batchTableSource: BatchTableSource[_] =>
        // check if a table (source or sink) is registered
        getTable(name) match {

          // table source and/or sink is registered
          case Some(table: TableSourceSinkTable[_, _]) => table.tableSourceTable match {

            // wrapper contains source
            case Some(_: TableSourceTable[_]) if !replace =>
              throw new TableException(s"Table '$name' already exists. " +
                s"Please choose a different name.")

            // wrapper contains only sink (not source)
            case _ =>
              val enrichedTable = new TableSourceSinkTable(
                Some(new BatchTableSourceTable(batchTableSource, statistic)),
                table.tableSinkTable)
              replaceRegisteredTable(name, enrichedTable)
          }

          // no table is registered
          case _ =>
            val newTable = new TableSourceSinkTable(
              Some(new BatchTableSourceTable(batchTableSource, statistic)),
              None)
            registerTableInternal(name, newTable)
        }

      // not a batch table source
      case _ =>
        throw new TableException("Only BatchTableSource can be " +
          "registered in BatchTableEnvironment.")
    }
  }

}
