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

import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.jobgraph.ScheduleMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.graph.{StreamGraph, StreamGraphGenerator}
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.operations.DataStreamQueryOperation
import org.apache.flink.table.plan.`trait`.FlinkRelDistributionTraitDef
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.nodes.process.DAGProcessContext
import org.apache.flink.table.plan.nodes.resource.parallelism.ParallelismProcessor
import org.apache.flink.table.plan.optimize.{BatchCommonSubGraphBasedOptimizer, Optimizer}
import org.apache.flink.table.plan.reuse.DeadlockBreakupProcessor
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.plan.util.{ExecNodePlanDumper, FlinkRelOptUtil}
import org.apache.flink.table.sinks._
import org.apache.flink.table.sources._
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType
import org.apache.flink.table.util.PlanUtil
import org.apache.flink.util.InstantiationUtil
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
    val execEnv: StreamExecutionEnvironment,
    config: TableConfig)
  extends TableEnvironment(execEnv, config) {

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

  override protected def getOptimizer: Optimizer = new BatchCommonSubGraphBasedOptimizer(this)

  override private[flink] def isBatch = true

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

  override protected def validateTableSource(tableSource: TableSource[_]): Unit = {
    // TODO TableSourceUtil.validateTableSource(tableSource)
    tableSource match {
      // check for proper batch table source
      case boundedTableSource: StreamTableSource[_] if boundedTableSource.isBounded => // ok
      // a lookupable table source can also be registered in the env
      case _: LookupableTableSource[_] => // ok
      // not a batch table source
      case _ =>
        throw new TableException("Only LookupableTableSouce and BatchTableSource can be " +
          "registered in BatchTableEnvironment.")
    }
  }

  override def execute(jobName: String): JobExecutionResult = {
    generateStreamGraph(jobName)
    // TODO supports streamEnv.execute(streamGraph)
    streamEnv.execute(jobName)
  }

  protected override def translateStreamGraph(
      streamingTransformations: Seq[StreamTransformation[_]],
      jobName: Option[String]): StreamGraph = {
    mergeParameters()

    // TODO avoid cloning ExecutionConfig
    val executionConfig = InstantiationUtil.clone(streamEnv.getConfig)
    executionConfig.enableObjectReuse()
    executionConfig.setLatencyTrackingInterval(-1)

    new StreamGraphGenerator(streamingTransformations.toList, executionConfig, new CheckpointConfig)
      .setChaining(streamEnv.isChainingEnabled)
      .setStateBackend(streamEnv.getStateBackend)
      .setDefaultBufferTimeout(-1)
      .setTimeCharacteristic(TimeCharacteristic.ProcessingTime)
      .setUserArtifacts(streamEnv.getCachedFiles)
      .setSlotSharingEnabled(false)
      .setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES)
      .setJobName(jobName.getOrElse(DEFAULT_JOB_NAME))
      .generate()
  }

  override private[flink] def translateToExecNodeDag(rels: Seq[RelNode]): Seq[ExecNode[_, _]] = {
    val nodeDag = super.translateToExecNodeDag(rels)
    val context = new DAGProcessContext(this)
    // breakup deadlock
    val postNodeDag = new DeadlockBreakupProcessor().process(nodeDag, context)
    new ParallelismProcessor().process(postNodeDag, context)
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

  override protected def translateToPlan(
      sinks: Seq[ExecNode[_, _]]): Seq[StreamTransformation[_]] = sinks.map(translateToPlan)

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
    val execNodeDag = compileToExecNodePlan(ast)
    val transformations = translateToPlan(execNodeDag)
    val streamGraph = translateStreamGraph(transformations)
    val executionPlan = PlanUtil.explainStreamGraph(streamGraph)

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
      s"${ExecNodePlanDumper.dagToString(execNodeDag, explainLevel)}" +
      System.lineSeparator +
      s"== Physical Execution Plan ==" +
      System.lineSeparator +
      s"$executionPlan"
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
    val sinkExecNodes = compileToExecNodePlan(sinkNodes: _*)
    val sinkTransformations = translateToPlan(sinkExecNodes)
    val streamGraph = translateStreamGraph(sinkTransformations)
    val sqlPlan = PlanUtil.explainStreamGraph(streamGraph)

    val sb = new StringBuilder
    sb.append("== Abstract Syntax Tree ==")
    sb.append(System.lineSeparator)
    sinkNodes.foreach { sink =>
      sb.append(FlinkRelOptUtil.toString(sink))
      sb.append(System.lineSeparator)
    }

    sb.append("== Optimized Logical Plan ==")
    sb.append(System.lineSeparator)
    val explainLevel = if (extended) {
      SqlExplainLevel.ALL_ATTRIBUTES
    } else {
      SqlExplainLevel.EXPPLAN_ATTRIBUTES
    }
    sb.append(ExecNodePlanDumper.dagToString(sinkExecNodes, explainLevel))
    sb.append(System.lineSeparator)

    sb.append("== Physical Execution Plan ==")
    sb.append(System.lineSeparator)
    sb.append(sqlPlan)
    sb.toString()
  }

  @VisibleForTesting
  private[flink] def asQueryOperation[T](
      boundedStream: DataStream[T],
      fields: Option[Array[String]],
      fieldNullables: Option[Array[Boolean]] = None,
      statistic: Option[FlinkStatistic] = None): DataStreamQueryOperation[T] = {
    val streamType = boundedStream.getType

    // get field names and types for all non-replaced fields
    val (indices, names) = fields match {
      case Some(f) =>
        fieldNullables match {
          case Some(nulls) => require(nulls.length == f.length,
            "length of `fields` and length of `fieldNullables` should be equal")
          case _ => // do nothing
        }
        val fieldIndexes = f.indices.toArray
        (fieldIndexes, f)
      case None =>
        val (fieldNames, fieldIndexes) = getFieldInfo[T](fromLegacyInfoToDataType(streamType))
        (fieldIndexes, fieldNames)
    }

    val dataStreamTable = new DataStreamQueryOperation(
      boundedStream,
      indices,
      TableEnvironment.calculateTableSchema(streamType, indices, names),
      fieldNullables.getOrElse(Array.fill(indices.length)(true)),
      statistic.getOrElse(FlinkStatistic.UNKNOWN))
    dataStreamTable
  }

}
