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

package org.apache.flink.table.planner.delegation

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.ExecutionOptions
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.table.api.{ExplainDetail, TableConfig, TableException}
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog, ObjectIdentifier}
import org.apache.flink.table.delegation.Executor
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.operations.{ModifyOperation, Operation, QueryOperation, SinkModifyOperation}
import org.apache.flink.table.planner.operations.PlannerQueryOperation
import org.apache.flink.table.planner.plan.`trait`._
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph
import org.apache.flink.table.planner.plan.nodes.exec.processor.ExecNodeGraphProcessor
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodePlanDumper
import org.apache.flink.table.planner.plan.optimize.{Optimizer, StreamCommonSubGraphBasedOptimizer}
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.table.planner.utils.DummyStreamExecutionEnvironment

import org.apache.calcite.plan.{ConventionTraitDef, RelTrait, RelTraitDef}
import org.apache.calcite.rel.logical.LogicalTableModify
import org.apache.calcite.sql.SqlExplainLevel

import java.util

import _root_.scala.collection.JavaConversions._

class StreamPlanner(
    executor: Executor,
    config: TableConfig,
    moduleManager: ModuleManager,
    functionCatalog: FunctionCatalog,
    catalogManager: CatalogManager)
  extends PlannerBase(executor, config, moduleManager, functionCatalog, catalogManager,
    isStreamingMode = true) {

  override protected def getTraitDefs: Array[RelTraitDef[_ <: RelTrait]] = {
    Array(
      ConventionTraitDef.INSTANCE,
      FlinkRelDistributionTraitDef.INSTANCE,
      MiniBatchIntervalTraitDef.INSTANCE,
      ModifyKindSetTraitDef.INSTANCE,
      UpdateKindTraitDef.INSTANCE)
  }

  override protected def getOptimizer: Optimizer = new StreamCommonSubGraphBasedOptimizer(this)

  override protected def getExecNodeGraphProcessors: Seq[ExecNodeGraphProcessor] = Seq()

  override protected def translateToPlan(execGraph: ExecNodeGraph): util.List[Transformation[_]] = {
    validateAndOverrideConfiguration()
    val planner = createDummyPlanner()
    val transformations = execGraph.getRootNodes.map {
      case node: StreamExecNode[_] => node.translateToPlan(planner)
      case _ =>
        throw new TableException("Cannot generate DataStream due to an invalid logical plan. " +
          "This is a bug and should not happen. Please file an issue.")
    }
    cleanupInternalConfigurations()
    transformations
  }

  override def explain(operations: util.List[Operation], extraDetails: ExplainDetail*): String = {
    val (sinkRelNodes, optimizedRelNodes, execGraph, streamGraph) = getExplainGraphs(operations)

    val sb = new StringBuilder
    sb.append("== Abstract Syntax Tree ==")
    sb.append(System.lineSeparator)
    sinkRelNodes.foreach { sink =>
      // use EXPPLAN_ATTRIBUTES to make the ast result more readable
      // and to keep the previous behavior
      sb.append(FlinkRelOptUtil.toString(sink, SqlExplainLevel.EXPPLAN_ATTRIBUTES))
      sb.append(System.lineSeparator)
    }

    sb.append("== Optimized Physical Plan ==")
    sb.append(System.lineSeparator)
    val explainLevel = if (extraDetails.contains(ExplainDetail.ESTIMATED_COST)) {
      SqlExplainLevel.ALL_ATTRIBUTES
    } else {
      SqlExplainLevel.DIGEST_ATTRIBUTES
    }
    val withChangelogTraits = extraDetails.contains(ExplainDetail.CHANGELOG_MODE)
    optimizedRelNodes.foreach { rel =>
      sb.append(FlinkRelOptUtil.toString(
        rel,
        explainLevel,
        withChangelogTraits = withChangelogTraits))
      sb.append(System.lineSeparator)
    }

    sb.append("== Optimized Execution Plan ==")
    sb.append(System.lineSeparator)
    sb.append(ExecNodePlanDumper.dagToString(execGraph))

    if (extraDetails.contains(ExplainDetail.JSON_EXECUTION_PLAN)) {
      sb.append(System.lineSeparator)
      sb.append("== Physical Execution Plan ==")
      sb.append(System.lineSeparator)
      sb.append(streamGraph.getStreamingPlanAsJSON)
    }

    sb.toString()
  }

  private def createDummyPlanner(): StreamPlanner = {
    val dummyExecEnv = new DummyStreamExecutionEnvironment(getExecEnv)
    val executor = new DefaultExecutor(dummyExecEnv)
    new StreamPlanner(executor, config, moduleManager, functionCatalog, catalogManager)
  }

  override def explainJsonPlan(jsonPlan: String, extraDetails: ExplainDetail*): String = {
    validateAndOverrideConfiguration()
    val execGraph = ExecNodeGraph.createExecNodeGraph(jsonPlan, createSerdeContext)
    val transformations = translateToPlan(execGraph)
    cleanupInternalConfigurations()

    val streamGraph = executor.createPipeline(transformations, config.getConfiguration, null)
      .asInstanceOf[StreamGraph]

    val sb = new StringBuilder
    sb.append("== Optimized Execution Plan ==")
    sb.append(System.lineSeparator)
    sb.append(ExecNodePlanDumper.dagToString(execGraph))

    if (extraDetails.contains(ExplainDetail.JSON_EXECUTION_PLAN)) {
      sb.append(System.lineSeparator)
      sb.append("== Physical Execution Plan ==")
      sb.append(System.lineSeparator)
      sb.append(streamGraph.getStreamingPlanAsJSON)
    }

    sb.toString()
  }

  override def validateAndOverrideConfiguration(): Unit = {
    super.validateAndOverrideConfiguration()
    val runtimeMode = getConfiguration.get(ExecutionOptions.RUNTIME_MODE)
    if (runtimeMode != RuntimeExecutionMode.STREAMING) {
      throw new IllegalArgumentException(
        "Mismatch between configured runtime mode and actual runtime mode. " +
          "Currently, the 'execution.runtime-mode' can only be set when instantiating the " +
          "table environment. Subsequent changes are not supported. " +
          "Please instantiate a new TableEnvironment if necessary."
      )
    }
  }
}
