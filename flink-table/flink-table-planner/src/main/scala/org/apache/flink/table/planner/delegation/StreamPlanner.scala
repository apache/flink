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
import org.apache.flink.table.api.PlanReference.{ContentPlanReference, FilePlanReference, ResourcePlanReference}
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.api.{ExplainDetail, PlanReference, TableConfig, TableException}
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}
import org.apache.flink.table.delegation.{Executor, InternalPlan}
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.operations.{ModifyOperation, Operation}
import org.apache.flink.table.planner.plan.ExecNodeGraphInternalPlan
import org.apache.flink.table.planner.plan.`trait`._
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph
import org.apache.flink.table.planner.plan.nodes.exec.processor.ExecNodeGraphProcessor
import org.apache.flink.table.planner.plan.nodes.exec.serde.{JsonSerdeUtil, SerdeContext}
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodePlanDumper
import org.apache.flink.table.planner.plan.optimize.{Optimizer, StreamCommonSubGraphBasedOptimizer}
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.table.planner.utils.DummyStreamExecutionEnvironment

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader

import org.apache.calcite.plan.{ConventionTraitDef, RelTrait, RelTraitDef}
import org.apache.calcite.sql.SqlExplainLevel

import java.io.{File, IOException}
import java.util

import _root_.scala.collection.JavaConversions._

class StreamPlanner(
    executor: Executor,
    tableConfig: TableConfig,
    moduleManager: ModuleManager,
    functionCatalog: FunctionCatalog,
    catalogManager: CatalogManager)
  extends PlannerBase(executor, tableConfig, moduleManager, functionCatalog, catalogManager,
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
    beforeTranslation()
    val planner = createDummyPlanner()
    val transformations = execGraph.getRootNodes.map {
      case node: StreamExecNode[_] => node.translateToPlan(planner)
      case _ =>
        throw new TableException("Cannot generate DataStream due to an invalid logical plan. " +
          "This is a bug and should not happen. Please file an issue.")
    }
    afterTranslation()
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
    new StreamPlanner(executor, tableConfig, moduleManager, functionCatalog, catalogManager)
  }

  override def loadPlan(planReference: PlanReference): InternalPlan = {
    val ctx = createSerdeContext
    val objectReader: ObjectReader = JsonSerdeUtil.createObjectReader(ctx)
    val execNodeGraph = planReference match {
      case filePlanReference: FilePlanReference =>
        objectReader.readValue(filePlanReference.getFile, classOf[ExecNodeGraph])
      case contentPlanReference: ContentPlanReference =>
        objectReader.readValue(contentPlanReference.getContent, classOf[ExecNodeGraph])
      case resourcePlanReference: ResourcePlanReference =>
        val url = resourcePlanReference.getClassLoader
          .getResource(resourcePlanReference.getResourcePath)
        if (url == null) {
          throw new IOException(
            "Cannot load the plan reference from classpath: " + planReference)
        }
        objectReader.readValue(new File(url.toURI), classOf[ExecNodeGraph])
      case _ => throw new IllegalStateException(
        "Unknown PlanReference. This is a bug, please contact the developers")
    }

    new ExecNodeGraphInternalPlan(
      JsonSerdeUtil.createObjectWriter(ctx)
        .withDefaultPrettyPrinter()
        .writeValueAsString(execNodeGraph),
      execNodeGraph)
  }

  override def compilePlan(
     modifyOperations: util.List[ModifyOperation]): InternalPlan = {
    beforeTranslation()
    val relNodes = modifyOperations.map(translateToRel)
    val optimizedRelNodes = optimize(relNodes)
    val execGraph = translateToExecNodeGraph(optimizedRelNodes)
    afterTranslation()

    new ExecNodeGraphInternalPlan(
      JsonSerdeUtil.createObjectWriter(createSerdeContext)
        .withDefaultPrettyPrinter()
        .writeValueAsString(execGraph),
      execGraph)
  }

  override def translatePlan(plan: InternalPlan): util.List[Transformation[_]] = {
    beforeTranslation()
    val execGraph = plan.asInstanceOf[ExecNodeGraphInternalPlan].getExecNodeGraph
    val transformations = translateToPlan(execGraph)
    afterTranslation()
    transformations
  }

  override def explainPlan(plan: InternalPlan, extraDetails: ExplainDetail*): String = {
    beforeTranslation()
    val execGraph = plan.asInstanceOf[ExecNodeGraphInternalPlan].getExecNodeGraph
    val transformations = translateToPlan(execGraph)
    afterTranslation()

    val streamGraph = executor.createPipeline(transformations, tableConfig.getConfiguration, null)
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

  override def beforeTranslation(): Unit = {
    super.beforeTranslation()
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
