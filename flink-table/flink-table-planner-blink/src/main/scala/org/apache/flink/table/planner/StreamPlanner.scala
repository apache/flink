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

package org.apache.flink.table.planner

import org.apache.flink.api.dag.Transformation
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}
import org.apache.flink.table.delegation.Executor
import org.apache.flink.table.executor.StreamExecutor
import org.apache.flink.table.operations.{ModifyOperation, Operation, QueryOperation}
import org.apache.flink.table.plan.`trait`._
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.nodes.process.DAGProcessContext
import org.apache.flink.table.plan.nodes.resource.parallelism.ParallelismProcessor
import org.apache.flink.table.plan.optimize.{Optimizer, StreamCommonSubGraphBasedOptimizer}
import org.apache.flink.table.plan.util.{ExecNodePlanDumper, FlinkRelOptUtil}
import org.apache.flink.table.util.PlanUtil

import org.apache.calcite.plan.{ConventionTraitDef, RelTrait, RelTraitDef}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.SqlExplainLevel

import java.util

import _root_.scala.collection.JavaConversions._

class StreamPlanner(
    executor: Executor,
    config: TableConfig,
    functionCatalog: FunctionCatalog,
    catalogManager: CatalogManager)
  extends PlannerBase(executor, config, functionCatalog, catalogManager) {

  override protected def getTraitDefs: Array[RelTraitDef[_ <: RelTrait]] = {
    Array(
      ConventionTraitDef.INSTANCE,
      FlinkRelDistributionTraitDef.INSTANCE,
      MiniBatchIntervalTraitDef.INSTANCE,
      UpdateAsRetractionTraitDef.INSTANCE,
      AccModeTraitDef.INSTANCE)
  }

  override protected def getOptimizer: Optimizer = new StreamCommonSubGraphBasedOptimizer(this)

  override private[flink] def translateToExecNodePlan(
      optimizedRelNodes: Seq[RelNode]): util.List[ExecNode[_, _]] = {
    val execNodePlan = super.translateToExecNodePlan(optimizedRelNodes)
    val context = new DAGProcessContext(this)
    new ParallelismProcessor().process(execNodePlan, context)
  }

  override protected def translateToPlan(
      execNodes: util.List[ExecNode[_, _]]): util.List[Transformation[_]] = {
    execNodes.map {
      case node: StreamExecNode[_] => node.translateToPlan(this)
      case _ =>
        throw new TableException("Cannot generate DataStream due to an invalid logical plan. " +
          "This is a bug and should not happen. Please file an issue.")
    }
  }

  override def explain(operations: util.List[Operation], extended: Boolean): String = {
    require(operations.nonEmpty, "operations should not be empty")
    val sinkRelNodes = operations.map {
      case queryOperation: QueryOperation =>
        getRelBuilder.queryOperation(queryOperation).build()
      case modifyOperation: ModifyOperation =>
        translateToRel(modifyOperation)
      case o => throw new TableException(s"Unsupported operation: ${o.getClass.getCanonicalName}")
    }
    val optimizedRelNodes = optimize(sinkRelNodes)
    val execNodes = translateToExecNodePlan(optimizedRelNodes)
    val transformations = translateToPlan(execNodes)
    val streamExecutor = new StreamExecutor(getExecEnv)
    streamExecutor.setTableConfig(getTableConfig)
    val streamGraph = streamExecutor.generateStreamGraph(transformations, "")
    val executionPlan = PlanUtil.explainStreamGraph(streamGraph)

    val sb = new StringBuilder
    sb.append("== Abstract Syntax Tree ==")
    sb.append(System.lineSeparator)
    sinkRelNodes.foreach { sink =>
      sb.append(FlinkRelOptUtil.toString(sink))
      sb.append(System.lineSeparator)
    }

    sb.append("== Optimized Logical Plan ==")
    sb.append(System.lineSeparator)
    val (explainLevel, withRetractTraits) = if (extended) {
      (SqlExplainLevel.ALL_ATTRIBUTES, true)
    } else {
      (SqlExplainLevel.EXPPLAN_ATTRIBUTES, false)
    }
    sb.append(ExecNodePlanDumper.dagToString(
      execNodes,
      explainLevel,
      withRetractTraits = withRetractTraits))
    sb.append(System.lineSeparator)

    sb.append("== Physical Execution Plan ==")
    sb.append(System.lineSeparator)
    sb.append(executionPlan)
    sb.toString()
  }
}
