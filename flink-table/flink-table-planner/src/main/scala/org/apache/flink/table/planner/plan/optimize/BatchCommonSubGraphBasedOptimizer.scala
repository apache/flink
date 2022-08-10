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
package org.apache.flink.table.planner.plan.optimize

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.planner.calcite.{FlinkRelBuilder, RexFactory}
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.calcite.{LegacySink, Sink}
import org.apache.flink.table.planner.plan.optimize.program.{BatchOptimizeContext, FlinkBatchProgram}
import org.apache.flink.table.planner.plan.schema.IntermediateRelTable
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext
import org.apache.flink.table.planner.utils.TableConfigUtils
import org.apache.flink.util.Preconditions

import org.apache.calcite.rel.RelNode

import java.util.Collections

/** A [[CommonSubGraphBasedOptimizer]] for Batch. */
class BatchCommonSubGraphBasedOptimizer(planner: BatchPlanner)
  extends CommonSubGraphBasedOptimizer {

  override protected def doOptimize(roots: Seq[RelNode]): Seq[RelNodeBlock] = {
    // build RelNodeBlock plan
    val rootBlocks =
      RelNodeBlockPlanBuilder.buildRelNodeBlockPlan(roots, planner.getTableConfig)
    // optimize recursively RelNodeBlock
    rootBlocks.foreach(optimizeBlock)
    rootBlocks
  }

  private def optimizeBlock(block: RelNodeBlock): Unit = {
    block.children.foreach {
      child =>
        if (child.getNewOutputNode.isEmpty) {
          optimizeBlock(child)
        }
    }

    val originTree = block.getPlan
    val optimizedTree = optimizeTree(originTree)

    optimizedTree match {
      case _: LegacySink | _: Sink => // ignore
      case _ =>
        val name = createUniqueIntermediateRelTableName
        val intermediateRelTable =
          new IntermediateRelTable(Collections.singletonList(name), optimizedTree)
        val newTableScan = wrapIntermediateRelTableToTableScan(intermediateRelTable, name)
        block.setNewOutputNode(newTableScan)
        block.setOutputTableName(name)
    }
    block.setOptimizedPlan(optimizedTree)
  }

  /**
   * Generates the optimized [[RelNode]] tree from the original relational node tree.
   *
   * @param relNode
   *   The original [[RelNode]] tree
   * @return
   *   The optimized [[RelNode]] tree
   */
  private def optimizeTree(relNode: RelNode): RelNode = {
    val tableConfig = planner.getTableConfig
    val programs = TableConfigUtils
      .getCalciteConfig(tableConfig)
      .getBatchProgram
      .getOrElse(FlinkBatchProgram.buildProgram(tableConfig))
    Preconditions.checkNotNull(programs)

    val context = unwrapContext(relNode)

    programs.optimize(
      relNode,
      new BatchOptimizeContext {

        override def isBatchMode: Boolean = true

        override def getTableConfig: TableConfig = tableConfig

        override def getFunctionCatalog: FunctionCatalog = planner.functionCatalog

        override def getCatalogManager: CatalogManager = planner.catalogManager

        override def getModuleManager: ModuleManager = planner.moduleManager

        override def getRexFactory: RexFactory = context.getRexFactory

        override def getFlinkRelBuilder: FlinkRelBuilder = planner.createRelBuilder

        override def needFinalTimeIndicatorConversion: Boolean = true

        override def getClassLoader: ClassLoader = context.getClassLoader
      }
    )
  }

}
