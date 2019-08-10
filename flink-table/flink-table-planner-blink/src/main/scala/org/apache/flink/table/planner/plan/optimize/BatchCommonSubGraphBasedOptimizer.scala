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
import org.apache.flink.table.catalog.FunctionCatalog
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecSink
import org.apache.flink.table.planner.plan.optimize.program.{BatchOptimizeContext, FlinkBatchProgram}
import org.apache.flink.table.planner.plan.schema.IntermediateRelTable
import org.apache.flink.table.planner.utils.TableConfigUtils
import org.apache.flink.util.Preconditions

import org.apache.calcite.rel.RelNode

/**
  * A [[CommonSubGraphBasedOptimizer]] for Batch.
  */
class BatchCommonSubGraphBasedOptimizer(planner: BatchPlanner)
  extends CommonSubGraphBasedOptimizer {

  override protected def doOptimize(roots: Seq[RelNode]): Seq[RelNodeBlock] = {
    // build RelNodeBlock plan
    val rootBlocks = RelNodeBlockPlanBuilder.buildRelNodeBlockPlan(roots, planner.getTableConfig)
    // optimize recursively RelNodeBlock
    rootBlocks.foreach(optimizeBlock)
    rootBlocks
  }

  private def optimizeBlock(block: RelNodeBlock): Unit = {
    block.children.foreach { child =>
      if (child.getNewOutputNode.isEmpty) {
        optimizeBlock(child)
      }
    }

    val originTree = block.getPlan
    val optimizedTree = optimizeTree(originTree)

    optimizedTree match {
      case _: BatchExecSink[_] => // ignore
      case _ =>
        val name = createUniqueIntermediateRelTableName
        val intermediateRelTable =  new IntermediateRelTable(optimizedTree)
        val newTableScan = wrapIntermediateRelTableToTableScan(intermediateRelTable, name)
        block.setNewOutputNode(newTableScan)
        block.setOutputTableName(name)
    }
    block.setOptimizedPlan(optimizedTree)
  }

  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param relNode The original [[RelNode]] tree
    * @return The optimized [[RelNode]] tree
    */
  private def optimizeTree(relNode: RelNode): RelNode = {
    val config = planner.getTableConfig
    val programs = TableConfigUtils.getCalciteConfig(config).getBatchProgram
      .getOrElse(FlinkBatchProgram.buildProgram(config.getConfiguration))
    Preconditions.checkNotNull(programs)

    programs.optimize(relNode, new BatchOptimizeContext {
      override def getTableConfig: TableConfig = config

      override def getFunctionCatalog: FunctionCatalog = planner.functionCatalog
    })
  }

}
