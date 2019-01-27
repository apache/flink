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

package org.apache.flink.table.plan.subplan

import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.plan.logical.LogicalNode
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSink
import org.apache.flink.table.plan.schema.IntermediateRelNodeTable

import org.apache.calcite.rel.RelNode

/**
  * DAG optimizer for Batch.
  */
object BatchDAGOptimizer extends AbstractDAGOptimizer[BatchTableEnvironment] {

  override protected def doOptimize(
      sinks: Seq[LogicalNode],
      tEnv: BatchTableEnvironment): Seq[RelNodeBlock] = {
    // build RelNodeBlock plan
    val relNodeBlocks = RelNodeBlockPlanBuilder.buildRelNodeBlockPlan(sinks, tEnv)
    // optimize recursively RelNodeBlock
    relNodeBlocks.foreach(block => optimizeBlock(block, tEnv))
    relNodeBlocks
  }

  private def optimizeBlock(block: RelNodeBlock, tEnv: BatchTableEnvironment): Unit = {
    block.children.foreach { child =>
      if (child.getNewOutputNode.isEmpty) {
        optimizeBlock(child, tEnv)
      }
    }

    val originTree = block.getPlan
    val optimizedTree = tEnv.optimize(originTree)

    optimizedTree match {
      case _: BatchExecSink[_] => // ignore
      case _ =>
        val name = tEnv.createUniqueTableName()
        registerIntermediateTable(tEnv, name, optimizedTree)
        val newTable = tEnv.scan(name)
        block.setNewOutputNode(newTable.getRelNode)
        block.setOutputTableName(name)
    }
    block.setOptimizedPlan(optimizedTree)
  }

  private def registerIntermediateTable(
      tEnv: BatchTableEnvironment,
      name: String,
      relNode: RelNode): Unit = {
    val table = new IntermediateRelNodeTable(relNode)
    tEnv.registerTableInternal(name, table)
  }

}
