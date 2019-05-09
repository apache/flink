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

package org.apache.flink.table.plan.optimize

import org.apache.flink.table.api.{BatchTableEnvironment, TableConfig}
import org.apache.flink.table.plan.optimize.program.{BatchOptimizeContext, FlinkBatchProgram}
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.rel.RelNode

/**
  * Query optimizer for Batch.
  */
class BatchOptimizer(tEnv: BatchTableEnvironment) extends Optimizer {

  override def optimize(roots: Seq[RelNode]): Seq[RelNode] = {
    // TODO optimize multi-roots as a whole DAG
    roots.map(optimizeTree)
  }

  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param relNode The original [[RelNode]] tree
    * @return The optimized [[RelNode]] tree
    */
  private def optimizeTree(relNode: RelNode): RelNode = {
    val config = tEnv.getConfig
    val programs = config.getCalciteConfig.getBatchProgram
      .getOrElse(FlinkBatchProgram.buildProgram(config.getConf))
    Preconditions.checkNotNull(programs)

    programs.optimize(relNode, new BatchOptimizeContext {
      override def getTableConfig: TableConfig = config

      override def getVolcanoPlanner: VolcanoPlanner = tEnv.getPlanner.asInstanceOf[VolcanoPlanner]
    })
  }

}
