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

import org.apache.flink.table.plan.schema.IntermediateRelTable

import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.{RelNode, RelShuttleImpl}

import java.util

import scala.collection.JavaConversions._

/**
  * Base class for [[Optimizer]].
  */
abstract class OptimizerBase extends Optimizer {

  /**
    * Generates the optimized [[RelNode]] DAG from the original relational nodes.
    * NOTES: the reused node in result DAG will be converted to the same RelNode,
    * and the result doesn't contain [[IntermediateRelTable]].
    *
    * @param roots the original relational nodes.
    * @return a list of RelNode represents an optimized RelNode DAG.
    */
  override def optimize(roots: Seq[RelNode]): Seq[RelNode] = {
    val sinkBlocks = doOptimize(roots)
    val optimizedPlan = sinkBlocks.map { block =>
      val plan = block.getOptimizedPlan
      require(plan != null)
      plan
    }
    expandIntermediateTableScan(optimizedPlan)
  }

  /**
    * Decompose RelNode trees into multiple [[RelNodeBlock]]s, optimize recursively each
    * [[RelNodeBlock]], return optimized [[RelNodeBlock]]s.
    *
    * @return optimized [[RelNodeBlock]]s.
    */
  protected def doOptimize(roots: Seq[RelNode]): Seq[RelNodeBlock]

  /**
    * Expand [[IntermediateRelTable]] in each RelNodeBlock.
    */
  private def expandIntermediateTableScan(nodes: Seq[RelNode]): Seq[RelNode] = {

    class ExpandShuttle extends RelShuttleImpl {

      // ensure the same intermediateTable would be expanded to the same RelNode tree.
      private val expandedIntermediateTables =
        new util.IdentityHashMap[IntermediateRelTable, RelNode]()

      override def visit(scan: TableScan): RelNode = {
        val intermediateTable = scan.getTable.unwrap(classOf[IntermediateRelTable])
        if (intermediateTable != null) {
          expandedIntermediateTables.getOrElseUpdate(intermediateTable, {
            val underlyingRelNode = intermediateTable.relNode
            underlyingRelNode.accept(this)
          })
        } else {
          scan
        }
      }
    }

    val shuttle = new ExpandShuttle
    nodes.map(_.accept(shuttle))
  }

}
