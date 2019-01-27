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

import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.plan.logical.LogicalNode
import org.apache.flink.table.plan.schema.IntermediateRelNodeTable

import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.{RelNode, RelShuttleImpl}

import java.util

import scala.collection.JavaConversions._

/**
  * Base class for every [[DAGOptimizer]].
  *
  * @tparam E The TableEnvironment
  */
abstract class AbstractDAGOptimizer[E <: TableEnvironment] extends DAGOptimizer[E] {

  /**
    * Optimize [[LogicalNode]] DAG to [[RelNode]] DAG(doesn't contain [[IntermediateRelNodeTable]]).
    * NOTES: the reused node in result DAG will be converted to the same RelNode.
    *
    * @param sinks A DAG which is composed by [[LogicalNode]]
    * @param tEnv The TableEnvironment
    * @return a list of RelNode represents an optimized RelNode DAG.
    */
  override def optimize(sinks: Seq[LogicalNode], tEnv: E): Seq[RelNode] = {
    val relNodeBlocks = doOptimize(sinks, tEnv)
    expandIntermediateTableScan(relNodeBlocks.map(_.getOptimizedPlan))
  }

  /**
    * Decompose RelNode DAG into multiple [[RelNodeBlock]]s, optimize recursively each
    * [[RelNodeBlock]], return optimized [[RelNodeBlock]]s.
    *
    * @return optimized [[RelNodeBlock]]s.
    */
  protected def doOptimize(sinks: Seq[LogicalNode], tEnv: E): Seq[RelNodeBlock]

  /**
    * Expand [[IntermediateRelNodeTable]] in each RelNodeBlock.
    */
  private def expandIntermediateTableScan(nodes: Seq[RelNode]): Seq[RelNode] = {

    class ExpandShuttle extends RelShuttleImpl {

      // ensure the same intermediateTable would be expanded to the same RelNode tree.
      private val expandedIntermediateTables =
        new util.IdentityHashMap[IntermediateRelNodeTable, RelNode]()

      override def visit(scan: TableScan): RelNode = {
        val intermediateTable = scan.getTable.unwrap(classOf[IntermediateRelNodeTable])
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
