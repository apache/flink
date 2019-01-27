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

import org.apache.calcite.rel.RelNode

/**
  * Defines an optimizer to optimize a DAG instead of a simple tree.
  *
  * @tparam E The TableEnvironment
  */
trait DAGOptimizer[E <: TableEnvironment] {

  /**
    * Optimize [[LogicalNode]] DAG to [[RelNode]] DAG.
    * NOTES: the reused node in result DAG will be converted to the same RelNode.
    *
    * @param sinks A DAG which is composed by [[LogicalNode]]
    * @param tEnv The TableEnvironment
    * @return a list of RelNode represents an optimized RelNode DAG.
    */
  def optimize(sinks: Seq[LogicalNode], tEnv: E): Seq[RelNode]
}
