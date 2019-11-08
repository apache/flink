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

import org.apache.calcite.rel.RelNode

/**
  * The query [[Optimizer]] that transforms relational expressions into
  * semantically equivalent relational expressions.
  */
trait Optimizer {

  /**
    * Generates the optimized [[RelNode]] DAG from the original relational nodes.
    * <p>NOTES: The reused node in result DAG will be converted to the same RelNode.
    *
    * @param roots the original relational nodes.
    * @return a list of RelNode represents an optimized RelNode DAG.
    */
  def optimize(roots: Seq[RelNode]): Seq[RelNode]
}
