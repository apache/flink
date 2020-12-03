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

package org.apache.flink.table.planner.plan.nodes.exec

import org.apache.flink.api.dag.Transformation
import org.apache.flink.table.delegation.Planner
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel

import java.util

/**
  * The representation of execution information for a [[FlinkPhysicalRel]].
  *
  * @tparam T The type of the elements that result from this [[Transformation]]
  */
trait ExecNode[T] {

  /**
    * Translates this node into a Flink operator.
    *
    * <p>NOTE: returns same translate result if called multiple times.
    *
    * @param planner The [[Planner]] of the translated Table.
    */
  def translateToPlan(planner: Planner): Transformation[T]

  /**
    * Returns a list of this node's input nodes. If there are no inputs,
    * returns an empty list, not null.
    *
    * @return List of this node's input nodes
    */
  def getInputNodes: util.List[ExecNode[_]]

  /**
   * Returns a list of this node's input edges. If there are no inputs,
   * returns an empty list, not null.
   *
   * @return List of this node's input edges
   */
  def getInputEdges: util.List[ExecEdge]

  /**
    * Replaces the <code>ordinalInParent</code><sup>th</sup> input.
    * You must override this method if you override [[getInputNodes]].
    *
    * @param ordinalInParent Position of the child input, 0 is the first
    * @param newInputNode New node that should be put at position ordinalInParent
    */
  def replaceInputNode(ordinalInParent: Int, newInputNode: ExecNode[_]): Unit

  /**
    * Accepts a visit from a [[ExecNodeVisitor]].
    *
    * @param visitor ExecNodeVisitor
    */
  def accept(visitor: ExecNodeVisitor)

}
