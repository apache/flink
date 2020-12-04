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

package org.apache.flink.table.planner.plan.nodes.physical

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, ExecNodeBase}

import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

/**
  * Base class for flink physical relational expression.
  */
trait FlinkPhysicalRel extends FlinkRelNode {

  /**
   * The translated [[ExecNode]], this could make sure that
   * one physical RelNode can be translated into one [[ExecNode]].
   */
  private var execNode: ExecNode[_] = _

  /**
    * Try to satisfy required traits by descendant of current node. If descendant can satisfy
    * required traits, and current node will not destroy it, then returns the new node with
    * converted inputs.
    *
    * @param requiredTraitSet required traits
    * @return A converted node which satisfy required traits by inputs node of current node.
    *         Returns None if required traits cannot be satisfied.
    */
  def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = None

  /**
   * Translate this physical RelNode into an [[ExecNode]].
   */
  def translateToExecNode(): ExecNode[_] = {
    if (execNode == null) {
      execNode = translateToExecNodeInternal()
    }
    execNode
  }

  /**
   * Internal method, translates this physical RelNode into an [[ExecNode]].
   * TODO just keep the method definition and
   *      remove the implementation after all sub-classes do not extend from ExecNode.
   */
  def translateToExecNodeInternal(): ExecNode[_] = {
    val inputNodes: util.List[ExecNode[_]] = getInputs.map {
      case i: FlinkPhysicalRel => i.translateToExecNode()
      case _ => throw new TableException("This should not happen.")
    }.toList

    this match {
      case e: ExecNodeBase[_, _] =>
        e.setInputNodes(inputNodes)
        e
      case _ =>
        // NOTE: the sub-classes which do not extend from ExecNode should overwrite this method
        this.translateToExecNode()
    }
  }
}
