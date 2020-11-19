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

import org.apache.calcite.rel.RelDistribution
import org.apache.calcite.rel.core.Exchange

import scala.collection.JavaConversions._

/**
 * Base class of [[ExecNode]]s.
 *
 * @tparam P The Planner
 * @tparam T The type of the elements that result from this [[Transformation]]
 */
trait ExecNodeBase[P <: Planner, T] extends ExecNode[T] {

  /**
   * The [[Transformation]] translated from this node.
   */
  private var transformation: Transformation[T] = _

  /**
   * Translates this node into a Flink operator.
   *
   * <p>NOTE: returns same translate result if called multiple times.
   *
   * @param planner The [[Planner]] of the translated Table.
   */
  def translateToPlan(planner: Planner): Transformation[T] = {
    if (transformation == null) {
      transformation = translateToPlanInternal(planner.asInstanceOf[P])
    }
    transformation
  }

  /**
   * Internal method, translates this node into a Flink operator.
   *
   * @param planner The [[Planner]] of the translated Table.
   */
  protected def translateToPlanInternal(planner: P): Transformation[T]

  /**
   * Accepts a visit from a [[ExecNodeVisitor]].
   *
   * @param visitor ExecNodeVisitor
   */
  def accept(visitor: ExecNodeVisitor): Unit = {
    visitor.visit(this)
  }

  /**
   *  Whether there is singleton exchange node as input.
   */
  protected def inputsContainSingleton(): Boolean = {
    getInputNodes.exists { node =>
      node.isInstanceOf[Exchange] &&
        node.asInstanceOf[Exchange].getDistribution.getType == RelDistribution.Type.SINGLETON
    }
  }
}
