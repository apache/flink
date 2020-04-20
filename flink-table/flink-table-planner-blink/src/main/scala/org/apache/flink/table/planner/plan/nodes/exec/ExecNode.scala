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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.operators.StreamOperatorFactory
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, TwoInputTransformation}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.delegation.Planner
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel

import org.apache.calcite.rel.RelDistribution
import org.apache.calcite.rel.core.Exchange

import java.util

import scala.collection.JavaConversions._

/**
  * The representation of execution information for a [[FlinkPhysicalRel]].
  *
  * @tparam E The Planner
  * @tparam T The type of the elements that result from this [[Transformation]]
  */
trait ExecNode[E <: Planner, T] {

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
  def translateToPlan(planner: E): Transformation[T] = {
    if (transformation == null) {
      transformation = translateToPlanInternal(planner)
    }
    transformation
  }

  /**
    * Internal method, translates this node into a Flink operator.
    *
    * @param planner The [[Planner]] of the translated Table.
    */
  protected def translateToPlanInternal(planner: E): Transformation[T]

  /**
    * Returns an array of this node's inputs. If there are no inputs,
    * returns an empty list, not null.
    *
    * @return Array of this node's inputs
    */
  def getInputNodes: util.List[ExecNode[E, _]]

  /**
    * Replaces the <code>ordinalInParent</code><sup>th</sup> input.
    * You must override this method if you override [[getInputNodes]].
    *
    * @param ordinalInParent Position of the child input, 0 is the first
    * @param newInputNode New node that should be put at position ordinalInParent
    */
  def replaceInputNode(ordinalInParent: Int, newInputNode: ExecNode[E, _]): Unit

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

object ExecNode {

  /**
    * Set memoryBytes to Transformation.setManagedMemoryWeight.
    */
  def setManagedMemoryWeight[T](
      transformation: Transformation[T],
      memoryBytes: Long = 0): Transformation[T] = {
    if (transformation.getManagedMemoryWeight != Transformation.DEFAULT_MANAGED_MEMORY_WEIGHT) {
      throw new TableException("Managed memory weight has been set, this should not happen.")
    }

    // Using Bytes can easily overflow
    // Using KibiBytes to cast to int
    // Careful about zero
    val memoryKibiBytes = if (memoryBytes == 0) 0 else Math.max(1, (memoryBytes >> 10).toInt)
    transformation.setManagedMemoryWeight(memoryKibiBytes)
    transformation
  }

  /**
    * Create a [[OneInputTransformation]] with memoryBytes.
    */
  def createOneInputTransformation[T](
      input: Transformation[T],
      name: String,
      operatorFactory: StreamOperatorFactory[T],
      outputType: TypeInformation[T],
      parallelism: Int,
      memoryBytes: Long = 0): OneInputTransformation[T, T] = {
    val ret = new OneInputTransformation[T, T](
      input, name, operatorFactory, outputType, parallelism)
    setManagedMemoryWeight(ret, memoryBytes)
    ret
  }

  /**
    * Create a [[TwoInputTransformation]] with memoryBytes.
    */
  def createTwoInputTransformation[T](
      input1: Transformation[T],
      input2: Transformation[T],
      name: String,
      operatorFactory: StreamOperatorFactory[T],
      outputType: TypeInformation[T],
      parallelism: Int,
      memoryBytes: Long = 0): TwoInputTransformation[T, T, T] = {
    val ret = new TwoInputTransformation[T, T, T](
      input1, input2, name, operatorFactory, outputType, parallelism)
    setManagedMemoryWeight(ret, memoryBytes)
    ret
  }
}
