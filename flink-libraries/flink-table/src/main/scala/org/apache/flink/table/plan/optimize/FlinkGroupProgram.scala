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

import org.apache.calcite.rel.RelNode

import scala.collection.mutable

/**
  * A FlinkOptimizeProgram that contains a sequence of sub-[[FlinkOptimizeProgram]]s as a group.
  * Each program in the group will be executed in sequence,
  * and the group will be executed `iterations` times.
  *
  * @tparam OC OptimizeContext
  */
class FlinkGroupProgram[OC <: OptimizeContext] extends FlinkOptimizeProgram[OC] {

  private val programs = new mutable.ArrayBuffer[(FlinkOptimizeProgram[OC], String)]()
  private var iterations = 1 // default value

  override def optimize(input: RelNode, context: OC): RelNode = {
    if (programs.isEmpty) {
      return input
    }

    (0 until iterations).foldLeft(input) {
      case (root, _) =>
        programs.foldLeft(root) {
          case (currentInput, (program, desc)) => // desc for debug
            program.optimize(currentInput, context)
        }
    }
  }

  def addProgram(program: FlinkOptimizeProgram[OC], description: String = ""): Unit = {
    val desc = if (description != null) description else ""
    programs.append((program, desc))
  }

  def setIterations(iterations: Int): Unit = {
    require(iterations > 0)
    this.iterations = iterations
  }
}

class FlinkGroupProgramBuilder[OC <: OptimizeContext] {
  private val groupProgram = new FlinkGroupProgram[OC]

  def addProgram(
      program: FlinkOptimizeProgram[OC], description: String = ""): FlinkGroupProgramBuilder[OC] = {
    groupProgram.addProgram(program, description)
    this
  }

  def setIterations(iterations: Int): FlinkGroupProgramBuilder[OC] = {
    groupProgram.setIterations(iterations)
    this
  }

  def build(): FlinkGroupProgram[OC] = groupProgram

}

object FlinkGroupProgramBuilder {
  def newBuilder[OC <: OptimizeContext] = new FlinkGroupProgramBuilder[OC]
}
