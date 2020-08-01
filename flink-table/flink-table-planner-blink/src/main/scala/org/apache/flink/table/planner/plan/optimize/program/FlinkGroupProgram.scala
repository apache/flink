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

package org.apache.flink.table.planner.plan.optimize.program

import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.table.planner.utils.Logging
import org.apache.flink.util.Preconditions

import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

/**
  * A FlinkOptimizeProgram that contains a sequence of sub-[[FlinkOptimizeProgram]]s as a group.
  * Programs in the group will be executed in sequence,
  * and the group will be executed `iterations` times.
  *
  * @tparam OC OptimizeContext
  */
class FlinkGroupProgram[OC <: FlinkOptimizeContext] extends FlinkOptimizeProgram[OC] with Logging {

  /**
    * Sub-programs in this program.
    */
  private val programs = new util.ArrayList[(FlinkOptimizeProgram[OC], String)]()

  /**
    * Repeat execution times for sub-programs as a group.
    */
  private var iterations = 1

  override def optimize(root: RelNode, context: OC): RelNode = {
    if (programs.isEmpty) {
      return root
    }

    (0 until iterations).foldLeft(root) {
      case (input, i) =>
        if (LOG.isDebugEnabled) {
          LOG.debug(s"iteration: ${i + 1}")
        }
        programs.foldLeft(input) {
          case (currentInput, (program, description)) =>
            val start = System.currentTimeMillis()
            val result = program.optimize(currentInput, context)
            val end = System.currentTimeMillis()

            if (LOG.isDebugEnabled) {
              LOG.debug(s"optimize $description cost ${end - start} ms.\n" +
                s"optimize result:\n ${FlinkRelOptUtil.toString(result)}")
            }
            result
        }
    }
  }

  def addProgram(program: FlinkOptimizeProgram[OC], description: String = ""): Unit = {
    Preconditions.checkNotNull(program)
    val desc = if (description != null) description else ""
    programs.add((program, desc))
  }

  def setIterations(iterations: Int): Unit = {
    Preconditions.checkArgument(iterations > 0)
    this.iterations = iterations
  }
}

class FlinkGroupProgramBuilder[OC <: FlinkOptimizeContext] {
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
  def newBuilder[OC <: FlinkOptimizeContext] = new FlinkGroupProgramBuilder[OC]
}
