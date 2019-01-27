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

import org.apache.flink.table.plan.metadata.FlinkRelMdNonCumulativeCost
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.RelTrait
import org.apache.calcite.plan.hep.{HepPlanner, HepProgram}
import org.apache.calcite.rel.RelNode

/**
  * A FlinkOptimizeProgram that runs with [[HepPlanner]].
  *
  * <p>In most case [[FlinkHepRuleSetProgram]] could meet our requirements.
  * Otherwise we could choose this program for some advanced features,
  * and use [[org.apache.calcite.plan.hep.HepProgramBuilder]] to create [[HepProgram]].
  *
  * @tparam OC OptimizeContext
  */
class FlinkHepProgram[OC <: OptimizeContext] extends FlinkOptimizeProgram[OC] {

  private var hepProgram: HepProgram = _
  private var targetTraits = Array.empty[RelTrait]

  override def optimize(input: RelNode, context: OC): RelNode = {
    Preconditions.checkNotNull(hepProgram)
    try {
      val planner = new HepPlanner(hepProgram, context.getContext)
      FlinkRelMdNonCumulativeCost.THREAD_PLANNER.set(planner)
      planner.setRoot(input)

      if (targetTraits.nonEmpty) {
        val targetTraitSet = input.getTraitSet.plusAll(targetTraits)
        if (!input.getTraitSet.equals(targetTraitSet)) {
          planner.changeTraits(input, targetTraitSet.simplify)
        }
      }
      planner.findBestExp
    } finally {
      FlinkRelMdNonCumulativeCost.THREAD_PLANNER.remove()
    }

  }

  /**
    * Sets target traits that the optimized relational expression should contain them.
    */
  def setTargetTraits(relTraits: Array[RelTrait]): Unit = {
    if (relTraits != null) {
      targetTraits = relTraits
    } else {
      targetTraits = Array.empty[RelTrait]
    }
  }

  /**
    * Sets hep program instance.
    */
  def setHepProgram(hepProgram: HepProgram): Unit = {
    this.hepProgram = hepProgram
  }

}

object FlinkHepProgram {

  def apply[OC <: OptimizeContext](
    hepProgram: HepProgram,
    targetTraits: Array[RelTrait] = Array.empty[RelTrait])
  : FlinkHepProgram[OC] = {

    val flinkHepProgram = new FlinkHepProgram[OC]()
    flinkHepProgram.setHepProgram(hepProgram)
    flinkHepProgram.setTargetTraits(targetTraits)
    flinkHepProgram
  }
}
