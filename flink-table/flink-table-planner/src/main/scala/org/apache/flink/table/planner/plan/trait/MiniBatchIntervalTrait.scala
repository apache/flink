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

package org.apache.flink.table.planner.plan.`trait`

import org.apache.calcite.plan.{RelOptPlanner, RelTrait, RelTraitDef}

/**
  * The MiniBatchIntervalTrait is used to describe how the elements are divided into batches
  * when flowing out from a [[org.apache.calcite.rel.RelNode]],
  * e,g,. MiniBatchIntervalTrait(1000L, ProcTime)
  * means elements are divided into 1000ms proctime mini batches.
  */
class MiniBatchIntervalTrait(miniBatchInterval: MiniBatchInterval) extends RelTrait {

  def getMiniBatchInterval: MiniBatchInterval = miniBatchInterval

  override def getTraitDef: RelTraitDef[_ <: RelTrait] = MiniBatchIntervalTraitDef.INSTANCE

  override def satisfies(`trait`: RelTrait): Boolean = this.equals(`trait`)

  override def register(planner: RelOptPlanner): Unit = {}

  override def hashCode(): Int = {
    miniBatchInterval
      .getInterval
      .hashCode()
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case eTrait: MiniBatchIntervalTrait =>
        this.getMiniBatchInterval == eTrait.getMiniBatchInterval
      case _ => false
    }
  }

  override def toString: String = miniBatchInterval.getMode + ": " + miniBatchInterval.getInterval
}

object MiniBatchIntervalTrait {
  val NONE = new MiniBatchIntervalTrait(MiniBatchInterval.NONE)
  val NO_MINIBATCH = new MiniBatchIntervalTrait(MiniBatchInterval.NO_MINIBATCH)
}
