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

import org.apache.flink.table.planner.plan.`trait`.MiniBatchMode.MiniBatchMode

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
      .interval
      .hashCode()
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case eTrait: MiniBatchIntervalTrait =>
        this.getMiniBatchInterval == eTrait.getMiniBatchInterval
      case _ => false
    }
  }

  override def toString: String = miniBatchInterval.mode + ": " + miniBatchInterval.interval
}

/**
  * @param interval interval of mini-batch
  * @param mode type of mini-batch: rowtime/proctime
  */
case class MiniBatchInterval(interval: Long, mode: MiniBatchMode)

object MiniBatchInterval {
  // default none value.
  val NONE = MiniBatchInterval(0L, MiniBatchMode.None)
  // specific for cases when there exists nodes require watermark but mini-batch interval of
  // watermark is disabled by force, e.g. existing window aggregate.
  // The difference between NONE AND NO_MINIBATCH is when merging with other miniBatchInterval,
  // NONE case yields other miniBatchInterval, while NO_MINIBATCH case yields NO_MINIBATCH.
  val NO_MINIBATCH = MiniBatchInterval(-1L, MiniBatchMode.None)
}

object MiniBatchIntervalTrait {
  val NONE = new MiniBatchIntervalTrait(MiniBatchInterval.NONE)
  val NO_MINIBATCH = new MiniBatchIntervalTrait(MiniBatchInterval.NO_MINIBATCH)
}

/**
  * The type of minibatch interval: rowtime or proctime.
  */
object MiniBatchMode extends Enumeration {
  type MiniBatchMode = Value
  /**
    * An operator in [[ProcTime]] mode requires watermarks emitted in proctime interval,
    * i.e., unbounded group agg with minibatch enabled.
    */
  val ProcTime = Value

  /**
    * An operator in [[RowTime]] mode requires watermarks extracted from elements,
    * and emitted in rowtime interval, e.g., window, window join...
    */
  val RowTime = Value

  /**
    * Default value, meaning no minibatch interval is required.
    */
  val None = Value
}
