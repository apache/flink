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

package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan.{RelOptPlanner, RelTrait, RelTraitDef}
import org.apache.flink.table.plan.nodes.datastream.AccMode.AccMode

/** Tracks if a [[org.apache.calcite.rel.RelNode]] needs to send update and delete changes as
  * retraction messages.
  */
class UpdateAsRetractionTrait extends RelTrait {

  /**
    * Defines whether the [[org.apache.calcite.rel.RelNode]] needs to send update and delete
    * changes as retraction messages.
    */
  private var updateAsRetraction: Boolean = false

  def this(updateAsRetraction: Boolean) {
    this()
    this.updateAsRetraction = updateAsRetraction
  }

  def sendsUpdatesAsRetractions: Boolean = updateAsRetraction

  override def register(planner: RelOptPlanner): Unit = { }

  override def getTraitDef: RelTraitDef[_ <: RelTrait] = UpdateAsRetractionTraitDef.INSTANCE

  override def satisfies(`trait`: RelTrait): Boolean = this.equals(`trait`)

  override def toString: String = updateAsRetraction.toString

}

object UpdateAsRetractionTrait {
  val DEFAULT = new UpdateAsRetractionTrait(false)
}

/**
  * Tracks the AccMode of a [[org.apache.calcite.rel.RelNode]].
  */
class AccModeTrait extends RelTrait {

  /** Defines the accumulating mode for a operator. */
  private var accMode = AccMode.Acc

  def this(accMode: AccMode) {
    this()
    this.accMode = accMode
  }

  def getAccMode: AccMode = accMode

  override def register(planner: RelOptPlanner): Unit = { }

  override def getTraitDef: RelTraitDef[_ <: RelTrait] = AccModeTraitDef.INSTANCE

  override def satisfies(`trait`: RelTrait): Boolean = this.equals(`trait`)

  override def toString: String = accMode.toString
}

object AccModeTrait {
  val DEFAULT = new AccModeTrait(AccMode.Acc)
}

/**
  * The AccMode indicates which kinds of messages a [[org.apache.calcite.rel.RelNode]] might
  * produce.
  * In [[AccMode.Acc]] the node only emit accumulate messages.
  * In [[AccMode.AccRetract]], the node produces accumulate messages for insert changes,
  * retraction messages for delete changes, and accumulate and retraction messages
  * for update changes.
  */
object AccMode extends Enumeration {
  type AccMode = Value

  val Acc        = Value // Operator produces only accumulate (insert) messages
  val AccRetract = Value // Operator produces accumulate (insert, update) and
                         //   retraction (delete, update) messages
}


