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
  * The [[AccMode]] determines how insert, update, and delete changes of tables are encoded
  * by the messeages that an operator emits.
  */
object AccMode extends Enumeration {
  type AccMode = Value

  /**
    * An operator in [[Acc]] mode emits change messages as
    * [[org.apache.flink.table.runtime.types.CRow]] which encode a pair of (Boolean, Row).
    *
    * An operator in [[Acc]] mode may only produce update and delete messages, if the table has
    * a unique key and all key attributes are contained in the Row.
    *
    * Changes are encoded as follows:
    * - insert: (true, NewRow)
    * - update: (true, NewRow) // the Row includes the full unique key to identify the row to update
    * - delete: (false, OldRow) // the Row includes the full unique key to identify the row to
    * delete
    *
    */
  val Acc = Value

  /**
    * An operator in [[AccRetract]] mode emits change messages as
    * [[org.apache.flink.table.runtime.types.CRow]] which encode a pair of (Boolean, Row).
    *
    * Changes are encoded as follows:
    * - insert: (true, NewRow)
    * - update: (false, OldRow), (true, NewRow) // updates are encoded in two messages!
    * - delete: (false, OldRow)
    *
    */
  val AccRetract = Value
}


