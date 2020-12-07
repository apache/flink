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

import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils
import org.apache.flink.types.RowKind

import org.apache.calcite.plan.{RelOptPlanner, RelTrait, RelTraitDef}

/**
 * UpdateKindTrait is used to describe the kind of update operation.
 */
class UpdateKindTrait(val updateKind: UpdateKind) extends RelTrait {

  override def satisfies(relTrait: RelTrait): Boolean = relTrait match {
    case other: UpdateKindTrait =>
      // should totally match
      other.updateKind == this.updateKind
    case _ => false
  }

  override def getTraitDef: RelTraitDef[_ <: RelTrait] = UpdateKindTraitDef.INSTANCE

  override def register(planner: RelOptPlanner): Unit = {}

  override def hashCode(): Int = updateKind.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case t: UpdateKindTrait => this.updateKind.equals(t.updateKind)
    case _ => false
  }

  override def toString: String = s"[${updateKind.toString}]"
}

object UpdateKindTrait {

  /**
   * An [[UpdateKindTrait]] that describes the node doesn't provide any kind of updates
   * as a provided trait, or requires nothing about kind of updates as a required trait.
   *
   * <p>It also indicates that the [[ModifyKindSetTrait]] of current node doesn't contain
   * [[ModifyKind#UPDATE]] operation.
   */
  val NONE = new UpdateKindTrait(UpdateKind.NONE)

  /**
   * An [[UpdateKindTrait]] that describes the node produces update changes just as a
   * single row of [[org.apache.flink.types.RowKind#UPDATE_AFTER]]
   */
  val ONLY_UPDATE_AFTER = new UpdateKindTrait(UpdateKind.ONLY_UPDATE_AFTER)

  /**
   * An [[UpdateKindTrait]] that describes the node produces update changes consists of
   * a row of [[org.apache.flink.types.RowKind#UPDATE_BEFORE]] and
   * [[org.apache.flink.types.RowKind#UPDATE_AFTER]].
   */
  val BEFORE_AND_AFTER = new UpdateKindTrait(UpdateKind.BEFORE_AND_AFTER)

  /**
   * Returns ONLY_UPDATE_AFTER [[UpdateKindTrait]] if there is update changes.
   * Otherwise, returns NONE [[UpdateKindTrait]].
   */
  def onlyAfterOrNone(modifyKindSet: ModifyKindSet): UpdateKindTrait = {
    val updateKind = if (modifyKindSet.contains(ModifyKind.UPDATE)) {
      UpdateKind.ONLY_UPDATE_AFTER
    } else {
      UpdateKind.NONE
    }
    new UpdateKindTrait(updateKind)
  }

  /**
   * Returns BEFORE_AND_AFTER [[UpdateKindTrait]] if there is update changes.
   * Otherwise, returns NONE [[UpdateKindTrait]].
   */
  def beforeAfterOrNone(modifyKindSet: ModifyKindSet): UpdateKindTrait = {
    val updateKind = if (modifyKindSet.contains(ModifyKind.UPDATE)) {
      UpdateKind.BEFORE_AND_AFTER
    } else {
      UpdateKind.NONE
    }
    new UpdateKindTrait(updateKind)
  }

  /**
   * Creates an instance of [[UpdateKindTrait]] from the given [[ChangelogMode]].
   */
  def fromChangelogMode(changelogMode: ChangelogMode): UpdateKindTrait = {
    val hasUpdateBefore = changelogMode.contains(RowKind.UPDATE_BEFORE)
    val hasUpdateAfter = changelogMode.contains(RowKind.UPDATE_AFTER)
    (hasUpdateBefore, hasUpdateAfter) match {
      case (true, true) => BEFORE_AND_AFTER
      case (false, true) => ONLY_UPDATE_AFTER
      case (true, false) =>
        throw new IllegalArgumentException("Unsupported changelog mode: " +
          ChangelogPlanUtils.stringifyChangelogMode(Some(changelogMode)))
      case (false, false) => NONE
    }
  }
}
