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
import org.apache.flink.types.RowKind

import org.apache.calcite.plan.{RelOptPlanner, RelTrait, RelTraitDef}

/** DeleteKindTrait is used to describe the kind of delete operation. */
class DeleteKindTrait(val deleteKind: DeleteKind) extends RelTrait {

  override def satisfies(relTrait: RelTrait): Boolean = relTrait match {
    case other: DeleteKindTrait =>
      // should totally match
      other.deleteKind == this.deleteKind
    case _ => false
  }

  override def getTraitDef: RelTraitDef[_ <: RelTrait] = DeleteKindTraitDef.INSTANCE

  override def register(planner: RelOptPlanner): Unit = {}

  override def hashCode(): Int = deleteKind.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case t: DeleteKindTrait => this.deleteKind.equals(t.deleteKind)
    case _ => false
  }

  override def toString: String = s"[${deleteKind.toString}]"
}

object DeleteKindTrait {

  /** An [[DeleteKindTrait]] that indicates the node does not support delete operation. */
  val NONE = new DeleteKindTrait(DeleteKind.NONE)

  /** An [[DeleteKindTrait]] that indicates the node supports deletes by key only. */
  val DELETE_BY_KEY = new DeleteKindTrait(DeleteKind.DELETE_BY_KEY)

  /** An [[DeleteKindTrait]] that indicates the node produces requires deletes by full records. */
  val FULL_DELETE = new DeleteKindTrait(DeleteKind.FULL_DELETE)

  /**
   * Returns DELETE_BY_KEY [[DeleteKindTrait]] if there are delete changes. Otherwise, returns NONE
   * [[DeleteKindTrait]].
   */
  def deleteOnKeyOrNone(modifyKindSet: ModifyKindSet): DeleteKindTrait = {
    val deleteKind = if (modifyKindSet.contains(ModifyKind.DELETE)) {
      DeleteKind.DELETE_BY_KEY
    } else {
      DeleteKind.NONE
    }
    new DeleteKindTrait(deleteKind)
  }

  /**
   * Returns FULL_DELETE [[DeleteKindTrait]] if there are delete changes. Otherwise, returns NONE
   * [[DeleteKindTrait]].
   */
  def fullDeleteOrNone(modifyKindSet: ModifyKindSet): DeleteKindTrait = {
    val deleteKind = if (modifyKindSet.contains(ModifyKind.DELETE)) {
      DeleteKind.FULL_DELETE
    } else {
      DeleteKind.NONE
    }
    new DeleteKindTrait(deleteKind)
  }

  /** Creates an instance of [[DeleteKindTrait]] from the given [[ChangelogMode]]. */
  def fromChangelogMode(changelogMode: ChangelogMode): DeleteKindTrait = {
    val hasDelete = changelogMode.contains(RowKind.DELETE)
    if (!hasDelete) {
      DeleteKindTrait.NONE
    } else {
      val hasDeleteOnKey = changelogMode.keyOnlyDeletes()
      if (hasDeleteOnKey) {
        DeleteKindTrait.DELETE_BY_KEY
      } else {
        DeleteKindTrait.FULL_DELETE
      }
    }
  }
}
