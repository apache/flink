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
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.types.RowKind

import scala.collection.JavaConversions._

/**
 * ModifyKindSetTrait is used to describe what modify operation will be produced by this node.
 */
class ModifyKindSetTrait(val modifyKindSet: ModifyKindSet) extends RelTrait {

  override def satisfies(relTrait: RelTrait): Boolean = relTrait match {
    case other: ModifyKindSetTrait =>
      // it’s satisfied when modify kinds are included in the required set,
      // e.g. [I,U] satisfy [I,U,D]
      //      [I,U,D] not satisfy [I,D]
      this.modifyKindSet.getContainedKinds.forall(other.modifyKindSet.contains)
    case _ => false
  }

  override def getTraitDef: RelTraitDef[_ <: RelTrait] = ModifyKindSetTraitDef.INSTANCE

  override def register(planner: RelOptPlanner): Unit = {}

  override def hashCode(): Int = modifyKindSet.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case t: ModifyKindSetTrait => this.modifyKindSet.equals(t.modifyKindSet)
    case _ => false
  }

  override def toString: String = s"[${modifyKindSet.toString}]"
}

object ModifyKindSetTrait {
  /**
   * An empty [[ModifyKindSetTrait]] which doesn't contain any [[ModifyKind]].
   */
  val EMPTY = new ModifyKindSetTrait(ModifyKindSet.newBuilder().build())

  /**
   * Insert-only [[ModifyKindSetTrait]].
   */
  val INSERT_ONLY = new ModifyKindSetTrait(ModifyKindSet.INSERT_ONLY)

  /**
   * A modify [[ModifyKindSetTrait]] that contains all change operations.
   */
  val ALL_CHANGES = new ModifyKindSetTrait(ModifyKindSet.ALL_CHANGES)

  /**
   * Creates an instance of [[ModifyKindSetTrait]] from th given [[ChangelogMode]].
   */
  def fromChangelogMode(changelogMode: ChangelogMode): ModifyKindSetTrait = {
    val builder = ModifyKindSet.newBuilder
    changelogMode.getContainedKinds.foreach {
      case RowKind.INSERT => builder.addContainedKind(ModifyKind.INSERT)
      case RowKind.DELETE => builder.addContainedKind(ModifyKind.DELETE)
      case _ => builder.addContainedKind(ModifyKind.UPDATE) // otherwise updates
    }
    new ModifyKindSetTrait(builder.build)
  }

}
