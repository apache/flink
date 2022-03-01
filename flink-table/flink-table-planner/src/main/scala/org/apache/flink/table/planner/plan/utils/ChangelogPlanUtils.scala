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

package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.planner.plan.`trait`.{ModifyKind, ModifyKindSetTraitDef, UpdateKind, UpdateKindTraitDef}
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel
import org.apache.flink.table.planner.plan.optimize.program.FlinkChangelogModeInferenceProgram
import org.apache.flink.types.RowKind
import org.apache.calcite.rel.RelNode

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Utilities for changelog plan.
 */
object ChangelogPlanUtils {

  /**
   * A [[ChangelogMode]] contains all kinds of [[RowKind]].
   */
  val FULL_CHANGELOG_MODE: ChangelogMode = ChangelogMode.newBuilder()
    .addContainedKind(RowKind.INSERT)
    .addContainedKind(RowKind.UPDATE_BEFORE)
    .addContainedKind(RowKind.UPDATE_AFTER)
    .addContainedKind(RowKind.DELETE)
    .build()

  /**
   * Returns true if the inputs of current node produce insert-only changes.
   *
   *  <p>Note: this method must be called after [[FlinkChangelogModeInferenceProgram]] is applied.
   */
  def inputInsertOnly(node: StreamPhysicalRel): Boolean = {
    node.getInputs.forall {
      case input: StreamPhysicalRel => isInsertOnly(input)
    }
  }

  /**
   * Returns true if current node produces insert-only changes.
   *
   *  <p>Note: this method must be called after [[FlinkChangelogModeInferenceProgram]] is applied.
   */
  def isInsertOnly(node: StreamPhysicalRel): Boolean = {
    val modifyKindSetTrait = node.getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
    modifyKindSetTrait.modifyKindSet.isInsertOnly
  }

  /**
   * Returns true if the [[RelNode]] will generate UPDATE_BEFORE messages.
   * This method is used to determine whether the runtime operator should
   * produce UPDATE_BEFORE messages with UPDATE_AFTER message together.
   *
   * <p>Note: this method must be called after [[FlinkChangelogModeInferenceProgram]] is applied.
   */
  def generateUpdateBefore(node: StreamPhysicalRel): Boolean = {
    val updateKindTrait = node.getTraitSet.getTrait(UpdateKindTraitDef.INSTANCE)
    updateKindTrait.updateKind == UpdateKind.BEFORE_AND_AFTER
  }

  /**
   * Gets an optional [[ChangelogMode]] of the given physical node.
   * The [[ChangelogMode]] is inferred from ModifyKindSetTrait and UpdateKindTrait.
   * The returned value is None if the given node is Sink node.
   *
   * <p>Note: this method must be called after [[FlinkChangelogModeInferenceProgram]] is applied.
   */
  def getChangelogMode(node: StreamPhysicalRel): Option[ChangelogMode] = {
    val modifyKindSet = node.getTraitSet
      .getTrait(ModifyKindSetTraitDef.INSTANCE)
      .modifyKindSet
    val updateKind = node.getTraitSet
      .getTrait(UpdateKindTraitDef.INSTANCE)
      .updateKind

    if (modifyKindSet.isEmpty) {
      None
    } else {
      val modeBuilder = ChangelogMode.newBuilder()
      if (modifyKindSet.contains(ModifyKind.INSERT)) {
        modeBuilder.addContainedKind(RowKind.INSERT)
      }
      if (modifyKindSet.contains(ModifyKind.DELETE)) {
        modeBuilder.addContainedKind(RowKind.DELETE)
      }
      if (modifyKindSet.contains(ModifyKind.UPDATE)) {
        modeBuilder.addContainedKind(RowKind.UPDATE_AFTER)
        if (updateKind == UpdateKind.BEFORE_AND_AFTER) {
          modeBuilder.addContainedKind(RowKind.UPDATE_BEFORE)
        }
      }
      Some(modeBuilder.build())
    }
  }

  /**
   * Returns the string representation of an optional ChangelogMode.
   */
  def stringifyChangelogMode(optionMode: Option[ChangelogMode]): String = optionMode match {
    case None => "NONE"
    case Some(mode) =>
      val kinds = new ArrayBuffer[String]
      if (mode.contains(RowKind.INSERT)) {
        kinds += "I"
      }
      if (mode.contains(RowKind.UPDATE_BEFORE)) {
        kinds += "UB"
      }
      if (mode.contains(RowKind.UPDATE_AFTER)) {
        kinds += "UA"
      }
      if (mode.contains(RowKind.DELETE)) {
        kinds += "D"
      }
      kinds.mkString(",")
  }
}
