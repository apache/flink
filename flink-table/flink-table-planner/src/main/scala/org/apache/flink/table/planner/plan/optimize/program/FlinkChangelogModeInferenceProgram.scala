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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.`trait`._
import org.apache.flink.table.planner.plan.nodes.physical.stream._
import org.apache.flink.table.planner.plan.utils._

import org.apache.calcite.rel.RelNode

/** An optimize program to infer ChangelogMode for every physical node. */
class FlinkChangelogModeInferenceProgram extends FlinkOptimizeProgram[StreamOptimizeContext] {

  override def optimize(root: RelNode, context: StreamOptimizeContext): RelNode = {
    // step1: satisfy ModifyKindSet trait
    val physicalRoot = root.asInstanceOf[StreamPhysicalRel]
    val rootWithModifyKindSet = new SatisfyModifyKindSetTraitVisitor().visit(
      physicalRoot,
      // we do not propagate the ModifyKindSet requirement and requester among blocks
      // set default ModifyKindSet requirement and requester for root
      ModifyKindSetTrait.ALL_CHANGES,
      "ROOT"
    )

    // step2: satisfy UpdateKind trait
    val rootModifyKindSet = getModifyKindSet(rootWithModifyKindSet)
    // use the required UpdateKindTrait from parent blocks
    val requiredUpdateKindTraits = if (rootModifyKindSet.contains(ModifyKind.UPDATE)) {
      if (context.isUpdateBeforeRequired) {
        Seq(UpdateKindTrait.BEFORE_AND_AFTER)
      } else {
        // update_before is not required, and input contains updates
        // try ONLY_UPDATE_AFTER first, and then BEFORE_AND_AFTER
        Seq(UpdateKindTrait.ONLY_UPDATE_AFTER, UpdateKindTrait.BEFORE_AND_AFTER)
      }
    } else {
      // there is no updates
      Seq(UpdateKindTrait.NONE)
    }

    val updateKindTraitVisitor = new SatisfyUpdateKindTraitVisitor(context)
    val updateRoot = requiredUpdateKindTraits.flatMap {
      requiredUpdateKindTrait =>
        val updated = updateKindTraitVisitor.visit(rootWithModifyKindSet, requiredUpdateKindTrait)
        if (updated.isPresent) Some(updated.get) else None
    }

    // step3: satisfy DeleteKind trait
    val requiredDeleteKindTraits = if (rootModifyKindSet.contains(ModifyKind.DELETE)) {
      root match {
        case _: StreamPhysicalSink =>
          // try DELETE_BY_KEY first, and then FULL_DELETE
          Seq(DeleteKindTrait.DELETE_BY_KEY, DeleteKindTrait.FULL_DELETE)
        case _ =>
          // for non-sink nodes prefer full deletes
          Seq(DeleteKindTrait.FULL_DELETE)
      }
    } else {
      // there is no deletes
      Seq(DeleteKindTrait.NONE)
    }

    val deleteKindTraitVisitor = new SatisfyDeleteKindTraitVisitor(context)
    val finalRoot = if (updateRoot.isEmpty) {
      updateRoot
    } else {
      requiredDeleteKindTraits.flatMap {
        requiredDeleteKindTrait =>
          val deleteRoot = deleteKindTraitVisitor.visit(updateRoot.head, requiredDeleteKindTrait)
          if (deleteRoot.isPresent) Some(deleteRoot.get) else None
      }
    }

    // step4: sanity check and return non-empty root
    if (finalRoot.isEmpty) {
      val plan = FlinkRelOptUtil.toString(root, withChangelogTraits = true)
      throw new TableException(
        "Can't generate a valid execution plan for the given query:\n" + plan)
    } else {
      finalRoot.head
    }
  }

  private def getModifyKindSet(node: RelNode): ModifyKindSet =
    ChangelogModeInferenceUtils.getModifyKindSet(node)
}
