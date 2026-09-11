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
package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.`trait`.{DeleteKind, DeleteKindTraitDef}
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rex.RexProgram

import scala.collection.JavaConversions._

/** Stream physical RelNode for [[Calc]]. */
class StreamPhysicalCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    calcProgram: RexProgram,
    outputRowType: RelDataType)
  extends StreamPhysicalCalcBase(cluster, traitSet, inputRel, calcProgram, outputRowType) {

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new StreamPhysicalCalc(cluster, traitSet, child, program, outputRowType)
  }

  override def translateToExecNode(): ExecNode[_] = {
    val projection = calcProgram.getProjectList.map(calcProgram.expandLocalRef)
    val condition = if (calcProgram.getCondition != null) {
      calcProgram.expandLocalRef(calcProgram.getCondition)
    } else {
      null
    }

    new StreamExecCalc(
      unwrapTableConfig(this),
      projection,
      condition,
      partialDeleteKeys,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }

  /**
   * If this Calc forwards DELETE_BY_KEY changes, the rest of a delete-by-key tombstone's row may
   * not be present (see DeleteKind.DELETE_BY_KEY). Returns the output column indices to keep
   * (evaluated from the regular projection); every other column is handled separately in code
   * generation as a typed NULL. Returns `null` when this Calc does not need such handling, or no
   * output key column could be identified (in which case the full projection is always evaluated).
   */
  private def partialDeleteKeys: Array[Int] = {
    val deleteKind = Option(getTraitSet.getTrait(DeleteKindTraitDef.INSTANCE))
      .map(_.deleteKind)
      .getOrElse(DeleteKind.NONE)
    if (deleteKind != DeleteKind.DELETE_BY_KEY) {
      return null
    }

    val outputUpsertKeys = FlinkRelMetadataQuery
      .reuseOrCreate(cluster.getMetadataQuery)
      .getUpsertKeys(this)
    if (outputUpsertKeys == null || outputUpsertKeys.isEmpty) {
      // no identifiable output key column: fall back to always evaluating the full projection
      return null
    }

    // Every column in every candidate is, by construction of
    // FlinkRelMdUniqueKeys.getProjectUniqueKeys, guaranteed to be a trivial
    // pass-through of an input field - never a risky expression to evaluate.
    val keyIndices = outputUpsertKeys.flatMap(bitSet => bitSet.map(_.intValue())).toSet.toArray
    if (keyIndices.nonEmpty) {
      keyIndices
    } else {
      null
    }
  }
}
