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

package org.apache.flink.table.plan.rules.physical.batch.runtimefilter

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.functions.sql.internal.SqlRuntimeFilterBuilderFunction
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecCalc, BatchExecHashJoinBase}
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter.BaseRuntimeFilterPushDownRule.findRfBuilders
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter.RfBuilderJoinTransposeRule.{getIndexFromCall, updateRuntimeFilterBuilderFunction}
import org.apache.flink.table.plan.util.FlinkRelOptUtil

import org.apache.calcite.plan.{RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex._
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.JavaConversions._

/**
  * Planner rule that pushes a [[SqlRuntimeFilterBuilderFunction]] past a [[BatchExecHashJoinBase]].
  */
class RfBuilderJoinTransposeRule extends BaseRuntimeFilterPushDownRule(
  classOf[BatchExecHashJoinBase],
  "RfBuilderJoinTransposeRule"){

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: BatchExecCalc = call.rel(0)
    val join: BatchExecHashJoinBase = call.rel(1)

    (join.flinkJoinType == FlinkJoinRelType.INNER ||
        join.flinkJoinType == FlinkJoinRelType.SEMI) &&
        findRfBuilders(calc.getProgram).nonEmpty
  }

  override def canPush(
      rel: BatchExecHashJoinBase,
      rCols: ImmutableBitSet,
      cond: RexNode): Boolean = {

    val conf = FlinkRelOptUtil.getTableConfig(rel)
    val maxRatio = conf.getConf.getDouble(
      TableConfigOptions.SQL_EXEC_RUNTIME_FILTER_BUILDER_PUSH_DOWN_RATIO_MAX)

    val joinKeys = rel.joinInfo.leftKeys ++
        rel.joinInfo.rightKeys.map(_ + rel.getLeft.getRowType.getFieldCount)
    val inJoinKeys = rCols.forall(joinKeys.contains)

    if (inJoinKeys) {
      cond match {
        case call: RexCall => call.getOperator match {
          case func: SqlRuntimeFilterBuilderFunction =>
            val index = getIndexFromCall(call)
            val query = rel.getCluster.getMetadataQuery
            val pushDownNdv = query.getDistinctRowCount(
              rel.buildRel,
              ImmutableBitSet.of(index + getFieldAdjustments(rel)(index)),
              null)
            val pushDownRowCount = query.getRowCount(rel.buildRel)
            if (pushDownNdv == null || pushDownRowCount == null) {
              false
            } else {
              pushDownNdv / func.ndv <= maxRatio && pushDownRowCount / func.rowCount <= maxRatio
            }
          case _ => false
        }
        case _ => false
      }
    } else {
      false
    }
  }

  /**
    * Convert Probe key to build key.
    */
  override def getFieldAdjustments(rel: BatchExecHashJoinBase): Array[Int] = {
    val adjustments = new Array[Int](rel.getRowType.getFieldCount)
    val offset = rel.getLeft.getRowType.getFieldCount
    rel.buildKeys.zip(rel.probeKeys).foreach { case (buildKey, probeKey) =>
      if (rel.flinkJoinType == FlinkJoinRelType.SEMI) {
        // semi join just output probe fields.
        adjustments(probeKey) = buildKey - probeKey
      } else {
        if (rel.leftIsBuild) {
          adjustments(probeKey + offset) = buildKey - probeKey - offset
        } else {
          adjustments(buildKey + offset) = -offset
          adjustments(probeKey) = buildKey - probeKey
        }
      }
    }
    adjustments
  }

  override def updateRfFunction(filterInput: RelNode, program: RexProgram): Unit =
    updateRuntimeFilterBuilderFunction(filterInput, program)

  override def getInputOfInput(input: BatchExecHashJoinBase): RelNode = input.buildRel

  override def replaceInput(input: BatchExecHashJoinBase, filter: BatchExecCalc): RelNode = {
    val inputs = if (input.leftIsBuild) {
      Seq(filter, input.probeRel)
    } else {
      Seq(input.probeRel, filter)
    }
    input.copy(input.getTraitSet, inputs)
  }
}

object RfBuilderJoinTransposeRule {

  val INSTANCE = new RfBuilderJoinTransposeRule

  def updateRuntimeFilterBuilderFunction(filterInput: RelNode, program: RexProgram): Unit = {
    // update ndv
    val rfCalls = findRfBuilders(program)
    rfCalls.foreach { call =>
      val fieldIndex = getIndexFromCall(call)
      val rf = call.getOperator.asInstanceOf[SqlRuntimeFilterBuilderFunction]
      val query = filterInput.getCluster.getMetadataQuery
      val ndv = query.getDistinctRowCount(filterInput, ImmutableBitSet.of(fieldIndex), null)
      if (ndv != null) {
        rf.ndv = ndv
      }
      val rowCount = query.getRowCount(filterInput)
      if (rowCount != null) {
        rf.rowCount = rowCount
      }
    }
  }

  def getIndexFromCall(call: RexCall): Int = {
    val indexArray = RelOptUtil.InputFinder.bits(call).toArray
    require(indexArray.length == 1)
    indexArray.head
  }
}
