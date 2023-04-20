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

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalJoin
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalJoin
import org.apache.flink.table.planner.plan.utils.{TemporalJoinUtil, TemporalTableJoinUtil}
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil.{TEMPORAL_JOIN_CONDITION, TEMPORAL_JOIN_CONDITION_PRIMARY_KEY}
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig
import org.apache.flink.util.Preconditions.checkState

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.rex._

import java.util.Optional

import scala.collection.JavaConverters._

/**
 * Stream physical node for temporal table join (FOR SYSTEM_TIME AS OF) and temporal TableFunction
 * join (LATERAL TemporalTableFunction(proctime)).
 *
 * <p>The legacy temporal table function join is the subset of temporal table join, the only
 * difference is the validation, we reuse most same logic here.
 */
class StreamPhysicalTemporalJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType)
  extends CommonPhysicalJoin(cluster, traitSet, leftRel, rightRel, condition, joinType)
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = TemporalJoinUtil.isRowTimeJoin(joinSpec)

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new StreamPhysicalTemporalJoin(cluster, traitSet, left, right, conditionExpr, joinType)
  }

  override def translateToExecNode(): ExecNode[_] = {
    val textualRepresentation = this.toString
    val rexBuilder = cluster.getRexBuilder
    val isTemporalFunctionJoin =
      TemporalJoinUtil.isTemporalFunctionJoin(rexBuilder, joinInfo)

    val leftFieldCount = getLeft.getRowType.getFieldCount
    val temporalJoinConditionExtractor = new TemporalJoinConditionExtractor(
      textualRepresentation,
      leftFieldCount,
      joinSpec,
      cluster.getRexBuilder,
      isTemporalFunctionJoin)
    val remainingNonEquiJoinCondition =
      temporalJoinConditionExtractor.apply(joinSpec.getNonEquiCondition.orElse(null))
    val temporalJoinSpec = new JoinSpec(
      joinSpec.getJoinType,
      joinSpec.getLeftKeys,
      joinSpec.getRightKeys,
      joinSpec.getFilterNulls,
      remainingNonEquiJoinCondition)

    val (leftTimeAttributeInputRef, rightRowTimeAttributeInputRef: Optional[Integer]) =
      if (TemporalJoinUtil.isRowTimeJoin(joinSpec)) {
        checkState(
          temporalJoinConditionExtractor.leftTimeAttribute.isDefined &&
            temporalJoinConditionExtractor.rightPrimaryKey.isDefined,
          "Missing %s in Event-Time temporal join condition",
          TEMPORAL_JOIN_CONDITION
        )

        val leftTimeAttributeInputRef = TemporalJoinUtil.extractInputRef(
          temporalJoinConditionExtractor.leftTimeAttribute.get,
          textualRepresentation)
        val rightTimeAttributeInputRef = TemporalJoinUtil.extractInputRef(
          temporalJoinConditionExtractor.rightTimeAttribute.get,
          textualRepresentation)
        val rightInputRef = rightTimeAttributeInputRef - leftFieldCount

        (leftTimeAttributeInputRef, Optional.of(new Integer(rightInputRef)))
      } else {
        val leftTimeAttributeInputRef = TemporalJoinUtil.extractInputRef(
          temporalJoinConditionExtractor.leftTimeAttribute.get,
          textualRepresentation)
        // right time attribute defined in temporal join condition iff in Event time join
        (leftTimeAttributeInputRef, Optional.empty().asInstanceOf[Optional[Integer]])
      }

    new StreamExecTemporalJoin(
      unwrapTableConfig(this),
      temporalJoinSpec,
      isTemporalFunctionJoin,
      leftTimeAttributeInputRef,
      rightRowTimeAttributeInputRef.orElse(
        StreamExecTemporalJoin.FIELD_INDEX_FOR_PROC_TIME_ATTRIBUTE),
      InputProperty.DEFAULT,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }

  /**
   * TemporalJoinConditionExtractor extracts [[TEMPORAL_JOIN_CONDITION]] from non-equi join
   * conditions.
   *
   * <p>TimeAttributes of both sides and primary keys of right side will be extracted and the
   * TEMPORAL_JOIN_CONDITION RexCall will be pruned after extraction. </p>
   */
  private class TemporalJoinConditionExtractor(
      textualRepresentation: String,
      rightKeysStartingOffset: Int,
      joinSpec: JoinSpec,
      rexBuilder: RexBuilder,
      isTemporalFunctionJoin: Boolean)
    extends RexShuttle {

    var leftTimeAttribute: Option[RexNode] = None

    var rightTimeAttribute: Option[RexNode] = None

    var rightPrimaryKey: Option[Array[RexNode]] = None

    override def visitCall(call: RexCall): RexNode = {
      if (call.getOperator != TEMPORAL_JOIN_CONDITION) {
        return super.visitCall(call)
      }

      // at most one temporal function in a temporal join node
      if (isTemporalFunctionJoin) {
        checkState(
          leftTimeAttribute.isEmpty
            && rightPrimaryKey.isEmpty
            && rightTimeAttribute.isEmpty,
          "Multiple %s temporal functions in [%s]",
          TEMPORAL_JOIN_CONDITION,
          textualRepresentation
        )
      }

      if (
        TemporalTableJoinUtil.isRowTimeTemporalTableJoinCondition(call) ||
        TemporalJoinUtil.isRowTimeTemporalFunctionJoinCon(call)
      ) {
        leftTimeAttribute = Some(call.getOperands.get(0))
        rightTimeAttribute = Some(call.getOperands.get(1))
        rightPrimaryKey = Some(extractPrimaryKeyArray(call.getOperands.get(2)))
      } else {
        leftTimeAttribute = Some(call.getOperands.get(0))
        rightPrimaryKey = Some(extractPrimaryKeyArray(call.getOperands.get(1)))
      }

      // the condition of temporal function comes from WHERE clause,
      // so it's not been validated in logical plan
      if (isTemporalFunctionJoin) {
        TemporalJoinUtil.validateTemporalFunctionCondition(
          call,
          leftTimeAttribute.get,
          rightTimeAttribute,
          rightPrimaryKey,
          rightKeysStartingOffset,
          joinSpec,
          "Temporal Table Function")
      }

      rexBuilder.makeLiteral(true)
    }

    private def extractPrimaryKeyArray(rightPrimaryKey: RexNode): Array[RexNode] = {
      if (
        !rightPrimaryKey.isInstanceOf[RexCall] ||
        rightPrimaryKey.asInstanceOf[RexCall].getOperator != TEMPORAL_JOIN_CONDITION_PRIMARY_KEY
      ) {
        throw new ValidationException(
          s"No primary key [${rightPrimaryKey.asInstanceOf[RexCall]}] " +
            s"defined in versioned table of Event-time temporal table join")
      }
      rightPrimaryKey.asInstanceOf[RexCall].getOperands.asScala.toArray
    }
  }
}
