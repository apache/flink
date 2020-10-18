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

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalJoin, FlinkLogicalRel, FlinkLogicalSnapshot}
import org.apache.flink.table.planner.plan.rules.common.CommonTemporalTableJoinRule
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.{RexBuilder, RexCall, RexInputRef, RexNode, RexShuttle}

import scala.collection.JavaConversions._

/**
  * Planner rule that rewrites temporal join with extracted primary key, Event-time temporal
  * table join requires primary key and row time attribute of versioned table. The versioned table
  * could be a table source or a view only if it contains the unique key and time attribute.
  *
  * <p> Flink support extract the primary key and row time attribute from the view if the view comes
  * from [[LogicalRank]] node which can convert to a [[Deduplicate]] node.
  */
class TemporalJoinRewriteWithUniqueKeyRule extends RelOptRule(
  operand(classOf[FlinkLogicalJoin],
    operand(classOf[FlinkLogicalRel], any()),
    operand(classOf[FlinkLogicalSnapshot],
      operand(classOf[FlinkLogicalRel], any()))),
  "TemporalJoinRewriteWithUniqueKeyRule")
  with CommonTemporalTableJoinRule {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel[FlinkLogicalJoin](0)
    val snapshot = call.rel[FlinkLogicalSnapshot](2)
    val snapshotInput = call.rel[FlinkLogicalRel](3)

    val isTemporalJoin = matches(snapshot)
    val canConvertToLookup = canConvertToLookupJoin(snapshot, snapshotInput)
    val supportedJoinTypes = Seq(JoinRelType.INNER, JoinRelType.LEFT)

    isTemporalJoin && !canConvertToLookup && supportedJoinTypes.contains(join.getJoinType)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join = call.rel[FlinkLogicalJoin](0)
    val leftInput = call.rel[FlinkLogicalRel](1)
    val snapshot = call.rel[FlinkLogicalSnapshot](2)

    val joinCondition = join.getCondition

    val newJoinCondition = joinCondition.accept(new RexShuttle {
      override def visitCall(call: RexCall): RexNode = {
        if (call.getOperator == TemporalJoinUtil.TEMPORAL_JOIN_CONDITION &&
        isRowTimeTemporalTableJoin(snapshot)) {
          val snapshotTimeInputRef = call.operands(0)
          val rightTimeInputRef = call.operands(1)
          val leftJoinKey = call.operands(2).asInstanceOf[RexCall].operands
          val rightJoinKey = call.operands(3).asInstanceOf[RexCall].operands

          val rexBuilder = join.getCluster.getRexBuilder
          val primaryKeyInputRefs = extractPrimaryKeyInputRefs(leftInput, snapshot, rexBuilder)

          validateRightPrimaryKey(rightJoinKey, primaryKeyInputRefs)
          TemporalJoinUtil.makeRowTimeTemporalJoinConditionCall(rexBuilder, snapshotTimeInputRef,
            rightTimeInputRef, leftJoinKey, rightJoinKey, primaryKeyInputRefs.get)
        }
        else {
          super.visitCall(call)
        }
      }
    })
    val rewriteJoin = FlinkLogicalJoin.create(
      leftInput, snapshot, newJoinCondition, join.getJoinType)
    call.transformTo(rewriteJoin)
  }

  private def validateRightPrimaryKey(
      rightJoinKeyExpression: Seq[RexNode],
      rightPrimaryKeyInputRefs: Option[Seq[RexNode]]): Unit = {

    if (rightPrimaryKeyInputRefs.isEmpty) {
      throw new ValidationException("Event-Time Temporal Table Join requires both" +
        s" primary key and row time attribute in versioned table," +
        s" but no primary key can be found.")
    }

    val rightJoinKeyRefIndices = rightJoinKeyExpression
      .map(rex => rex.asInstanceOf[RexInputRef].getIndex )
      .toArray

    val rightPrimaryKeyRefIndices= rightPrimaryKeyInputRefs.get
      .map(rex => rex.asInstanceOf[RexInputRef].getIndex)
      .toArray

    val primaryKeyContainedInJoinKey = rightPrimaryKeyRefIndices
      .forall(pk => rightJoinKeyRefIndices.contains(pk))

    if (!primaryKeyContainedInJoinKey) {
      throw new ValidationException(
        s"Join key must be the same as temporal table's primary key " +
          s"in Event-time temporal table join.")
    }
  }

  private def extractPrimaryKeyInputRefs(
      leftInput: RelNode,
      snapshot: FlinkLogicalSnapshot,
      rexBuilder: RexBuilder): Option[Seq[RexNode]] = {
    val rightFields = snapshot.getRowType.getFieldList
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(snapshot.getCluster.getMetadataQuery)

    val uniqueKeySet = fmq.getUniqueKeys(snapshot.getInput())
    val fields = snapshot.getRowType.getFieldList

    if (uniqueKeySet != null && uniqueKeySet.size() > 0) {
      val leftFieldCnt = leftInput.getRowType.getFieldCount
      val uniqueKeySetInputRefs = uniqueKeySet.filter(_.nonEmpty)
        .map(_.toArray
          .map(fields)
          // build InputRef of unique key in snapshot
          .map(f => rexBuilder.makeInputRef(
            f.getType,
            leftFieldCnt + rightFields.indexOf(f)))
          .toSeq)
      // select shortest unique key as primary key
      uniqueKeySetInputRefs
        .toArray
        .sortBy(_.length)
        .headOption
    } else {
      None
    }
  }

  private def isRowTimeTemporalTableJoin(snapshot: FlinkLogicalSnapshot): Boolean =
    snapshot.getPeriod.getType match {
      case t: TimeIndicatorRelDataType if t.isEventTime => true
      case _ => false
    }
}

object TemporalJoinRewriteWithUniqueKeyRule {
  val INSTANCE: TemporalJoinRewriteWithUniqueKeyRule = new TemporalJoinRewriteWithUniqueKeyRule
}
