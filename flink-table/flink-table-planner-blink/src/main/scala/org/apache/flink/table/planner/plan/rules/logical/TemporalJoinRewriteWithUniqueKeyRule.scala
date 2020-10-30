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
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex._

import scala.collection.JavaConversions._

/**
  * Planner rule that rewrites temporal join with extracted primary key, Event-time temporal
  * table join requires primary key and row time attribute of versioned table. The versioned table
  * could be a table source or a view only if it contains the unique key and time attribute.
  *
  * <p> Flink supports extract the primary key and row time attribute from the view if the view
  * comes from [[LogicalRank]] node which can convert to a [[Deduplicate]] node.
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
        if (call.getOperator == TemporalJoinUtil.INITIAL_TEMPORAL_JOIN_CONDITION) {
          val (snapshotTimeInputRef, leftJoinKey, rightJoinKey) =
            if (TemporalJoinUtil.isInitialRowTimeTemporalTableJoin(call)) {
              val snapshotTimeInputRef = call.operands(0)
              val leftJoinKey = call.operands(2).asInstanceOf[RexCall].operands
              val rightJoinKey = call.operands(3).asInstanceOf[RexCall].operands
              (snapshotTimeInputRef, leftJoinKey, rightJoinKey)
            } else {
              val snapshotTimeInputRef = call.operands(0)
              val leftJoinKey = call.operands(1).asInstanceOf[RexCall].operands
              val rightJoinKey = call.operands(2).asInstanceOf[RexCall].operands
              (snapshotTimeInputRef, leftJoinKey, rightJoinKey)
            }

          val rexBuilder = join.getCluster.getRexBuilder
          val primaryKeyInputRefs = extractPrimaryKeyInputRefs(leftInput, snapshot, rexBuilder)
          validateRightPrimaryKey(join, rightJoinKey, primaryKeyInputRefs)

          if (TemporalJoinUtil.isInitialRowTimeTemporalTableJoin(call)) {
            val rightTimeInputRef = call.operands(1)
            TemporalJoinUtil.makeRowTimeTemporalTableJoinConCall(
              rexBuilder,
              snapshotTimeInputRef,
              rightTimeInputRef,
              primaryKeyInputRefs.get,
              leftJoinKey,
              rightJoinKey)
          } else {
            TemporalJoinUtil.makeProcTimeTemporalTableJoinConCall(
              rexBuilder,
              snapshotTimeInputRef,
              primaryKeyInputRefs.get,
              leftJoinKey,
              rightJoinKey)
          }
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
      join: FlinkLogicalJoin,
      rightJoinKeyExpression: Seq[RexNode],
      rightPrimaryKeyInputRefs: Option[Seq[RexNode]]): Unit = {

    if (rightPrimaryKeyInputRefs.isEmpty) {
      throw new ValidationException(
          "Temporal Table Join requires primary key in versioned table, " +
            s"but no primary key can be found. " +
            s"The physical plan is:\n${RelOptUtil.toString(join)}\n")
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
      val joinFieldNames = join.getRowType.getFieldNames
      val joinLeftFieldNames = join.getLeft.getRowType.getFieldNames
      val joinRightFieldNamess = join.getRight.getRowType.getFieldNames
      val primaryKeyNames = rightPrimaryKeyRefIndices
        .map(i => joinFieldNames.get(i)).toList
        .mkString(",")
      val joinEquiInfo = join.analyzeCondition.pairs().map { pair =>
        joinLeftFieldNames.get(pair.source) + "=" + joinRightFieldNamess.get(pair.target)
      }.toList.mkString(",")
      throw new ValidationException(
        s"Temporal table's primary key [$primaryKeyNames] must be included in the equivalence " +
          s"condition of temporal join, but current temporal join condition is [$joinEquiInfo].")
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
}

object TemporalJoinRewriteWithUniqueKeyRule {
  val INSTANCE: TemporalJoinRewriteWithUniqueKeyRule = new TemporalJoinRewriteWithUniqueKeyRule
}
