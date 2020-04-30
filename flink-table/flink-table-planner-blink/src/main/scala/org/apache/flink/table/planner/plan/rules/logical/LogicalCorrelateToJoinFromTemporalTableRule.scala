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

import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamExecLookupJoin, StreamExecTemporalJoin}

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.{LogicalCorrelate, LogicalFilter, LogicalSnapshot}
import org.apache.calcite.rex.{RexCorrelVariable, RexFieldAccess, RexInputRef, RexNode, RexShuttle}

/**
  * The initial temporal table join (FOR SYSTEM_TIME AS OF) is a Correlate, rewrite it into a Join
  * to make join condition can be pushed-down. The join will be translated into
  * [[StreamExecLookupJoin]] in physical and might be translated into [[StreamExecTemporalJoin]]
  * in the future.
  */
abstract class LogicalCorrelateToJoinFromTemporalTableRule(
    operand: RelOptRuleOperand,
    description: String)
  extends RelOptRule(operand, description) {

  def getFilterCondition(call: RelOptRuleCall): RexNode

  def getLogicalSnapshot(call: RelOptRuleCall): LogicalSnapshot

  override def onMatch(call: RelOptRuleCall): Unit = {
    val correlate: LogicalCorrelate = call.rel(0)
    val leftInput: RelNode = call.rel(1)
    val filterCondition = getFilterCondition(call)
    val snapshot = getLogicalSnapshot(call)

    val leftRowType = leftInput.getRowType
    val joinCondition = filterCondition.accept(new RexShuttle() {
      // change correlate variable expression to normal RexInputRef (which is from left side)
      override def visitFieldAccess(fieldAccess: RexFieldAccess): RexNode = {
        fieldAccess.getReferenceExpr match {
          case corVar: RexCorrelVariable =>
            require(correlate.getCorrelationId.equals(corVar.id))
            val index = leftRowType.getFieldList.indexOf(fieldAccess.getField)
            RexInputRef.of(index, leftRowType)
          case _ => super.visitFieldAccess(fieldAccess)
        }
      }

      // update the field index from right side
      override def visitInputRef(inputRef: RexInputRef): RexNode = {
        val rightIndex = leftRowType.getFieldCount + inputRef.getIndex
        new RexInputRef(rightIndex, inputRef.getType)
      }
    })

    val builder = call.builder()
    builder.push(leftInput)
    builder.push(snapshot)
    builder.join(correlate.getJoinType, joinCondition)

    val rel = builder.build()
    call.transformTo(rel)
  }

}

/**
  * Planner rule that matches temporal table join which join condition is not true,
  * that means the right input of the Correlate is a Filter.
  * e.g. SELECT * FROM MyTable AS T JOIN temporalTest FOR SYSTEM_TIME AS OF T.proctime AS D
  * ON T.a = D.id
  */
class LogicalCorrelateToJoinFromTemporalTableRuleWithFilter
  extends LogicalCorrelateToJoinFromTemporalTableRule(
    operand(classOf[LogicalCorrelate],
      operand(classOf[RelNode], any()),
      operand(classOf[LogicalFilter],
        operand(classOf[LogicalSnapshot], any()))),
    "LogicalCorrelateToJoinFromTemporalTableRuleWithFilter"
  ) {

  override def getFilterCondition(call: RelOptRuleCall): RexNode = {
    val filter: LogicalFilter = call.rel(2)
    filter.getCondition
  }

  override def getLogicalSnapshot(call: RelOptRuleCall): LogicalSnapshot = {
    call.rels(3).asInstanceOf[LogicalSnapshot]
  }
}

/**
  * Planner rule that matches temporal table join which join condition is true,
  * that means the right input of the Correlate is a Snapshot.
  * e.g. SELECT * FROM MyTable AS T JOIN temporalTest FOR SYSTEM_TIME AS OF T.proctime AS D ON true
  */
class LogicalCorrelateToJoinFromTemporalTableRuleWithoutFilter
  extends LogicalCorrelateToJoinFromTemporalTableRule(
    operand(classOf[LogicalCorrelate],
      operand(classOf[RelNode], any()),
      operand(classOf[LogicalSnapshot], any())),
    "LogicalCorrelateToJoinFromTemporalTableRuleWithoutFilter"
  ) {

  override def getFilterCondition(call: RelOptRuleCall): RexNode = {
    call.builder().literal(true)
  }

  override def getLogicalSnapshot(call: RelOptRuleCall): LogicalSnapshot = {
    call.rels(2).asInstanceOf[LogicalSnapshot]
  }
}

object LogicalCorrelateToJoinFromTemporalTableRule {
  val WITH_FILTER = new LogicalCorrelateToJoinFromTemporalTableRuleWithFilter
  val WITHOUT_FILTER = new LogicalCorrelateToJoinFromTemporalTableRuleWithoutFilter
}
