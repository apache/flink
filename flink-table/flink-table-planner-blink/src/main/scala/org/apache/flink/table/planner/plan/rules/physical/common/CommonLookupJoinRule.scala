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
package org.apache.flink.table.planner.plan.rules.physical.common

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.nodes.common.CommonLookupJoin
import org.apache.flink.table.planner.plan.nodes.logical._
import org.apache.flink.table.planner.plan.nodes.physical.PhysicalTableSourceScan
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType
import org.apache.flink.table.planner.plan.utils.JoinUtil
import org.apache.flink.table.sources.{LookupableTableSource, TableSource}

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rex.{RexCorrelVariable, RexFieldAccess, RexProgram}

import java.util

import scala.collection.JavaConversions._

/**
  * Base implementation for both
  * [[org.apache.flink.table.planner.plan.rules.physical.batch.BatchExecLookupJoinRule]] and
  * [[org.apache.flink.table.planner.plan.rules.physical.stream.StreamExecLookupJoinRule]].
  */
trait CommonLookupJoinRule {

  protected def matches(
      join: FlinkLogicalJoin,
      snapshot: FlinkLogicalSnapshot,
      tableScan: TableScan): Boolean = {
    // TODO: shouldn't match temporal UDTF join
    if (findTableSource(tableScan).isEmpty) {
      throw new TableException(
        "Temporal table join only support join on a LookupableTableSource " +
          "not on a DataStream or an intermediate query")
    }
    // period specification check
    snapshot.getPeriod match {
      // it's left table's field, pass
      case r: RexFieldAccess if r.getReferenceExpr.isInstanceOf[RexCorrelVariable] =>
      case _ =>
        throw new TableException("Temporal table join currently only supports " +
          "'FOR SYSTEM_TIME AS OF' left table's proctime field, doesn't support 'PROCTIME()'")
    }
    snapshot.getPeriod.getType match {
      // TODO: support to translate rowtime temporal join to TemporalTableJoin in the future
      case t: TimeIndicatorRelDataType if !t.isEventTime => // pass
      case _ =>
        throw new TableException("Temporal table join currently only supports " +
          "'FOR SYSTEM_TIME AS OF' left table's proctime field, doesn't support 'PROCTIME()'")
    }
    // currently temporal table join only support LookupableTableSource
    isLookupableTableSource(tableScan)
  }

  protected def findTableSource(relNode: RelNode): Option[TableSource[_]] = {
    relNode match {
      case logicalScan: FlinkLogicalTableSourceScan => Some(logicalScan.tableSource)
      case physicalScan: PhysicalTableSourceScan => Some(physicalScan.tableSource)
      // TODO: find TableSource in FlinkLogicalIntermediateTableScan
      case _ => None
    }
  }

  protected def isLookupableTableSource(relNode: RelNode): Boolean = {
    relNode match {
      case logicalScan: FlinkLogicalTableSourceScan =>
        logicalScan.tableSource.isInstanceOf[LookupableTableSource[_]]
      case physicalScan: PhysicalTableSourceScan =>
        physicalScan.tableSource.isInstanceOf[LookupableTableSource[_]]
      // TODO: find TableSource in FlinkLogicalIntermediateTableScan
      case _ => false
    }
  }

  // TODO Support `IS NOT DISTINCT FROM` in the future: FLINK-13509
  protected def validateJoin(join: FlinkLogicalJoin): Unit = {

    val filterNulls: Array[Boolean] = {
      val filterNulls = new util.ArrayList[java.lang.Boolean]
      JoinUtil.createJoinInfo(join.getLeft, join.getRight, join.getCondition, filterNulls)
      filterNulls.map(_.booleanValue()).toArray
    }

    if (filterNulls.contains(false)) {
      throw new TableException(
        s"LookupJoin doesn't support join condition contains 'a IS NOT DISTINCT FROM b' (or " +
          s"alternative '(a = b) or (a IS NULL AND b IS NULL)'), the join condition is " +
          s"'${join.getCondition}'")
    }
  }

  protected def transform(
    join: FlinkLogicalJoin,
    input: FlinkLogicalRel,
    tableSource: TableSource[_],
    calcProgram: Option[RexProgram]): CommonLookupJoin
}

abstract class BaseSnapshotOnTableScanRule(description: String)
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin],
      operand(classOf[FlinkLogicalRel], any()),
      operand(classOf[FlinkLogicalSnapshot],
        operand(classOf[TableScan], any()))),
    description)
  with CommonLookupJoinRule {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel[FlinkLogicalJoin](0)
    val snapshot = call.rel[FlinkLogicalSnapshot](2)
    val tableScan = call.rel[TableScan](3)
    matches(join, snapshot, tableScan)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join = call.rel[FlinkLogicalJoin](0)
    val input = call.rel[FlinkLogicalRel](1)
    val tableScan = call.rel[RelNode](3)
    val tableSource = findTableSource(tableScan).orNull

    validateJoin(join)
    val temporalJoin = transform(join, input, tableSource, None)
    call.transformTo(temporalJoin)
  }

}

abstract class BaseSnapshotOnCalcTableScanRule(description: String)
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin],
      operand(classOf[FlinkLogicalRel], any()),
      operand(classOf[FlinkLogicalSnapshot],
        operand(classOf[FlinkLogicalCalc],
          operand(classOf[TableScan], any())))),
    description)
  with CommonLookupJoinRule {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel[FlinkLogicalJoin](0)
    val snapshot = call.rel[FlinkLogicalSnapshot](2)
    val tableScan = call.rel[TableScan](4)
    matches(join, snapshot, tableScan)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join = call.rel[FlinkLogicalJoin](0)
    val input = call.rel[FlinkLogicalRel](1)
    val calc = call.rel[FlinkLogicalCalc](3)
    val tableScan = call.rel[RelNode](4)
    val tableSource = findTableSource(tableScan).orNull

    validateJoin(join)
    val temporalJoin = transform(
      join, input, tableSource, Some(calc.getProgram))
    call.transformTo(temporalJoin)
  }

}
