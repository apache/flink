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
package org.apache.flink.table.plan.rules.physical.batch

import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.connector.DefinedDistribution
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical._
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTemporalTableJoin
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.plan.util.TemporalJoinUtil.containsTemporalJoinCondition
import org.apache.flink.table.sources.LookupableTableSource
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rex.{RexNode, RexProgram}

object BatchExecTemporalTableJoinRule {
  val SNAPSHOT_ON_TABLESCAN: RelOptRule = new SnapshotOnTableScanRule
  val SNAPSHOT_ON_CALC_TABLESCAN: RelOptRule = new SnapshotOnCalcTableScanRule

  class SnapshotOnTableScanRule
    extends RelOptRule(
      operand(
        classOf[FlinkLogicalJoin],
        operand(classOf[FlinkLogicalRel], any()),
        operand(classOf[FlinkLogicalSnapshot],
                operand(classOf[TableScan], any()))),
      "BatchExecSnapshotOnTableScanRule") {

    override def matches(call: RelOptRuleCall): Boolean = {
      val join = call.rel[FlinkLogicalJoin](0)
      val tableScan = call.rel[TableScan](3)
      BatchExecTemporalTableJoinRule.matches(join, tableScan)
    }

    override def onMatch(call: RelOptRuleCall): Unit = {
      val join = call.rel[FlinkLogicalJoin](0)
      val input = call.rel[FlinkLogicalRel](1)
      val snapshot = call.rel[FlinkLogicalSnapshot](2)
      val tableScan = call.rel[FlinkLogicalTableSourceScan](3)

      val temporalJoin = transform(join, input, tableScan, snapshot.getPeriod, None)
      call.transformTo(temporalJoin)
    }

  }

  class SnapshotOnCalcTableScanRule
    extends RelOptRule(
      operand(
        classOf[FlinkLogicalJoin],
        operand(classOf[FlinkLogicalRel], any()),
        operand(classOf[FlinkLogicalSnapshot],
                operand(classOf[FlinkLogicalCalc],
                        operand(classOf[TableScan], any())))),
      "BatchExecSnapshotOnCalcTableScanRule") {

    override def matches(call: RelOptRuleCall): Boolean = {
      val join = call.rel[FlinkLogicalJoin](0)
      val tableScan = call.rel[TableScan](4)
      BatchExecTemporalTableJoinRule.matches(join, tableScan)
    }

    override def onMatch(call: RelOptRuleCall): Unit = {
      val join = call.rel[FlinkLogicalJoin](0)
      val input = call.rel[FlinkLogicalRel](1)
      val snapshot = call.rel[FlinkLogicalSnapshot](2)
      val calc = call.rel[FlinkLogicalCalc](3)
      val tableScan = call.rel[FlinkLogicalTableSourceScan](4)
      val temporalJoin = transform(
        join, input, tableScan, snapshot.getPeriod, Some(calc.getProgram))
      call.transformTo(temporalJoin)
    }

  }

  private def matches(join: FlinkLogicalJoin, tableScan: TableScan): Boolean = {
    // shouldn't match temporal table function join
    if (containsTemporalJoinCondition(join.getCondition)) {
      return false
    }
    if (!tableScan.isInstanceOf[FlinkLogicalTableSourceScan]) {
      throw new TableException(
        "Temporal table join only support join on a TableSource " +
          "not on a DataStream or an intermediate query")
    }
    // currently temporal table join only support LookupableTableSource
    val tableSourceTable: TableSourceTable = tableScan.getTable.unwrap(classOf[TableSourceTable])
    tableSourceTable.tableSource.isInstanceOf[LookupableTableSource[_]]
  }

  private def transform(
    join: FlinkLogicalJoin,
    input: FlinkLogicalRel,
    tableScan: FlinkLogicalTableSourceScan,
    period: RexNode,
    calcProgram: Option[RexProgram]): BatchExecTemporalTableJoin = {

    val joinInfo = join.analyzeCondition

    val cluster = join.getCluster
    val typeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val tableSource = tableScan.tableSource
    val tableRowType = typeFactory.buildLogicalRowType(tableSource.getTableSchema, true)

    val providedTrait = join.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    var requiredTrait = input.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)

    // if partitioning enabled, use the join key as partition key
    tableSource match {
      case ps: DefinedDistribution if ps.getPartitionField() != null &&
        !joinInfo.pairs().isEmpty =>
        requiredTrait = requiredTrait.plus(FlinkRelDistribution.hash(joinInfo.leftKeys))
      case _ =>
    }

    val convInput = RelOptRule.convert(input, requiredTrait)
    new BatchExecTemporalTableJoin(
      cluster,
      providedTrait,
      convInput,
      tableSource,
      tableRowType,
      calcProgram,
      period,
      joinInfo,
      join.getJoinType)
  }
}
