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
package org.apache.flink.table.plan.rules.physical.stream

import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.common.CommonLookupJoin
import org.apache.flink.table.plan.nodes.logical._
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecLookupJoin
import org.apache.flink.table.plan.rules.physical.common.{BaseSnapshotOnCalcTableScanRule, BaseSnapshotOnTableScanRule}
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rex.RexProgram

/**
  * Rules that convert [[FlinkLogicalJoin]] on a [[FlinkLogicalSnapshot]]
  * into [[StreamExecLookupJoin]]
  *
  * There are 2 conditions for this rule:
  * 1. the root parent of [[FlinkLogicalSnapshot]] should be a TableSource which implements
  *   [[org.apache.flink.table.sources.LookupableTableSource]].
  * 2. the period of [[FlinkLogicalSnapshot]] must be left table's proctime attribute.
  */
object StreamExecLookupJoinRule {
  val SNAPSHOT_ON_TABLESCAN: RelOptRule = new SnapshotOnTableScanRule
  val SNAPSHOT_ON_CALC_TABLESCAN: RelOptRule = new SnapshotOnCalcTableScanRule

  class SnapshotOnTableScanRule
    extends BaseSnapshotOnTableScanRule("StreamExecSnapshotOnTableScanRule") {

    override protected def transform(
        join: FlinkLogicalJoin,
        input: FlinkLogicalRel,
        tableSource: TableSource[_],
        calcProgram: Option[RexProgram]): CommonLookupJoin = {
      doTransform(join, input, tableSource, calcProgram)
    }
  }

  class SnapshotOnCalcTableScanRule
    extends BaseSnapshotOnCalcTableScanRule("StreamExecSnapshotOnCalcTableScanRule") {

    override protected def transform(
        join: FlinkLogicalJoin,
        input: FlinkLogicalRel,
        tableSource: TableSource[_],
        calcProgram: Option[RexProgram]): CommonLookupJoin = {
      doTransform(join, input, tableSource, calcProgram)
    }
  }

  private def doTransform(
    join: FlinkLogicalJoin,
    input: FlinkLogicalRel,
    tableSource: TableSource[_],
    calcProgram: Option[RexProgram]): StreamExecLookupJoin = {

    val joinInfo = join.analyzeCondition

    val cluster = join.getCluster
    val typeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val tableRowType = typeFactory.buildRelNodeRowType(
      tableSource.getTableSchema.getFieldNames,
      tableSource.getTableSchema.getFieldDataTypes.map(fromDataTypeToLogicalType)
    )

    val providedTrait = join.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    var requiredTrait = input.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)

    val convInput = RelOptRule.convert(input, requiredTrait)
    new StreamExecLookupJoin(
      cluster,
      providedTrait,
      convInput,
      tableSource,
      tableRowType,
      calcProgram,
      joinInfo,
      join.getJoinType)
  }
}
