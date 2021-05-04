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

package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.table.catalog.{CatalogTable, ObjectIdentifier, ResolvedCatalogTable}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec
import org.apache.flink.table.planner.plan.nodes.calcite.Sink
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSink
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.hint.RelHint

import java.util

/**
 * Batch physical RelNode to to write data into an external sink defined by a
 * [[DynamicTableSink]].
 */
class BatchPhysicalSink(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    hints: util.List[RelHint],
    tableIdentifier: ObjectIdentifier,
    catalogTable: ResolvedCatalogTable,
    tableSink: DynamicTableSink,
    abilitySpecs: Array[SinkAbilitySpec])
  extends Sink(cluster, traitSet, inputRel, hints, tableIdentifier, catalogTable, tableSink)
  with BatchPhysicalRel {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchPhysicalSink(
      cluster,
      traitSet,
      inputs.get(0),
      hints,
      tableIdentifier,
      catalogTable,
      tableSink,
      abilitySpecs)
  }

  override def translateToExecNode(): ExecNode[_] = {
    val tableSinkSpec = new DynamicTableSinkSpec(
      tableIdentifier,
      catalogTable,
      util.Arrays.asList(abilitySpecs: _*))
    tableSinkSpec.setTableSink(tableSink)
    val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(this)
    tableSinkSpec.setReadableConfig(tableConfig.getConfiguration)

    new BatchExecSink(
      tableSinkSpec,
      // the input records will not trigger any output of a sink because it has no output,
      // so it's dam behavior is BLOCKING
      InputProperty.builder().damBehavior(InputProperty.DamBehavior.BLOCKING).build(),
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }
}
