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

package org.apache.flink.table.planner.plan.nodes.calcite

import org.apache.flink.table.catalog.{CatalogTable, ObjectIdentifier, ResolvedCatalogTable}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec

import org.apache.calcite.plan.{Convention, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.hint.RelHint

import java.util

import scala.collection.JavaConversions._

/**
  * Sub-class of [[Sink]] that is a relational expression
  * which writes out data of input node into a [[DynamicTableSink]].
  * This class corresponds to Calcite logical rel.
  */
final class LogicalSink(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    hints: util.List[RelHint],
    tableIdentifier: ObjectIdentifier,
    catalogTable: ResolvedCatalogTable,
    tableSink: DynamicTableSink,
    val staticPartitions: Map[String, String],
    val abilitySpecs: Array[SinkAbilitySpec])
  extends Sink(cluster, traitSet, input, hints, tableIdentifier, catalogTable, tableSink) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new LogicalSink(
      cluster,
      traitSet,
      inputs.head,
      hints,
      tableIdentifier,
      catalogTable,
      tableSink,
      staticPartitions,
      abilitySpecs)
  }
}

object LogicalSink {

  def create(
      input: RelNode,
      hints: util.List[RelHint],
      tableIdentifier: ObjectIdentifier,
      catalogTable: ResolvedCatalogTable,
      tableSink: DynamicTableSink,
      staticPartitions: util.Map[String, String],
      abilitySpecs: Array[SinkAbilitySpec]): LogicalSink = {
    val traits = input.getCluster.traitSetOf(Convention.NONE)
    new LogicalSink(
      input.getCluster,
      traits,
      input,
      hints,
      tableIdentifier,
      catalogTable,
      tableSink,
      staticPartitions.toMap,
      abilitySpecs)
  }
}




