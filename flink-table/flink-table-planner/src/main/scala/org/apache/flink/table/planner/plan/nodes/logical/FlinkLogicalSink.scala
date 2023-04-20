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
package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.catalog.ContextResolvedTable
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.calcite.{LogicalSink, Sink}

import org.apache.calcite.plan.{Convention, RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rel.hint.RelHint

import java.util

import scala.collection.JavaConversions._

/**
 * Sub-class of [[Sink]] that is a relational expression which writes out data of input node into a
 * [[DynamicTableSink]].
 */
class FlinkLogicalSink(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    hints: util.List[RelHint],
    contextResolvedTable: ContextResolvedTable,
    tableSink: DynamicTableSink,
    targetColumns: Array[Array[Int]],
    val staticPartitions: Map[String, String],
    val abilitySpecs: Array[SinkAbilitySpec])
  extends Sink(cluster, traitSet, input, hints, targetColumns, contextResolvedTable, tableSink)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalSink(
      cluster,
      traitSet,
      inputs.head,
      hints,
      contextResolvedTable,
      tableSink,
      targetColumns,
      staticPartitions,
      abilitySpecs)
  }

}

private class FlinkLogicalSinkConverter(config: Config) extends ConverterRule(config) {

  override def convert(rel: RelNode): RelNode = {
    val sink = rel.asInstanceOf[LogicalSink]
    val newInput = RelOptRule.convert(sink.getInput, FlinkConventions.LOGICAL)
    FlinkLogicalSink.create(
      newInput,
      sink.hints,
      sink.contextResolvedTable,
      sink.tableSink,
      sink.staticPartitions,
      sink.targetColumns,
      sink.abilitySpecs)
  }
}

object FlinkLogicalSink {
  val CONVERTER: ConverterRule = new FlinkLogicalSinkConverter(
    Config.INSTANCE.withConversion(
      classOf[LogicalSink],
      Convention.NONE,
      FlinkConventions.LOGICAL,
      "FlinkLogicalSinkConverter"))

  def create(
      input: RelNode,
      hints: util.List[RelHint],
      contextResolvedTable: ContextResolvedTable,
      tableSink: DynamicTableSink,
      staticPartitions: Map[String, String] = Map(),
      targetColumns: Array[Array[Int]],
      abilitySpecs: Array[SinkAbilitySpec] = Array.empty): FlinkLogicalSink = {
    val cluster = input.getCluster
    val traitSet = cluster.traitSetOf(FlinkConventions.LOGICAL).simplify()
    new FlinkLogicalSink(
      cluster,
      traitSet,
      input,
      hints,
      contextResolvedTable,
      tableSink,
      targetColumns,
      staticPartitions,
      abilitySpecs)
  }
}
