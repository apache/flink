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

import org.apache.flink.table.catalog.CatalogTable
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.calcite.{LogicalLegacySink, LegacySink}
import org.apache.flink.table.sinks.TableSink

import org.apache.calcite.plan.{Convention, RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule

import java.util

import scala.collection.JavaConversions._

/**
  * Sub-class of [[LegacySink]] that is a relational expression
  * which writes out data of input node into a [[TableSink]].
  */
class FlinkLogicalLegacySink(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    sink: TableSink[_],
    sinkName: String,
    val catalogTable: CatalogTable,
    val staticPartitions: Map[String, String])
  extends LegacySink(cluster, traitSet, input, sink, sinkName)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalLegacySink(
      cluster, traitSet, inputs.head, sink, sinkName, catalogTable, staticPartitions)
  }

}

private class FlinkLogicalLegacySinkConverter
  extends ConverterRule(
    classOf[LogicalLegacySink],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalLegacySinkConverter") {

  override def convert(rel: RelNode): RelNode = {
    val sink = rel.asInstanceOf[LogicalLegacySink]
    val newInput = RelOptRule.convert(sink.getInput, FlinkConventions.LOGICAL)
    FlinkLogicalLegacySink.create(
      newInput,
      sink.sink,
      sink.sinkName,
      sink.catalogTable,
      sink.staticPartitions)
  }
}

object FlinkLogicalLegacySink {
  val CONVERTER: ConverterRule = new FlinkLogicalLegacySinkConverter()

  def create(
      input: RelNode,
      sink: TableSink[_],
      sinkName: String,
      catalogTable: CatalogTable = null,
      staticPartitions: Map[String, String] = Map()): FlinkLogicalLegacySink = {
    val cluster = input.getCluster
    val traitSet = cluster.traitSetOf(FlinkConventions.LOGICAL).simplify()
    new FlinkLogicalLegacySink(
      cluster, traitSet, input, sink, sinkName, catalogTable, staticPartitions)
  }
}
