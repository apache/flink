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

package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.config.ExecutionConfigOptions.UpsertMaterialize
import org.apache.flink.table.catalog.{ObjectIdentifier, ResolvedCatalogTable}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.calcite.Sink
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.utils.{ChangelogPlanUtils, FlinkRelOptUtil, RelDescriptionWriterImpl}
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.types.RowKind

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.util.ImmutableBitSet

import java.io.{PrintWriter, StringWriter}
import java.util

import scala.collection.JavaConversions._

/**
 * Stream physical RelNode to to write data into an external sink defined by a
 * [[DynamicTableSink]].
 */
class StreamPhysicalSink(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    hints: util.List[RelHint],
    tableIdentifier: ObjectIdentifier,
    catalogTable: ResolvedCatalogTable,
    tableSink: DynamicTableSink,
    abilitySpecs: Array[SinkAbilitySpec])
  extends Sink(cluster, traitSet, inputRel, hints, tableIdentifier, catalogTable, tableSink)
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalSink(
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
    val inputChangelogMode = ChangelogPlanUtils.getChangelogMode(
      getInput.asInstanceOf[StreamPhysicalRel]).get
    val tableSinkSpec = new DynamicTableSinkSpec(
      tableIdentifier,
      catalogTable,
      util.Arrays.asList(abilitySpecs: _*))
    tableSinkSpec.setTableSink(tableSink)
    val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(this)
    tableSinkSpec.setReadableConfig(tableConfig.getConfiguration)

    val primaryKeys = toScala(catalogTable.getResolvedSchema
        .getPrimaryKey).map(_.getColumns).map(toScala[String]).getOrElse(Seq())

    val upsertMaterialize = tableConfig.getConfiguration.get(
      ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE) match {
      case UpsertMaterialize.FORCE => primaryKeys.nonEmpty
      case UpsertMaterialize.NONE => false
      case UpsertMaterialize.AUTO =>
        val insertOnly = tableSink
            .getChangelogMode(inputChangelogMode)
            .containsOnly(RowKind.INSERT)

        if (!insertOnly && primaryKeys.nonEmpty) {
          val columnNames = catalogTable.getResolvedSchema.getColumnNames
          val pks = ImmutableBitSet.of(primaryKeys.map(columnNames.indexOf): _*)

          val fmq = FlinkRelMetadataQuery.reuseOrCreate(getCluster.getMetadataQuery)
          val uniqueKeys = fmq.getUniqueKeys(getInput)
          val changeLogUpsertKeys = fmq.getUpsertKeys(getInput)

          if (uniqueKeys != null &&
              uniqueKeys.exists(pks.contains) &&
              !(changeLogUpsertKeys != null &&
                  changeLogUpsertKeys.exists(pks.contains))) {
            true
          } else {
            false
          }
        } else {
          false
        }
    }

    new StreamExecSink(
      tableSinkSpec,
      inputChangelogMode,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      upsertMaterialize,
      getDescriptionWithUpsert(upsertMaterialize)
    )
  }

  /**
   * The inputChangelogMode can only be obtained in translateToExecNode phase.
   */
  def getDescriptionWithUpsert(upsertMaterialize: Boolean): String = {
    val sw = new StringWriter
    val pw = new PrintWriter(sw)
    val relWriter = new RelDescriptionWriterImpl(pw)
    this.explainTerms(relWriter)
    relWriter.itemIf("upsertMaterialize", "true", upsertMaterialize)
    relWriter.done(this)
    sw.toString
  }
}
