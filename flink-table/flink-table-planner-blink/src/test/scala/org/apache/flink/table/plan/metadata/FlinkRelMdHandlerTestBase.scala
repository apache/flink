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

package org.apache.flink.table.plan.metadata

import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.{FlinkCalciteCatalogReader, FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalDataStreamTableScan
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecDataStreamScan
import org.apache.flink.table.plan.schema.FlinkRelOptTable

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.{Convention, ConventionTraitDef, RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rel.metadata.{JaninoRelMetadataProvider, RelMetadataQuery}
import org.apache.calcite.rel.{RelCollationTraitDef, RelDistributionTraitDef, RelNode, SingleRel}
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.tools.FrameworkConfig
import org.junit.{Before, BeforeClass}

import java.util.{List => JList}

class FlinkRelMdHandlerTestBase {

  val rootSchema: SchemaPlus = MetadataTestUtil.initRootSchema()
  val frameworkConfig: FrameworkConfig = MetadataTestUtil.createFrameworkConfig(rootSchema)
  val typeFactory: FlinkTypeFactory = new FlinkTypeFactory(frameworkConfig.getTypeSystem)
  val catalogReader: FlinkCalciteCatalogReader =
    MetadataTestUtil.createCatalogReader(rootSchema, typeFactory)
  val mq: FlinkRelMetadataQuery = FlinkRelMetadataQuery.instance()

  var relBuilder: FlinkRelBuilder = _
  var rexBuilder: RexBuilder = _
  var cluster: RelOptCluster = _

  var logicalTraits: RelTraitSet = _
  var flinkLogicalTraits: RelTraitSet = _
  var batchPhysicalTraits: RelTraitSet = _
  var streamPhysicalTraits: RelTraitSet = _

  @Before
  def setUp(): Unit = {
    relBuilder = FlinkRelBuilder.create(
      frameworkConfig,
      typeFactory,
      Array(
        ConventionTraitDef.INSTANCE,
        RelDistributionTraitDef.INSTANCE,
        RelCollationTraitDef.INSTANCE
      )
    )

    rexBuilder = relBuilder.getRexBuilder
    cluster = relBuilder.getCluster

    logicalTraits = cluster.traitSetOf(Convention.NONE)

    flinkLogicalTraits = cluster
      .traitSetOf(Convention.NONE)
      .replace(FlinkConventions.LOGICAL)

    batchPhysicalTraits = cluster
      .traitSetOf(Convention.NONE)
      .replace(FlinkConventions.BATCH_PHYSICAL)

    streamPhysicalTraits = cluster
      .traitSetOf(Convention.NONE)
      .replace(FlinkConventions.STREAM_PHYSICAL)
  }

  protected lazy val testRel = new TestRel(
    cluster, logicalTraits, createDataStreamScan(ImmutableList.of("student"), logicalTraits))

  protected lazy val studentLogicalScan: FlinkLogicalDataStreamTableScan =
    createDataStreamScan(ImmutableList.of("student"), flinkLogicalTraits)
  protected lazy val studentBatchScan: BatchExecBoundedStreamScan =
    createDataStreamScan(ImmutableList.of("student"), batchPhysicalTraits)
  protected lazy val studentStreamScan: StreamExecDataStreamScan =
    createDataStreamScan(ImmutableList.of("student"), streamPhysicalTraits)

  protected lazy val empLogicalScan: FlinkLogicalDataStreamTableScan =
    createDataStreamScan(ImmutableList.of("emp"), flinkLogicalTraits)
  protected lazy val empBatchScan: BatchExecBoundedStreamScan =
    createDataStreamScan(ImmutableList.of("emp"), batchPhysicalTraits)
  protected lazy val empStreamScan: StreamExecDataStreamScan =
    createDataStreamScan(ImmutableList.of("emp"), streamPhysicalTraits)

  protected def createDataStreamScan[T](
      tableNames: JList[String], traitSet: RelTraitSet): T = {
    val table = catalogReader.getTable(tableNames).asInstanceOf[FlinkRelOptTable]
    val conventionTrait = traitSet.getTrait(ConventionTraitDef.INSTANCE)
    val scan = conventionTrait match {
      case Convention.NONE =>
        relBuilder.clear()
        relBuilder.scan(tableNames).build()
      case FlinkConventions.LOGICAL =>
        new FlinkLogicalDataStreamTableScan(cluster, traitSet, table)
      case FlinkConventions.BATCH_PHYSICAL =>
        new BatchExecBoundedStreamScan(cluster, traitSet, table, table.getRowType)
      case FlinkConventions.STREAM_PHYSICAL =>
        new StreamExecDataStreamScan(cluster, traitSet, table, table.getRowType)
      case _ => throw new TableException(s"Unsupported convention trait: $conventionTrait")
    }
    scan.asInstanceOf[T]
  }
}

class TestRel(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    input: RelNode) extends SingleRel(cluster, traits, input) {

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    planner.getCostFactory.makeCost(1.0, 1.0, 1.0)
  }
}

object FlinkRelMdHandlerTestBase {
  @BeforeClass
  def beforeAll(): Unit = {
    RelMetadataQuery
      .THREAD_PROVIDERS
      .set(JaninoRelMetadataProvider.of(FlinkDefaultRelMetadataProvider.INSTANCE))
  }
}
