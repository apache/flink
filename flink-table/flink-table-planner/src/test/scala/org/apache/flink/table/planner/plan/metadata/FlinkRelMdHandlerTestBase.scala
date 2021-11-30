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

package org.apache.flink.table.planner.plan.metadata

import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.expressions.ApiExpressionUtils.intervalOfMillis
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.{FunctionIdentifier, UserDefinedFunctionHelper}
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.operations.TableSourceQueryOperation
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.planner.delegation.PlannerContext
import org.apache.flink.table.planner.expressions.{PlannerNamedWindowProperty, PlannerProctimeAttribute, PlannerRowtimeAttribute, PlannerWindowReference, PlannerWindowStart}
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.planner.functions.utils.AggSqlFunction
import org.apache.flink.table.planner.plan.PartialFinalType
import org.apache.flink.table.planner.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.planner.plan.logical.{LogicalWindow, TumblingGroupWindow}
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.calcite._
import org.apache.flink.table.planner.plan.nodes.logical._
import org.apache.flink.table.planner.plan.nodes.physical.batch._
import org.apache.flink.table.planner.plan.nodes.physical.stream._
import org.apache.flink.table.planner.plan.schema.{FlinkPreparingTableBase, IntermediateRelTable, TableSourceTable}
import org.apache.flink.table.planner.plan.stream.sql.join.TestTemporalTable
import org.apache.flink.table.planner.plan.utils._
import org.apache.flink.table.planner.utils.Top3
import org.apache.flink.table.runtime.operators.rank.{ConstantRankRange, RankType, VariableRankRange}
import org.apache.flink.table.types.AtomicDataType
import org.apache.flink.table.types.logical._
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.utils.CatalogManagerMocks

import com.google.common.collect.{ImmutableList, Lists}
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan._
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFieldImpl}
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.logical._
import org.apache.calcite.rel.metadata.{JaninoRelMetadataProvider, RelMetadataQuery, RelMetadataQueryBase}
import org.apache.calcite.rex._
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.{SqlAggFunction, SqlWindow}
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.`type`.{BasicSqlType, SqlTypeName}
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.sql.fun.{SqlCountAggFunction, SqlStdOperatorTable}
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.util._
import org.junit.{Before, BeforeClass}

import java.math.BigDecimal
import java.util
import java.util.Collections

import scala.collection.JavaConversions._

class FlinkRelMdHandlerTestBase {

  val tableConfig = new TableConfig()
  val rootSchema: SchemaPlus = MetadataTestUtil.initRootSchema()

  val catalogManager: CatalogManager = CatalogManagerMocks.createEmptyCatalogManager()
  val moduleManager = new ModuleManager

  // TODO batch RelNode and stream RelNode should have different PlannerContext
  //  and RelOptCluster due to they have different trait definitions.
  val plannerContext: PlannerContext =
  new PlannerContext(
    false,
    tableConfig,
    new FunctionCatalog(tableConfig, catalogManager, moduleManager),
    catalogManager,
    CalciteSchema.from(rootSchema),
    util.Arrays.asList(
      ConventionTraitDef.INSTANCE,
      FlinkRelDistributionTraitDef.INSTANCE,
      RelCollationTraitDef.INSTANCE
    )
  )
  val typeFactory: FlinkTypeFactory = plannerContext.getTypeFactory
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
    relBuilder = plannerContext.createRelBuilder("default_catalog", "default_database")

    rexBuilder = relBuilder.getRexBuilder
    cluster = relBuilder.getCluster

    logicalTraits = cluster.traitSetOf(Convention.NONE)

    flinkLogicalTraits = cluster.traitSetOf(FlinkConventions.LOGICAL)

    batchPhysicalTraits = cluster.traitSetOf(FlinkConventions.BATCH_PHYSICAL)

    streamPhysicalTraits = cluster.traitSetOf(FlinkConventions.STREAM_PHYSICAL)
  }

  protected def bd(value: Long): BigDecimal = {
    BigDecimal.valueOf(value)
  }

  protected def bd(value: Double): BigDecimal = {
    BigDecimal.valueOf(value)
  }

  protected val intType: RelDataType = typeFactory.createFieldTypeFromLogicalType(
    new IntType(false))

  protected val doubleType: RelDataType = typeFactory.createFieldTypeFromLogicalType(
    new DoubleType(false))

  protected val longType: RelDataType = typeFactory.createFieldTypeFromLogicalType(
    new BigIntType(false))

  protected val stringType: RelDataType = typeFactory.createFieldTypeFromLogicalType(
    new VarCharType(false, VarCharType.MAX_LENGTH))

  protected lazy val testRel = new TestRel(
    cluster, logicalTraits, createDataStreamScan(ImmutableList.of("student"), logicalTraits))

  protected lazy val studentLogicalScan: LogicalTableScan =
    createDataStreamScan(ImmutableList.of("student"), logicalTraits)
  protected lazy val studentFlinkLogicalScan: FlinkLogicalDataStreamTableScan =
    createDataStreamScan(ImmutableList.of("student"), flinkLogicalTraits)
  protected lazy val studentBatchScan: BatchPhysicalBoundedStreamScan =
    createDataStreamScan(ImmutableList.of("student"), batchPhysicalTraits)
  protected lazy val studentStreamScan: StreamPhysicalDataStreamScan =
    createDataStreamScan(ImmutableList.of("student"), streamPhysicalTraits)

  protected lazy val empLogicalScan: LogicalTableScan =
    createDataStreamScan(ImmutableList.of("emp"), logicalTraits)
  protected lazy val empFlinkLogicalScan: FlinkLogicalDataStreamTableScan =
    createDataStreamScan(ImmutableList.of("emp"), flinkLogicalTraits)
  protected lazy val empBatchScan: BatchPhysicalBoundedStreamScan =
    createDataStreamScan(ImmutableList.of("emp"), batchPhysicalTraits)
  protected lazy val empStreamScan: StreamPhysicalDataStreamScan =
    createDataStreamScan(ImmutableList.of("emp"), streamPhysicalTraits)

  protected lazy val tableSourceTableLogicalScan: LogicalTableScan =
    createTableSourceTable(ImmutableList.of("TableSourceTable1"), logicalTraits)
  protected lazy val tableSourceTableFlinkLogicalScan: FlinkLogicalDataStreamTableScan =
    createTableSourceTable(ImmutableList.of("TableSourceTable1"), flinkLogicalTraits)
  protected lazy val tableSourceTableBatchScan: BatchPhysicalBoundedStreamScan =
    createTableSourceTable(ImmutableList.of("TableSourceTable1"), batchPhysicalTraits)
  protected lazy val tableSourceTableStreamScan: StreamPhysicalDataStreamScan =
    createTableSourceTable(ImmutableList.of("TableSourceTable1"), streamPhysicalTraits)

  protected lazy val tablePartiallyProjectedKeyLogicalScan: LogicalTableScan =
    createTableSourceTable(ImmutableList.of("projected_table_source_table_with_partial_pk"),
      logicalTraits)
  protected lazy val tablePartiallyProjectedKeyFlinkLogicalScan: FlinkLogicalDataStreamTableScan =
    createTableSourceTable(ImmutableList.of("projected_table_source_table_with_partial_pk"),
      flinkLogicalTraits)
  protected lazy val tablePartiallyProjectedKeyBatchScan: BatchPhysicalBoundedStreamScan =
    createTableSourceTable(ImmutableList.of("projected_table_source_table_with_partial_pk"),
      batchPhysicalTraits)
  protected lazy val tablePartiallyProjectedKeyStreamScan: StreamPhysicalDataStreamScan =
    createTableSourceTable(ImmutableList.of("projected_table_source_table_with_partial_pk"),
      streamPhysicalTraits)

  protected lazy val tableSourceTableNonKeyLogicalScan: LogicalTableScan =
    createTableSourceTable(ImmutableList.of("TableSourceTable3"), logicalTraits)
  protected lazy val tableSourceTableNonKeyFlinkLogicalScan: FlinkLogicalDataStreamTableScan =
    createTableSourceTable(ImmutableList.of("TableSourceTable3"), flinkLogicalTraits)
  protected lazy val tableSourceTableNonKeyBatchScan: BatchPhysicalBoundedStreamScan =
    createTableSourceTable(ImmutableList.of("TableSourceTable3"), batchPhysicalTraits)
  protected lazy val tableSourceTableNonKeyStreamScan: StreamPhysicalDataStreamScan =
    createTableSourceTable(ImmutableList.of("TableSourceTable3"), streamPhysicalTraits)

  private lazy val valuesType = relBuilder.getTypeFactory
    .builder()
    .add("a", SqlTypeName.BIGINT)
    .add("b", SqlTypeName.BOOLEAN)
    .add("c", SqlTypeName.DATE)
    .add("d", SqlTypeName.TIME)
    .add("e", SqlTypeName.TIMESTAMP)
    .add("f", SqlTypeName.DOUBLE)
    .add("g", SqlTypeName.FLOAT)
    .add("h", SqlTypeName.VARCHAR)
    .build()

  protected lazy val emptyValues: LogicalValues = {
    relBuilder.values(valuesType)
    relBuilder.build().asInstanceOf[LogicalValues]
  }

  protected lazy val logicalValues: LogicalValues = {
    val tupleList = List(
      List("1", "true", "2017-10-01", "10:00:00", "2017-10-01 00:00:00", "2.12", null, "abc"),
      List(null, "false", "2017-09-01", "10:00:01", null, "3.12", null, null),
      List("3", "true", null, "10:00:02", "2017-10-01 01:00:00", "3.0", null, "xyz"),
      List("2", "true", "2017-10-02", "09:59:59", "2017-07-01 01:00:00", "-1", null, "F")
    ).map(createLiteralList(valuesType, _))
    relBuilder.values(tupleList, valuesType)
    relBuilder.build().asInstanceOf[LogicalValues]
  }

  // select id, name, score + 0.2, age - 1, height * 1.1 as h1, height / 0.9 as h2, height,
  // case sex = 'M' then 1 else 2, true, 2.1, 2, cast(score as double not null) as s from student
  protected lazy val logicalProject: LogicalProject = {
    relBuilder.push(studentLogicalScan)
    val projects = List(
      // id
      relBuilder.field(0),
      // name
      relBuilder.field(1),
      // score + 0.1
      relBuilder.call(PLUS, relBuilder.field(2), relBuilder.literal(0.2)),
      // age - 1
      relBuilder.call(MINUS, relBuilder.field(3), relBuilder.literal(1)),
      // height * 1.1 as h1
      relBuilder.alias(relBuilder.call(MULTIPLY, relBuilder.field(4), relBuilder.literal(1.1)),
        "h1"),
      // height / 0.9 as h2
      relBuilder.alias(relBuilder.call(DIVIDE, relBuilder.field(4), relBuilder.literal(0.9)), "h2"),
      // height
      relBuilder.field(4),
      // case sex = 'M' then 1 else 2
      relBuilder.call(CASE, relBuilder.call(EQUALS, relBuilder.field(5), relBuilder.literal("M")),
        relBuilder.literal(1), relBuilder.literal(2)),
      // true
      relBuilder.literal(true),
      // 2.1
      rexBuilder.makeLiteral(2.1D, doubleType, true),
      // 2
      rexBuilder.makeLiteral(2L, longType, true),
      // cast(score as double not null) as s
      rexBuilder.makeCast(doubleType, relBuilder.field(2))
    )
    relBuilder.project(projects).build().asInstanceOf[LogicalProject]
  }

  // filter: id < 10
  // calc = filter (id < 10) + logicalProject
  protected lazy val (logicalFilter, logicalCalc) = {
    relBuilder.push(studentLogicalScan)
    // id < 10
    val expr = relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(10))
    val filter = relBuilder.filter(expr).build

    val calc = createLogicalCalc(
      studentLogicalScan, logicalProject.getRowType, logicalProject.getProjects, List(expr))
    (filter, calc)
  }

  protected lazy val logicalWatermarkAssigner: RelNode = {
    val scan = relBuilder.scan("TemporalTable2").build()
    val flinkContext = cluster
      .getPlanner
      .getContext
      .unwrap(classOf[FlinkContext])
    val watermarkRexNode = flinkContext
      .getSqlExprToRexConverterFactory
      .create(scan.getTable.getRowType, null)
      .convertToRexNode("rowtime - INTERVAL '10' SECOND")

    relBuilder.push(scan)
    relBuilder.watermark(4, watermarkRexNode).build()
  }

  // id, name, score, age, height, sex, class, 1
  // id, null, score, age, height, sex, class, 4
  // id, null, score, age, height, null, class, 5
  protected lazy val (logicalExpand, flinkLogicalExpand, batchExpand, streamExpand) = {
    val cluster = studentLogicalScan.getCluster
    val expandProjects = ExpandUtil.createExpandProjects(
      studentLogicalScan.getCluster.getRexBuilder,
      studentLogicalScan.getRowType,
      ImmutableBitSet.of(1, 3, 5),
      ImmutableList.of(
        ImmutableBitSet.of(1, 3, 5),
        ImmutableBitSet.of(3, 5),
        ImmutableBitSet.of(3)),
      Array.empty[Integer])
    val logicalExpand = new LogicalExpand(cluster, studentLogicalScan.getTraitSet,
      studentLogicalScan, expandProjects, 7)

    val flinkLogicalExpand = new FlinkLogicalExpand(cluster, flinkLogicalTraits,
      studentFlinkLogicalScan, expandProjects, 7)

    val batchExpand = new BatchPhysicalExpand(cluster, batchPhysicalTraits,
      studentBatchScan, expandProjects, 7)

    val streamExecExpand = new StreamPhysicalExpand(cluster, streamPhysicalTraits,
      studentStreamScan, expandProjects, 7)

    (logicalExpand, flinkLogicalExpand, batchExpand, streamExecExpand)
  }

  // hash exchange on class
  protected lazy val (batchExchange, streamExchange) = createExchange(6)

  protected lazy val (batchExchangeById, streamExchangeById) = createExchange(0)

  protected def createExchange(hash: Int): (RelNode, RelNode) = {
    val hash6 = FlinkRelDistribution.hash(Array(hash), requireStrict = true)
    val batchExchange = new BatchPhysicalExchange(
      cluster,
      batchPhysicalTraits.replace(hash6),
      studentBatchScan,
      hash6
    )
    val streamExchange = new StreamPhysicalExchange(
      cluster,
      streamPhysicalTraits.replace(hash6),
      studentStreamScan,
      hash6
    )
    (batchExchange, streamExchange)
  }

  protected lazy val intermediateTable = new IntermediateRelTable(
    Seq(""), streamExchangeById, null, false, Set(ImmutableBitSet.of(0)))

  protected lazy val intermediateScan = new FlinkLogicalIntermediateTableScan(
    cluster, streamExchangeById.getTraitSet, intermediateTable)

  // equivalent SQL is
  // select * from student order by class asc, score desc
  protected lazy val (logicalSort, flinkLogicalSort, batchSort, streamSort) =
    createSorts(() =>
      Seq(relBuilder.field("class"),
      relBuilder.desc(relBuilder.field("score"))))

  // equivalent SQL is
  // select * from student order by id asc
  protected lazy val (logicalSortById, flinkLogicalSortById, batchSortById, streamSortById) =
    createSorts(() => Seq(relBuilder.field("id")))

  protected def createSorts(sortKeys: () => Seq[RexNode]): (RelNode, RelNode, RelNode, RelNode) = {
    val logicalSort = relBuilder.scan("student")
        .sort(sortKeys()).build.asInstanceOf[LogicalSort]
    val collation = logicalSort.getCollation
    val flinkLogicalSort = new FlinkLogicalSort(cluster, flinkLogicalTraits.replace(collation),
      studentFlinkLogicalScan, collation, null, null)
    val batchSort = new BatchPhysicalSort(cluster,
      batchPhysicalTraits.replace(collation).replace(FlinkRelDistribution.SINGLETON),
      studentBatchScan, collation)
    val streamSort = new StreamPhysicalSort(cluster,
      streamPhysicalTraits.replace(collation).replace(FlinkRelDistribution.SINGLETON),
      studentStreamScan, collation)
    (logicalSort, flinkLogicalSort, batchSort, streamSort)
  }

  // equivalent SQL is
  // select * from student limit 20 offset 10
  protected lazy val (
    logicalLimit,
    flinkLogicalLimit,
    batchLimit,
    batchLocalLimit,
    batchGlobalLimit,
    streamLimit) = {
    val logicalSort = relBuilder.scan("student").limit(10, 20)
      .build.asInstanceOf[LogicalSort]
    val collation = logicalSort.getCollation

    val flinkLogicalSort = new FlinkLogicalSort(
      cluster, flinkLogicalTraits.replace(collation), studentFlinkLogicalScan, collation,
      logicalSort.offset, logicalSort.fetch)

    val batchSort = new BatchPhysicalLimit(cluster, batchPhysicalTraits.replace(collation),
      new BatchPhysicalExchange(
        cluster, batchPhysicalTraits.replace(FlinkRelDistribution.SINGLETON), studentBatchScan,
        FlinkRelDistribution.SINGLETON),
      logicalSort.offset, logicalSort.fetch, true)

    val batchSortLocal = new BatchPhysicalLimit(cluster, batchPhysicalTraits.replace(collation),
      studentBatchScan,
      relBuilder.literal(0),
      relBuilder.literal(SortUtil.getLimitEnd(logicalSort.offset, logicalSort.fetch)),
      false)
    val batchSortGlobal = new BatchPhysicalLimit(cluster, batchPhysicalTraits.replace(collation),
      new BatchPhysicalExchange(
        cluster, batchPhysicalTraits.replace(FlinkRelDistribution.SINGLETON), batchSortLocal,
        FlinkRelDistribution.SINGLETON),
      logicalSort.offset, logicalSort.fetch, true)

    val streamLimit = new StreamPhysicalLimit(cluster, streamPhysicalTraits.replace(collation),
      studentStreamScan, logicalSort.offset, logicalSort.fetch)

    (logicalSort, flinkLogicalSort, batchSort, batchSortLocal, batchSortGlobal, streamLimit)
  }

  // equivalent SQL is
  // select * from student order by class asc, score desc limit 20 offset 10
  protected lazy val (
    logicalSortLimit,
    flinkLogicalSortLimit,
    batchSortLimit,
    batchLocalSortLimit,
    batchGlobalSortLimit,
    streamSortLimit) = createSortLimits(() => Seq(
    relBuilder.field("class"),
    relBuilder.desc(relBuilder.field("score"))))

  // equivalent SQL is
  // select * from student order by id asc limit 20 offset 10
  protected lazy val (
      logicalSortLimitById,
      flinkLogicalSortLimitById,
      batchSortLimitById,
      batchLocalSortLimitById,
      batchGlobalSortLimitById,
      streamSortLimitById) = createSortLimits(() => Seq(
      relBuilder.field("id")))

  protected def createSortLimits(sortKeys: () => Seq[RexNode])
    : (RelNode, RelNode, RelNode, RelNode, RelNode, RelNode) = {
    val logicalSortLimit = relBuilder.scan("student").sort(sortKeys())
        .limit(10, 20).build.asInstanceOf[LogicalSort]

    val collection = logicalSortLimit.collation
    val offset = logicalSortLimit.offset
    val fetch = logicalSortLimit.fetch

    val flinkLogicalSortLimit = new FlinkLogicalSort(cluster,
      flinkLogicalTraits.replace(collection), studentFlinkLogicalScan, collection, offset, fetch)

    val batchSortLimit = new BatchPhysicalSortLimit(
      cluster, batchPhysicalTraits.replace(collection),
      new BatchPhysicalExchange(
        cluster, batchPhysicalTraits.replace(FlinkRelDistribution.SINGLETON), studentBatchScan,
        FlinkRelDistribution.SINGLETON),
      collection, offset, fetch, true)

    val batchSortLocalLimit = new BatchPhysicalSortLimit(cluster,
      batchPhysicalTraits.replace(collection), studentBatchScan, collection,
      relBuilder.literal(0),
      relBuilder.literal(SortUtil.getLimitEnd(offset, fetch)),
      false)
    val batchSortGlobal = new BatchPhysicalSortLimit(
      cluster, batchPhysicalTraits.replace(collection),
      new BatchPhysicalExchange(
        cluster, batchPhysicalTraits.replace(FlinkRelDistribution.SINGLETON), batchSortLocalLimit,
        FlinkRelDistribution.SINGLETON),
      collection, offset, fetch, true)

    val streamSort = new StreamPhysicalSortLimit(cluster, streamPhysicalTraits.replace(collection),
      studentStreamScan, collection, offset, fetch, RankProcessStrategy.UNDEFINED_STRATEGY)

    (logicalSortLimit, flinkLogicalSortLimit,
        batchSortLimit, batchSortLocalLimit, batchSortGlobal, streamSort)
  }

  // equivalent SQL is
  // select * from (
  //  select id, name, score, age, height, sex, class,
  //  RANK() over (partition by class order by score) rk from student
  // ) t where rk <= 5
  protected lazy val (
      logicalRank,
      flinkLogicalRank,
      batchLocalRank,
      batchGlobalRank,
      streamRank) = createRanks(6)

  // equivalent SQL is
  // select * from (
  //  select id, name, score, age, height, sex, class,
  //  RANK() over (partition by id order by score) rk from student
  // ) t where rk <= 5
  protected lazy val (
      logicalRankById,
      flinkLogicalRankById,
      batchLocalRankById,
      batchGlobalRankById,
      streamRankById) = createRanks(0)

  protected def createRanks(partitionKey: Int): (RelNode, RelNode, RelNode, RelNode, RelNode) = {
    val logicalRank = new LogicalRank(
      cluster,
      logicalTraits,
      studentLogicalScan,
      ImmutableBitSet.of(partitionKey),
      RelCollations.of(2),
      RankType.RANK,
      new ConstantRankRange(1, 5),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = true
    )

    val flinkLogicalRank = new FlinkLogicalRank(
      cluster,
      flinkLogicalTraits,
      studentFlinkLogicalScan,
      ImmutableBitSet.of(partitionKey),
      RelCollations.of(2),
      RankType.RANK,
      new ConstantRankRange(1, 5),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = true
    )

    val batchLocalRank = new BatchPhysicalRank(
      cluster,
      batchPhysicalTraits,
      studentBatchScan,
      ImmutableBitSet.of(partitionKey),
      RelCollations.of(2),
      RankType.RANK,
      new ConstantRankRange(1, 5),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = false,
      isGlobal = false
    )

    val hash6 = FlinkRelDistribution.hash(Array(partitionKey), requireStrict = true)
    val batchExchange = new BatchPhysicalExchange(
      cluster, batchLocalRank.getTraitSet.replace(hash6), batchLocalRank, hash6)
    val batchGlobalRank = new BatchPhysicalRank(
      cluster,
      batchPhysicalTraits,
      batchExchange,
      ImmutableBitSet.of(partitionKey),
      RelCollations.of(2),
      RankType.RANK,
      new ConstantRankRange(1, 5),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = true,
      isGlobal = true
    )

    val streamExchange = new BatchPhysicalExchange(cluster,
      studentStreamScan.getTraitSet.replace(hash6), studentStreamScan, hash6)
    val streamRank = new StreamPhysicalRank(
      cluster,
      streamPhysicalTraits,
      streamExchange,
      ImmutableBitSet.of(partitionKey),
      RelCollations.of(2),
      RankType.RANK,
      new ConstantRankRange(1, 5),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = true,
      RankProcessStrategy.UNDEFINED_STRATEGY
    )

    (logicalRank, flinkLogicalRank, batchLocalRank, batchGlobalRank, streamRank)
  }

  // equivalent SQL is
  // select * from (
  //  select id, name, score, age, height, sex, class,
  //  RANK() over (partition by age order by score) rk from student
  // ) t where rk <= 5 and rk >= 3
  protected lazy val (
    logicalRank2,
    flinkLogicalRank2,
    batchLocalRank2,
    batchGlobalRank2,
    streamRank2) = {
    val logicalRank = new LogicalRank(
      cluster,
      logicalTraits,
      studentLogicalScan,
      ImmutableBitSet.of(3),
      RelCollations.of(2),
      RankType.RANK,
      new ConstantRankRange(3, 5),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = true
    )

    val flinkLogicalRank = new FlinkLogicalRank(
      cluster,
      flinkLogicalTraits,
      studentFlinkLogicalScan,
      ImmutableBitSet.of(3),
      RelCollations.of(2),
      RankType.RANK,
      new ConstantRankRange(3, 5),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = true
    )

    val batchLocalRank = new BatchPhysicalRank(
      cluster,
      batchPhysicalTraits,
      studentBatchScan,
      ImmutableBitSet.of(3),
      RelCollations.of(2),
      RankType.RANK,
      new ConstantRankRange(1, 5),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = false,
      isGlobal = false
    )

    val hash6 = FlinkRelDistribution.hash(Array(6), requireStrict = true)
    val batchExchange = new BatchPhysicalExchange(
      cluster, batchLocalRank.getTraitSet.replace(hash6), batchLocalRank, hash6)
    val batchGlobalRank = new BatchPhysicalRank(
      cluster,
      batchPhysicalTraits,
      batchExchange,
      ImmutableBitSet.of(3),
      RelCollations.of(2),
      RankType.RANK,
      new ConstantRankRange(3, 5),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = true,
      isGlobal = true
    )

    val streamExchange = new BatchPhysicalExchange(cluster,
      studentStreamScan.getTraitSet.replace(hash6), studentStreamScan, hash6)
    val streamRank = new StreamPhysicalRank(
      cluster,
      streamPhysicalTraits,
      streamExchange,
      ImmutableBitSet.of(3),
      RelCollations.of(2),
      RankType.RANK,
      new ConstantRankRange(3, 5),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = true,
      RankProcessStrategy.UNDEFINED_STRATEGY
    )

    (logicalRank, flinkLogicalRank, batchLocalRank, batchGlobalRank, streamRank)
  }

  // equivalent SQL is
  // select * from (
  //  select id, name, score, age, height, sex, class,
  //  ROW_NUMBER() over (order by height) rn from student
  // ) t where rk > 2 and rk < 7
  protected lazy val (logicalRowNumber, flinkLogicalRowNumber, streamRowNumber) = {
    val logicalRowNumber = new LogicalRank(
      cluster,
      logicalTraits,
      studentLogicalScan,
      ImmutableBitSet.of(),
      RelCollations.of(4),
      RankType.ROW_NUMBER,
      new ConstantRankRange(3, 6),
      new RelDataTypeFieldImpl("rn", 7, longType),
      outputRankNumber = true
    )

    val flinkLogicalRowNumber = new FlinkLogicalRank(
      cluster,
      flinkLogicalTraits,
      studentFlinkLogicalScan,
      ImmutableBitSet.of(),
      RelCollations.of(4),
      RankType.ROW_NUMBER,
      new ConstantRankRange(3, 6),
      new RelDataTypeFieldImpl("rn", 7, longType),
      outputRankNumber = true
    )

    val singleton = FlinkRelDistribution.SINGLETON
    val streamExchange = new BatchPhysicalExchange(cluster,
      studentStreamScan.getTraitSet.replace(singleton), studentStreamScan, singleton)
    val streamRowNumber = new StreamPhysicalRank(
      cluster,
      streamPhysicalTraits,
      streamExchange,
      ImmutableBitSet.of(),
      RelCollations.of(4),
      RankType.ROW_NUMBER,
      new ConstantRankRange(3, 6),
      new RelDataTypeFieldImpl("rn", 7, longType),
      outputRankNumber = true,
      RankProcessStrategy.UNDEFINED_STRATEGY
    )

    (logicalRowNumber, flinkLogicalRowNumber, streamRowNumber)
  }

  // equivalent SQL is
  // select a, b, c from (
  //  select a, b, c, proctime
  //  ROW_NUMBER() over (partition by b order by proctime) rn from TemporalTable3
  // ) t where rn <= 1
  //
  // select a, b, c from (
  //  select a, b, c, proctime
  //  ROW_NUMBER() over (partition by b, c order by proctime desc) rn from TemporalTable3
  // ) t where rn <= 1
  protected lazy val (streamProcTimeDeduplicateFirstRow, streamProcTimeDeduplicateLastRow) = {
    buildFirstRowAndLastRowDeduplicateNode(false)
  }

  // equivalent SQL is
  // select a, b, c from (
  //  select a, b, c, rowtime
  //  ROW_NUMBER() over (partition by b order by rowtime) rn from TemporalTable3
  // ) t where rn <= 1
  //
  // select a, b, c from (
  //  select a, b, c, rowtime
  //  ROW_NUMBER() over (partition by b, c order by rowtime desc) rn from TemporalTable3
  // ) t where rn <= 1
  protected lazy val (streamRowTimeDeduplicateFirstRow, streamRowTimeDeduplicateLastRow) = {
    buildFirstRowAndLastRowDeduplicateNode(true)
  }

  def buildFirstRowAndLastRowDeduplicateNode(isRowtime: Boolean): (RelNode, RelNode) = {
    val scan: StreamPhysicalDataStreamScan =
      createDataStreamScan(ImmutableList.of("TemporalTable3"), streamPhysicalTraits)
    val hash1 = FlinkRelDistribution.hash(Array(1), requireStrict = true)
    val streamExchange1 = new StreamPhysicalExchange(
      cluster, scan.getTraitSet.replace(hash1), scan, hash1)
    val firstRow = new StreamPhysicalDeduplicate(
      cluster,
      streamPhysicalTraits,
      streamExchange1,
      Array(1),
      isRowtime,
      keepLastRow = false
    )

    val builder = typeFactory.builder()
    firstRow.getRowType.getFieldList.dropRight(2).foreach(builder.add)
    val projectProgram = RexProgram.create(
      firstRow.getRowType,
      Array(0, 1, 2).map(i => RexInputRef.of(i, firstRow.getRowType)).toList,
      null,
      builder.build(),
      rexBuilder
    )
    val calcOfFirstRow = new StreamPhysicalCalc(
      cluster,
      streamPhysicalTraits,
      firstRow,
      projectProgram,
      projectProgram.getOutputRowType
    )

    val hash12 = FlinkRelDistribution.hash(Array(1, 2), requireStrict = true)
    val streamExchange2 = new BatchPhysicalExchange(cluster,
      scan.getTraitSet.replace(hash12), scan, hash12)
    val lastRow = new StreamPhysicalDeduplicate(
      cluster,
      streamPhysicalTraits,
      streamExchange2,
      Array(1, 2),
      isRowtime,
      keepLastRow = true
    )
    val calcOfLastRow = new StreamPhysicalCalc(
      cluster,
      streamPhysicalTraits,
      lastRow,
      projectProgram,
      projectProgram.getOutputRowType
    )

    (calcOfFirstRow, calcOfLastRow)
  }

  protected lazy val streamChangelogNormalize = {
    val key = Array(1, 0)
    val hash1 = FlinkRelDistribution.hash(key, requireStrict = true)
    val streamExchange = new StreamPhysicalExchange(
      cluster, studentStreamScan.getTraitSet.replace(hash1), studentStreamScan, hash1)
    new StreamPhysicalChangelogNormalize(
      cluster,
      streamPhysicalTraits,
      streamExchange,
      key)
  }

  protected lazy val streamDropUpdateBefore = {
    new StreamPhysicalDropUpdateBefore(
      cluster,
      streamPhysicalTraits,
      studentStreamScan
    )
  }

  // equivalent SQL is
  // select * from (
  //  select id, name, score, age, height, sex, class,
  //  RANK() over (partition by class order by score) rk from student
  // ) t where rk <= age
  protected lazy val (
    logicalRankWithVariableRange,
    flinkLogicalRankWithVariableRange,
    streamRankWithVariableRange) = {
    val logicalRankWithVariableRange = new LogicalRank(
      cluster,
      logicalTraits,
      studentLogicalScan,
      ImmutableBitSet.of(6),
      RelCollations.of(2),
      RankType.RANK,
      new VariableRankRange(3),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = true
    )

    val flinkLogicalRankWithVariableRange = new FlinkLogicalRank(
      cluster,
      logicalTraits,
      studentFlinkLogicalScan,
      ImmutableBitSet.of(6),
      RelCollations.of(2),
      RankType.RANK,
      new VariableRankRange(3),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = true
    )

    val streamRankWithVariableRange = new StreamPhysicalRank(
      cluster,
      logicalTraits,
      studentStreamScan,
      ImmutableBitSet.of(6),
      RelCollations.of(2),
      RankType.RANK,
      new VariableRankRange(3),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = true,
      RankProcessStrategy.UNDEFINED_STRATEGY
    )

    (logicalRankWithVariableRange, flinkLogicalRankWithVariableRange, streamRankWithVariableRange)
  }

  protected lazy val tableAggCall = {
    val top3 = new Top3
    val resultTypeInfo = UserDefinedFunctionHelper.getReturnTypeOfAggregateFunction(top3)
    val accTypeInfo = UserDefinedFunctionHelper.getAccumulatorTypeOfAggregateFunction(top3)

    val resultDataType = TypeConversions.fromLegacyInfoToDataType(resultTypeInfo)
    val accDataType = TypeConversions.fromLegacyInfoToDataType(accTypeInfo)

    val builder = typeFactory.builder()
    builder.add("f0", new BasicSqlType(typeFactory.getTypeSystem, SqlTypeName.INTEGER))
    builder.add("f1", new BasicSqlType(typeFactory.getTypeSystem, SqlTypeName.INTEGER))
    val relDataType = builder.build()

    AggregateCall.create(
      AggSqlFunction(
        FunctionIdentifier.of("top3"),
        "top3",
        new Top3,
        resultDataType,
        accDataType,
        typeFactory,
        false),
      false,
      false,
      false,
      Seq(Integer.valueOf(3)).toList,
      -1,
      RelCollationImpl.of(),
      relDataType,
      ""
    )
  }

  protected lazy val (logicalTableAgg, flinkLogicalTableAgg, streamExecTableAgg) = {

    val logicalTableAgg = new LogicalTableAggregate(
      cluster,
      logicalTraits,
      studentLogicalScan,
      ImmutableBitSet.of(0),
      null,
      Seq(tableAggCall))

    val flinkLogicalTableAgg = new FlinkLogicalTableAggregate(
      cluster,
      logicalTraits,
      studentLogicalScan,
      ImmutableBitSet.of(0),
      null,
      Seq(tableAggCall)
    )

    val builder = typeFactory.builder()
    builder.add("key", new BasicSqlType(typeFactory.getTypeSystem, SqlTypeName.BIGINT))
    builder.add("f0", new BasicSqlType(typeFactory.getTypeSystem, SqlTypeName.INTEGER))
    builder.add("f1", new BasicSqlType(typeFactory.getTypeSystem, SqlTypeName.INTEGER))
    val relDataType = builder.build()

    val streamTableAgg = new StreamPhysicalGroupTableAggregate(
      cluster,
      logicalTraits,
      studentLogicalScan,
      relDataType,
      Array(0),
      Seq(tableAggCall)
    )

    (logicalTableAgg, flinkLogicalTableAgg, streamTableAgg)
  }

  // equivalent Table API is
  // tEnv.scan("TemporalTable1")
  //  .select("c, a, b, rowtime")
  //  .window(Tumble.over("15.minutes").on("rowtime").as("w"))
  //  .groupBy("a, w")
  //  .flatAggregate("top3(c)")
  //  .select("a, f0, f1, w.start, w.end, w.rowtime, w.proctime")
  protected lazy val (
    logicalWindowTableAgg,
    flinkLogicalWindowTableAgg,
    streamWindowTableAgg) = {

    relBuilder.scan("TemporalTable1")
    val ts = relBuilder.peek()
    val project = relBuilder.project(relBuilder.fields(Seq[Integer](2, 0, 1, 4).toList))
      .build().asInstanceOf[Project]
    val program = RexProgram.create(
      ts.getRowType, project.getProjects, null, project.getRowType, rexBuilder)
    val aggCallOfWindowAgg = Lists.newArrayList(tableAggCall)
    val logicalWindowAgg = new LogicalWindowTableAggregate(
      ts.getCluster,
      ts.getTraitSet,
      project,
      ImmutableBitSet.of(1),
      ImmutableList.of(ImmutableBitSet.of(1)),
      aggCallOfWindowAgg,
      tumblingGroupWindow,
      namedPropertiesOfWindowAgg)

    val flinkLogicalTs: FlinkLogicalDataStreamTableScan =
      createDataStreamScan(ImmutableList.of("TemporalTable1"), flinkLogicalTraits)
    val flinkLogicalWindowAgg = new FlinkLogicalWindowTableAggregate(
      ts.getCluster,
      logicalTraits,
      new FlinkLogicalCalc(ts.getCluster, flinkLogicalTraits, flinkLogicalTs, program),
      ImmutableBitSet.of(1),
      ImmutableList.of(ImmutableBitSet.of(1)),
      aggCallOfWindowAgg,
      tumblingGroupWindow,
      namedPropertiesOfWindowAgg)

    val hash01 = FlinkRelDistribution.hash(Array(1), requireStrict = true)

    val streamTs: StreamPhysicalDataStreamScan =
      createDataStreamScan(ImmutableList.of("TemporalTable1"), streamPhysicalTraits)
    val streamCalc = new StreamPhysicalCalc(
      cluster, streamPhysicalTraits, streamTs, program, program.getOutputRowType)
    val streamExchange = new StreamPhysicalExchange(
      cluster, streamPhysicalTraits.replace(hash01), streamCalc, hash01)
    val emitStrategy = WindowEmitStrategy(tableConfig, tumblingGroupWindow)
    val streamWindowAgg = new StreamPhysicalGroupWindowTableAggregate(
      cluster,
      streamPhysicalTraits,
      streamExchange,
      flinkLogicalWindowAgg.getRowType,
      Array(1),
      flinkLogicalWindowAgg.getAggCallList,
      tumblingGroupWindow,
      namedPropertiesOfWindowAgg,
      emitStrategy
    )

    (logicalWindowAgg, flinkLogicalWindowAgg, streamWindowAgg)
  }

  // equivalent SQL is
  // select age,
  //        avg(score) as avg_score,
  //        sum(score) as sum_score,
  //        max(height) as max_height,
  //        min(height) as min_height,
  //        count(id) as cnt
  // from student group by age
  protected lazy val (
    logicalAgg,
    flinkLogicalAgg,
    batchLocalAgg,
    batchGlobalAggWithLocal,
    batchGlobalAggWithoutLocal,
    streamLocalAgg,
    streamGlobalAggWithLocal,
    streamGlobalAggWithoutLocal) = {
    val logicalAgg = relBuilder.push(studentLogicalScan).aggregate(
      relBuilder.groupKey(relBuilder.field(3)),
      relBuilder.avg(false, "avg_score", relBuilder.field(2)),
      relBuilder.sum(false, "sum_score", relBuilder.field(2)),
      relBuilder.max("max_height", relBuilder.field(4)),
      relBuilder.min("min_height", relBuilder.field(4)),
      relBuilder.count(false, "cnt", relBuilder.field(0))
    ).build().asInstanceOf[LogicalAggregate]

    val flinkLogicalAgg = new FlinkLogicalAggregate(
      cluster,
      flinkLogicalTraits,
      studentFlinkLogicalScan,
      logicalAgg.getGroupSet,
      logicalAgg.getGroupSets,
      logicalAgg.getAggCallList
    )

    val aggCalls = logicalAgg.getAggCallList
    val aggFunctionFactory = new AggFunctionFactory(
      FlinkTypeFactory.toLogicalRowType(studentBatchScan.getRowType),
      Array.empty[Int],
      Array.fill(aggCalls.size())(false),
      false)
    val aggCallToAggFunction = aggCalls.zipWithIndex.map {
      case (call, index) => (call, aggFunctionFactory.createAggFunction(call, index))
    }
    val rowTypeOfLocalAgg = typeFactory.builder
      .add("age", intType)
      .add("sum$0", doubleType)
      .add("count$1", longType)
      .add("sum_score", doubleType)
      .add("max_height", doubleType)
      .add("min_height", doubleType)
      .add("cnt", longType).build()

    val rowTypeOfGlobalAgg = typeFactory.builder
      .add("age", intType)
      .add("avg_score", doubleType)
      .add("sum_score", doubleType)
      .add("max_height", doubleType)
      .add("min_height", doubleType)
      .add("cnt", longType).build()

    val hash0 = FlinkRelDistribution.hash(Array(0), requireStrict = true)
    val hash3 = FlinkRelDistribution.hash(Array(3), requireStrict = true)

    val batchLocalAgg = new BatchPhysicalLocalHashAggregate(
      cluster,
      batchPhysicalTraits,
      studentBatchScan,
      rowTypeOfLocalAgg,
      studentBatchScan.getRowType,
      Array(3),
      auxGrouping = Array(),
      aggCallToAggFunction)

    val batchExchange1 = new BatchPhysicalExchange(
      cluster, batchLocalAgg.getTraitSet.replace(hash0), batchLocalAgg, hash0)
    val batchGlobalAgg = new BatchPhysicalHashAggregate(
      cluster,
      batchPhysicalTraits,
      batchExchange1,
      rowTypeOfGlobalAgg,
      batchExchange1.getRowType,
      batchLocalAgg.getInput.getRowType,
      Array(0),
      auxGrouping = Array(),
      aggCallToAggFunction,
      isMerge = true)

    val batchExchange2 = new BatchPhysicalExchange(cluster,
      studentBatchScan.getTraitSet.replace(hash3), studentBatchScan, hash3)
    val batchGlobalAggWithoutLocal = new BatchPhysicalHashAggregate(
      cluster,
      batchPhysicalTraits,
      batchExchange2,
      rowTypeOfGlobalAgg,
      batchExchange2.getRowType,
      batchExchange2.getRowType,
      Array(3),
      auxGrouping = Array(),
      aggCallToAggFunction,
      isMerge = false)

    val aggCallNeedRetractions = AggregateUtil.deriveAggCallNeedRetractions(
      1, aggCalls, needRetraction = false, null)
    val streamLocalAgg = new StreamPhysicalLocalGroupAggregate(
      cluster,
      streamPhysicalTraits,
      studentStreamScan,
      Array(3),
      aggCalls,
      aggCallNeedRetractions,
      false,
      PartialFinalType.NONE)

    val streamExchange1 = new StreamPhysicalExchange(
      cluster, streamLocalAgg.getTraitSet.replace(hash0), streamLocalAgg, hash0)
    val streamGlobalAgg = new StreamPhysicalGlobalGroupAggregate(
      cluster,
      streamPhysicalTraits,
      streamExchange1,
      rowTypeOfGlobalAgg,
      Array(0),
      aggCalls,
      aggCallNeedRetractions,
      streamLocalAgg.getInput.getRowType,
      AggregateUtil.needRetraction(streamLocalAgg),
      PartialFinalType.NONE)

    val streamExchange2 = new StreamPhysicalExchange(cluster,
      studentStreamScan.getTraitSet.replace(hash3), studentStreamScan, hash3)
    val streamGlobalAggWithoutLocal = new StreamPhysicalGroupAggregate(
      cluster,
      streamPhysicalTraits,
      streamExchange2,
      rowTypeOfGlobalAgg,
      Array(3),
      aggCalls)

    (logicalAgg, flinkLogicalAgg,
      batchLocalAgg, batchGlobalAgg, batchGlobalAggWithoutLocal,
      streamLocalAgg, streamGlobalAgg, streamGlobalAggWithoutLocal)
  }

  // equivalent SQL is
  // select age,
  //        avg(score) as avg_score,
  //        avg(score) filter(where sex = 'M') as m_avg_score,
  //        avg(score) filter(where class > 3) as c3_avg_score,
  //        sum(score) as sum_score,
  //        sum(score) filter(where sex = 'M') as m_sum_score,
  //        sum(score) filter(where class > 3) as c3_sum_score,
  //        max(height) as max_height,
  //        max(height) filter(where sex = 'M') as m_max_height,
  //        max(height) filter(where class > 3) as c3_max_height,
  //        min(height) as min_height,
  //        min(height) filter(where sex = 'M') as m_min_height,
  //        min(height) filter(where class > 3) as c3_min_height,
  //        count(id) as cnt,
  //        count(id) filter(where sex = 'M') as m_cnt,
  //        count(id) filter(where class > 3) as c3_cnt
  // from student group by age
  protected lazy val (
    logicalAggWithFilter,
    flinkLogicalAggWithFilter,
    batchLocalAggWithFilter,
    batchGlobalAggWithLocalWithFilter,
    batchGlobalAggWithoutLocalWithFilter,
    streamLocalAggWithFilter,
    streamGlobalAggWithLocalWithFilter,
    streamGlobalAggWithoutLocalWithFilter) = {

    relBuilder.push(studentLogicalScan)
    val projects = List(
      relBuilder.field(0),
      relBuilder.field(1),
      relBuilder.field(2),
      relBuilder.field(3),
      relBuilder.field(4),
      relBuilder.field(5),
      relBuilder.field(6),
      // sex is not null and sex = 'M'
      relBuilder.call(IS_TRUE,
        relBuilder.call(EQUALS, relBuilder.field(5), relBuilder.literal("M"))),
      // class is not null and class > 3
      relBuilder.call(IS_TRUE,
        relBuilder.call(GREATER_THAN, relBuilder.field(6), relBuilder.literal(3))))
    val outputRowType = typeFactory.buildRelNodeRowType(
      Array("id", "name", "score", "age", "height", "sex", "class", "f7", "f8"),
      Array(new BigIntType, new VarCharType, new DoubleType, new IntType, new DoubleType,
        new VarCharType, new IntType, new BooleanType(false), new BooleanType(false)))
    val calcOnStudentScan = createLogicalCalc(studentLogicalScan, outputRowType, projects, null)
    relBuilder.push(calcOnStudentScan)

    def createSingleArgAggWithFilter(
        aggFunction: SqlAggFunction,
        argIndex: Int,
        filterArg: Int,
        name: String): AggregateCall = {
      AggregateCall.create(
        aggFunction,
        false,
        false,
        false,
        List(Integer.valueOf(argIndex)),
        filterArg,
        RelCollations.EMPTY,
        1,
        calcOnStudentScan,
        null,
        name)
    }

    val aggCallList = List(
      createSingleArgAggWithFilter(AVG, 2, -1, "avg_score"),
      createSingleArgAggWithFilter(AVG, 2, 7, "m_avg_score"),
      createSingleArgAggWithFilter(AVG, 2, 8, "c3_avg_score"),
      createSingleArgAggWithFilter(SUM, 2, -1, "sum_score"),
      createSingleArgAggWithFilter(SUM, 2, 7, "m_sum_score"),
      createSingleArgAggWithFilter(SUM, 2, 8, "c3_sum_score"),
      createSingleArgAggWithFilter(MAX, 4, -1, "max_height"),
      createSingleArgAggWithFilter(MAX, 4, 7, "m_max_height"),
      createSingleArgAggWithFilter(MAX, 4, 8, "c3_max_height"),
      createSingleArgAggWithFilter(MIN, 4, -1, "min_height"),
      createSingleArgAggWithFilter(MIN, 4, 7, "c3_min_height"),
      createSingleArgAggWithFilter(MIN, 4, 8, "c3_min_height"),
      createSingleArgAggWithFilter(COUNT, 0, -1, "cnt"),
      createSingleArgAggWithFilter(COUNT, 0, 7, "m_cnt"),
      createSingleArgAggWithFilter(COUNT, 0, 8, "c3_cnt"))

    val logicalAggWithFilter = LogicalAggregate.create(
      calcOnStudentScan,
      List(),
      ImmutableBitSet.of(3),
      List(ImmutableBitSet.of(3)),
      aggCallList)

    val flinkLogicalAggWithFilter = new FlinkLogicalAggregate(
      cluster,
      flinkLogicalTraits,
      calcOnStudentScan,
      logicalAggWithFilter.getGroupSet,
      logicalAggWithFilter.getGroupSets,
      logicalAggWithFilter.getAggCallList)

    val aggCalls = logicalAggWithFilter.getAggCallList
    val aggFunctionFactory = new AggFunctionFactory(
      FlinkTypeFactory.toLogicalRowType(calcOnStudentScan.getRowType),
      Array.empty[Int],
      Array.fill(aggCalls.size())(false),
      false)
    val aggCallToAggFunction = aggCalls.zipWithIndex.map {
      case (call, index) => (call, aggFunctionFactory.createAggFunction(call, index))
    }
    val rowTypeOfLocalAgg = typeFactory.builder
      .add("age", intType)
      .add("sum$0", doubleType)
      .add("count$1", longType)
      .add("sum$2", doubleType)
      .add("count$3", longType)
      .add("sum$4", doubleType)
      .add("count$5", longType)
      .add("sum$6", doubleType)
      .add("sum$7", doubleType)
      .add("sum$8", doubleType)
      .add("max$9", doubleType)
      .add("max$10", doubleType)
      .add("max$11", doubleType)
      .add("min$12", doubleType)
      .add("min$13", doubleType)
      .add("min$14", doubleType)
      .add("count$15", longType)
      .add("count$16", longType)
      .add("count$17", longType).build()

    val rowTypeOfGlobalAgg = typeFactory.builder
      .add("age", intType)
      .add("avg_score", doubleType)
      .add("m_avg_score", doubleType)
      .add("c3_avg_score", doubleType)
      .add("sum_score", doubleType)
      .add("m_sum_score", doubleType)
      .add("c3_sum_score", doubleType)
      .add("max_height", doubleType)
      .add("m_max_height", doubleType)
      .add("c3_max_height", doubleType)
      .add("min_height", doubleType)
      .add("m_min_height", doubleType)
      .add("c3_min_height", doubleType)
      .add("cnt", longType)
      .add("m_cnt", longType)
      .add("c3_cnt", longType).build()

    val hash0 = FlinkRelDistribution.hash(Array(0), requireStrict = true)
    val hash3 = FlinkRelDistribution.hash(Array(3), requireStrict = true)

    val batchLocalAggWithFilter = new BatchPhysicalLocalHashAggregate(
      cluster,
      batchPhysicalTraits,
      calcOnStudentScan,
      rowTypeOfLocalAgg,
      calcOnStudentScan.getRowType,
      Array(3),
      auxGrouping = Array(),
      aggCallToAggFunction)

    val batchExchange1 = new BatchPhysicalExchange(
      cluster, batchLocalAggWithFilter.getTraitSet.replace(hash0), batchLocalAgg, hash0)
    val batchGlobalAgg = new BatchPhysicalHashAggregate(
      cluster,
      batchPhysicalTraits,
      batchExchange1,
      rowTypeOfGlobalAgg,
      batchExchange1.getRowType,
      batchLocalAggWithFilter.getInput.getRowType,
      Array(0),
      auxGrouping = Array(),
      aggCallToAggFunction,
      isMerge = true)

    val batchExchange2 = new BatchPhysicalExchange(
      cluster,
      calcOnStudentScan.getTraitSet.replace(hash3),
      calcOnStudentScan,
      hash3)
    val batchGlobalAggWithoutLocalWithFilter = new BatchPhysicalHashAggregate(
      cluster,
      batchPhysicalTraits,
      batchExchange2,
      rowTypeOfGlobalAgg,
      batchExchange2.getRowType,
      batchExchange2.getRowType,
      Array(3),
      auxGrouping = Array(),
      aggCallToAggFunction,
      isMerge = false)

    val aggCallNeedRetractions = AggregateUtil.deriveAggCallNeedRetractions(
      1, aggCalls, needRetraction = false, null)
    val streamLocalAggWithFilter = new StreamPhysicalLocalGroupAggregate(
      cluster,
      streamPhysicalTraits,
      calcOnStudentScan,
      Array(3),
      aggCalls,
      aggCallNeedRetractions,
      false,
      PartialFinalType.NONE)

    val streamExchange1 = new StreamPhysicalExchange(
      cluster, streamLocalAggWithFilter.getTraitSet.replace(hash0), streamLocalAgg, hash0)
    val streamGlobalAgg = new StreamPhysicalGlobalGroupAggregate(
      cluster,
      streamPhysicalTraits,
      streamExchange1,
      rowTypeOfGlobalAgg,
      Array(0),
      aggCalls,
      aggCallNeedRetractions,
      streamLocalAggWithFilter.getInput.getRowType,
      AggregateUtil.needRetraction(streamLocalAggWithFilter),
      PartialFinalType.NONE)

    val streamExchange2 = new StreamPhysicalExchange(
      cluster,
      calcOnStudentScan.getTraitSet.replace(hash3),
      calcOnStudentScan,
      hash3)
    val streamGlobalAggWithoutLocalWithFilter = new StreamPhysicalGroupAggregate(
      cluster,
      streamPhysicalTraits,
      streamExchange2,
      rowTypeOfGlobalAgg,
      Array(3),
      aggCalls)

    (logicalAggWithFilter, flinkLogicalAggWithFilter,
      batchLocalAggWithFilter, batchGlobalAgg, batchGlobalAggWithoutLocalWithFilter,
      streamLocalAggWithFilter, streamGlobalAgg, streamGlobalAggWithoutLocalWithFilter)
  }

  // equivalent SQL is
  // equivalent SQL is
  // select avg(score) as avg_score,
  //        sum(score) as sum_score,
  //        count(id) as cnt
  // from student group by id, name, height
  protected lazy val (
    logicalAggWithAuxGroup,
    flinkLogicalAggWithAuxGroup,
    batchLocalAggWithAuxGroup,
    batchGlobalAggWithLocalWithAuxGroup,
    batchGlobalAggWithoutLocalWithAuxGroup) = {
    val logicalAggWithAuxGroup = relBuilder.push(studentLogicalScan).aggregate(
      relBuilder.groupKey(relBuilder.field(0)),
      relBuilder.aggregateCall(FlinkSqlOperatorTable.AUXILIARY_GROUP, relBuilder.field(1)),
      relBuilder.aggregateCall(FlinkSqlOperatorTable.AUXILIARY_GROUP, relBuilder.field(4)),
      relBuilder.avg(false, "avg_score", relBuilder.field(2)),
      relBuilder.sum(false, "sum_score", relBuilder.field(2)),
      relBuilder.count(false, "cnt", relBuilder.field(0))
    ).build().asInstanceOf[LogicalAggregate]

    val flinkLogicalAggWithAuxGroup = new FlinkLogicalAggregate(
      cluster,
      flinkLogicalTraits,
      studentFlinkLogicalScan,
      logicalAggWithAuxGroup.getGroupSet,
      logicalAggWithAuxGroup.getGroupSets,
      logicalAggWithAuxGroup.getAggCallList
    )

    val aggCalls = logicalAggWithAuxGroup.getAggCallList.filter {
      call => call.getAggregation != FlinkSqlOperatorTable.AUXILIARY_GROUP
    }
    val aggFunctionFactory = new AggFunctionFactory(
      FlinkTypeFactory.toLogicalRowType(studentBatchScan.getRowType),
      Array.empty[Int],
      Array.fill(aggCalls.size())(false),
      false)
    val aggCallToAggFunction = aggCalls.zipWithIndex.map {
      case (call, index) => (call, aggFunctionFactory.createAggFunction(call, index))
    }
    val rowTypeOfLocalAgg = typeFactory.builder
      .add("id", intType)
      .add("name", stringType)
      .add("height", doubleType)
      .add("sum$0", doubleType)
      .add("count$1", longType)
      .add("sum_score", doubleType)
      .add("cnt", longType).build()

    val batchLocalAggWithAuxGroup = new BatchPhysicalLocalHashAggregate(
      cluster,
      batchPhysicalTraits,
      studentBatchScan,
      rowTypeOfLocalAgg,
      studentBatchScan.getRowType,
      Array(0),
      auxGrouping = Array(1, 4),
      aggCallToAggFunction)

    val hash0 = FlinkRelDistribution.hash(Array(0), requireStrict = true)
    val batchExchange = new BatchPhysicalExchange(cluster,
      batchLocalAggWithAuxGroup.getTraitSet.replace(hash0), batchLocalAggWithAuxGroup, hash0)

    val rowTypeOfGlobalAgg = typeFactory.builder
      .add("id", intType)
      .add("name", stringType)
      .add("height", doubleType)
      .add("avg_score", doubleType)
      .add("sum_score", doubleType)
      .add("cnt", longType).build()
    val batchGlobalAggWithAuxGroup = new BatchPhysicalHashAggregate(
      cluster,
      batchPhysicalTraits,
      batchExchange,
      rowTypeOfGlobalAgg,
      batchExchange.getRowType,
      batchLocalAggWithAuxGroup.getInput.getRowType,
      Array(0),
      auxGrouping = Array(1, 2),
      aggCallToAggFunction,
      isMerge = true)

    val batchExchange2 = new BatchPhysicalExchange(cluster,
      studentBatchScan.getTraitSet.replace(hash0), studentBatchScan, hash0)
    val batchGlobalAggWithoutLocalWithAuxGroup = new BatchPhysicalHashAggregate(
      cluster,
      batchPhysicalTraits,
      batchExchange2,
      rowTypeOfGlobalAgg,
      batchExchange2.getRowType,
      batchExchange2.getRowType,
      Array(0),
      auxGrouping = Array(1, 4),
      aggCallToAggFunction,
      isMerge = false)

    (logicalAggWithAuxGroup, flinkLogicalAggWithAuxGroup,
      batchLocalAggWithAuxGroup, batchGlobalAggWithAuxGroup, batchGlobalAggWithoutLocalWithAuxGroup)
  }

  // For window start/end/proc_time the windowAttribute inferred type is a hard code val,
  // only for row_time we distinguish by batch row time, for what we hard code DataTypes.TIMESTAMP,
  // which is ok here for testing.
  private lazy val windowRef: PlannerWindowReference =
    new PlannerWindowReference("w$", new TimestampType(3))

  protected lazy val tumblingGroupWindow: LogicalWindow =
    TumblingGroupWindow(
      windowRef,
      new FieldReferenceExpression(
        "rowtime",
        new AtomicDataType(new TimestampType(true, TimestampKind.ROWTIME, 3)),
        0,
        4),
      intervalOfMillis(900000)
    )

  protected lazy val namedPropertiesOfWindowAgg: Seq[PlannerNamedWindowProperty] =
    Seq(new PlannerNamedWindowProperty("w$start", new PlannerWindowStart(windowRef)),
      new PlannerNamedWindowProperty("w$end", new PlannerWindowStart(windowRef)),
      new PlannerNamedWindowProperty("w$rowtime", new PlannerRowtimeAttribute(windowRef)),
      new PlannerNamedWindowProperty("w$proctime", new PlannerProctimeAttribute(windowRef)))

  // equivalent SQL is
  // select a, b, count(c) as s,
  //   TUMBLE_START(rowtime, INTERVAL '15' MINUTE) as w$start,
  //   TUMBLE_END(rowtime, INTERVAL '15' MINUTE) as w$end,
  //   TUMBLE_ROWTIME(rowtime, INTERVAL '15' MINUTE) as w$rowtime,
  //   TUMBLE_PROCTIME(rowtime, INTERVAL '15' MINUTE) as w$proctime
  // from TemporalTable1 group by a, b, TUMBLE(rowtime, INTERVAL '15' MINUTE)
  protected lazy val (
    logicalWindowAgg,
    flinkLogicalWindowAgg,
    batchLocalWindowAgg,
    batchGlobalWindowAggWithLocalAgg,
    batchGlobalWindowAggWithoutLocalAgg,
    streamWindowAgg) = {
    relBuilder.scan("TemporalTable1")
    val ts = relBuilder.peek()
    val project = relBuilder.project(relBuilder.fields(Seq[Integer](0, 1, 4, 2).toList))
      .build().asInstanceOf[Project]
    val program = RexProgram.create(
      ts.getRowType, project.getProjects, null, project.getRowType, rexBuilder)
    val aggCallOfWindowAgg = Lists.newArrayList(AggregateCall.create(
      new SqlCountAggFunction("COUNT"), false, false, List[Integer](3), -1, 2, project, null, "s"))
    // TUMBLE(rowtime, INTERVAL '15' MINUTE))
    val logicalWindowAgg = new LogicalWindowAggregate(
      ts.getCluster,
      ts.getTraitSet,
      project,
      ImmutableBitSet.of(0, 1),
      aggCallOfWindowAgg,
      tumblingGroupWindow,
      namedPropertiesOfWindowAgg)

    val flinkLogicalTs: FlinkLogicalDataStreamTableScan =
      createDataStreamScan(ImmutableList.of("TemporalTable1"), flinkLogicalTraits)
    val flinkLogicalWindowAgg = new FlinkLogicalWindowAggregate(
      ts.getCluster,
      logicalTraits,
      new FlinkLogicalCalc(ts.getCluster, flinkLogicalTraits, flinkLogicalTs, program),
      ImmutableBitSet.of(0, 1),
      aggCallOfWindowAgg,
      tumblingGroupWindow,
      namedPropertiesOfWindowAgg)

    val batchTs: BatchPhysicalBoundedStreamScan =
      createDataStreamScan(ImmutableList.of("TemporalTable1"), batchPhysicalTraits)
    val batchCalc = new BatchPhysicalCalc(
      cluster, batchPhysicalTraits, batchTs, program, program.getOutputRowType)
    val hash01 = FlinkRelDistribution.hash(Array(0, 1), requireStrict = true)
    val batchExchange1 = new BatchPhysicalExchange(
      cluster, batchPhysicalTraits.replace(hash01), batchCalc, hash01)
    val (_, _, aggregates) =
      AggregateUtil.transformToBatchAggregateFunctions(
        FlinkTypeFactory.toLogicalRowType(batchExchange1.getRowType),
        flinkLogicalWindowAgg.getAggCallList)
    val aggCallToAggFunction = flinkLogicalWindowAgg.getAggCallList.zip(aggregates)

    val localWindowAggTypes =
      (Array(0, 1).map(batchCalc.getRowType.getFieldList.get(_).getType) ++ // grouping
        Array(longType) ++ // assignTs
        aggCallOfWindowAgg.map(_.getType)).toList // agg calls
    val localWindowAggNames =
      (Array(0, 1).map(batchCalc.getRowType.getFieldNames.get(_)) ++ // grouping
        Array("assignedWindow$") ++ // assignTs
        Array("count$0")).toList // agg calls
    val localWindowAggRowType = typeFactory.createStructType(
      localWindowAggTypes, localWindowAggNames)
    val batchLocalWindowAgg = new BatchPhysicalLocalHashWindowAggregate(
      batchCalc.getCluster,
      batchPhysicalTraits,
      batchCalc,
      localWindowAggRowType,
      batchCalc.getRowType,
      Array(0, 1),
      Array.empty,
      aggCallToAggFunction,
      tumblingGroupWindow,
      inputTimeFieldIndex = 2,
      inputTimeIsDate = false,
      namedPropertiesOfWindowAgg,
      enableAssignPane = false)
    val batchExchange2 = new BatchPhysicalExchange(
      cluster, batchPhysicalTraits.replace(hash01), batchLocalWindowAgg, hash01)
    val batchWindowAggWithLocal = new BatchPhysicalHashWindowAggregate(
      cluster,
      batchPhysicalTraits,
      batchExchange2,
      flinkLogicalWindowAgg.getRowType,
      batchCalc.getRowType,
      Array(0, 1),
      Array.empty,
      aggCallToAggFunction,
      tumblingGroupWindow,
      inputTimeFieldIndex = 2,
      inputTimeIsDate = false,
      namedPropertiesOfWindowAgg,
      enableAssignPane = false,
      isMerge = true
    )

    val batchWindowAggWithoutLocal = new BatchPhysicalHashWindowAggregate(
      batchExchange1.getCluster,
      batchPhysicalTraits,
      batchExchange1,
      flinkLogicalWindowAgg.getRowType,
      batchExchange1.getRowType,
      Array(0, 1),
      Array.empty,
      aggCallToAggFunction,
      tumblingGroupWindow,
      inputTimeFieldIndex = 2,
      inputTimeIsDate = false,
      namedPropertiesOfWindowAgg,
      enableAssignPane = false,
      isMerge = false
    )

    val streamTs: StreamPhysicalDataStreamScan =
      createDataStreamScan(ImmutableList.of("TemporalTable1"), streamPhysicalTraits)
    val streamCalc = new BatchPhysicalCalc(
      cluster, streamPhysicalTraits, streamTs, program, program.getOutputRowType)
    val streamExchange = new StreamPhysicalExchange(
      cluster, streamPhysicalTraits.replace(hash01), streamCalc, hash01)
    val emitStrategy = WindowEmitStrategy(tableConfig, tumblingGroupWindow)
    val streamWindowAgg = new StreamPhysicalGroupWindowAggregate(
      cluster,
      streamPhysicalTraits,
      streamExchange,
      flinkLogicalWindowAgg.getRowType,
      Array(0, 1),
      flinkLogicalWindowAgg.getAggCallList,
      tumblingGroupWindow,
      namedPropertiesOfWindowAgg,
      emitStrategy
    )

    (logicalWindowAgg, flinkLogicalWindowAgg, batchLocalWindowAgg, batchWindowAggWithLocal,
      batchWindowAggWithoutLocal, streamWindowAgg)
  }

  // equivalent SQL is
  // select b, count(a) as s,
  //   TUMBLE_START(rowtime, INTERVAL '15' MINUTE) as w$start,
  //   TUMBLE_END(rowtime, INTERVAL '15' MINUTE) as w$end,
  //   TUMBLE_ROWTIME(rowtime, INTERVAL '15' MINUTE) as w$rowtime,
  //   TUMBLE_PROCTIME(rowtime, INTERVAL '15' MINUTE) as w$proctime
  // from TemporalTable1 group by b, TUMBLE(rowtime, INTERVAL '15' MINUTE)
  protected lazy val (
    logicalWindowAgg2,
    flinkLogicalWindowAgg2,
    batchLocalWindowAgg2,
    batchGlobalWindowAggWithLocalAgg2,
    batchGlobalWindowAggWithoutLocalAgg2,
    streamWindowAgg2) = {
    relBuilder.scan("TemporalTable1")
    val ts = relBuilder.peek()
    val project = relBuilder.project(relBuilder.fields(Seq[Integer](0, 1, 4).toList))
      .build().asInstanceOf[Project]
    val program = RexProgram.create(
      ts.getRowType, project.getProjects, null, project.getRowType, rexBuilder)
    val aggCallOfWindowAgg = Lists.newArrayList(AggregateCall.create(
      new SqlCountAggFunction("COUNT"), false, false, List[Integer](0), -1, 1, project, null, "s"))
    // TUMBLE(rowtime, INTERVAL '15' MINUTE))
    val logicalWindowAgg = new LogicalWindowAggregate(
      ts.getCluster,
      ts.getTraitSet,
      project,
      ImmutableBitSet.of(1),
      aggCallOfWindowAgg,
      tumblingGroupWindow,
      namedPropertiesOfWindowAgg)

    val flinkLogicalTs: FlinkLogicalDataStreamTableScan =
      createDataStreamScan(ImmutableList.of("TemporalTable1"), flinkLogicalTraits)
    val flinkLogicalWindowAgg = new FlinkLogicalWindowAggregate(
      ts.getCluster,
      logicalTraits,
      new FlinkLogicalCalc(ts.getCluster, flinkLogicalTraits, flinkLogicalTs, program),
      ImmutableBitSet.of(1),
      aggCallOfWindowAgg,
      tumblingGroupWindow,
      namedPropertiesOfWindowAgg)

    val batchTs: BatchPhysicalBoundedStreamScan =
      createDataStreamScan(ImmutableList.of("TemporalTable1"), batchPhysicalTraits)
    val batchCalc = new BatchPhysicalCalc(
      cluster, batchPhysicalTraits, batchTs, program, program.getOutputRowType)
    val hash1 = FlinkRelDistribution.hash(Array(1), requireStrict = true)
    val batchExchange1 = new BatchPhysicalExchange(
      cluster, batchPhysicalTraits.replace(hash1), batchCalc, hash1)
    val (_, _, aggregates) =
      AggregateUtil.transformToBatchAggregateFunctions(
        FlinkTypeFactory.toLogicalRowType(batchExchange1.getRowType),
        flinkLogicalWindowAgg.getAggCallList)
    val aggCallToAggFunction = flinkLogicalWindowAgg.getAggCallList.zip(aggregates)

    val localWindowAggTypes =
      (Array(batchCalc.getRowType.getFieldList.get(1).getType) ++ // grouping
        Array(longType) ++ // assignTs
        aggCallOfWindowAgg.map(_.getType)).toList // agg calls
    val localWindowAggNames =
      (Array(batchCalc.getRowType.getFieldNames.get(1)) ++ // grouping
        Array("assignedWindow$") ++ // assignTs
        Array("count$0")).toList // agg calls
    val localWindowAggRowType = typeFactory.createStructType(
      localWindowAggTypes, localWindowAggNames)
    val batchLocalWindowAgg = new BatchPhysicalLocalHashWindowAggregate(
      batchCalc.getCluster,
      batchPhysicalTraits,
      batchCalc,
      localWindowAggRowType,
      batchCalc.getRowType,
      Array(1),
      Array.empty,
      aggCallToAggFunction,
      tumblingGroupWindow,
      inputTimeFieldIndex = 2,
      inputTimeIsDate = false,
      namedPropertiesOfWindowAgg,
      enableAssignPane = false)
    val batchExchange2 = new BatchPhysicalExchange(
      cluster, batchPhysicalTraits.replace(hash1), batchLocalWindowAgg, hash1)
    val batchWindowAggWithLocal = new BatchPhysicalHashWindowAggregate(
      cluster,
      batchPhysicalTraits,
      batchExchange2,
      flinkLogicalWindowAgg.getRowType,
      batchCalc.getRowType,
      Array(0),
      Array.empty,
      aggCallToAggFunction,
      tumblingGroupWindow,
      inputTimeFieldIndex = 2,
      inputTimeIsDate = false,
      namedPropertiesOfWindowAgg,
      enableAssignPane = false,
      isMerge = true
    )

    val batchWindowAggWithoutLocal = new BatchPhysicalHashWindowAggregate(
      batchExchange1.getCluster,
      batchPhysicalTraits,
      batchExchange1,
      flinkLogicalWindowAgg.getRowType,
      batchExchange1.getRowType,
      Array(1),
      Array.empty,
      aggCallToAggFunction,
      tumblingGroupWindow,
      inputTimeFieldIndex = 2,
      inputTimeIsDate = false,
      namedPropertiesOfWindowAgg,
      enableAssignPane = false,
      isMerge = false
    )

    val streamTs: StreamPhysicalDataStreamScan =
      createDataStreamScan(ImmutableList.of("TemporalTable1"), streamPhysicalTraits)
    val streamCalc = new StreamPhysicalCalc(
      cluster, streamPhysicalTraits, streamTs, program, program.getOutputRowType)
    val streamExchange = new StreamPhysicalExchange(
      cluster, streamPhysicalTraits.replace(hash1), streamCalc, hash1)
    val emitStrategy = WindowEmitStrategy(tableConfig, tumblingGroupWindow)
    val streamWindowAgg = new StreamPhysicalGroupWindowAggregate(
      cluster,
      streamPhysicalTraits,
      streamExchange,
      flinkLogicalWindowAgg.getRowType,
      Array(1),
      flinkLogicalWindowAgg.getAggCallList,
      tumblingGroupWindow,
      namedPropertiesOfWindowAgg,
      emitStrategy
    )

    (logicalWindowAgg, flinkLogicalWindowAgg, batchLocalWindowAgg, batchWindowAggWithLocal,
      batchWindowAggWithoutLocal, streamWindowAgg)
  }

  // equivalent SQL is
  // select a, c, count(b) as s,
  //   TUMBLE_START(rowtime, INTERVAL '15' MINUTE) as w$start,
  //   TUMBLE_END(rowtime, INTERVAL '15' MINUTE) as w$end,
  //   TUMBLE_ROWTIME(rowtime, INTERVAL '15' MINUTE) as w$rowtime,
  //   TUMBLE_PROCTIME(rowtime, INTERVAL '15' MINUTE) as w$proctime
  // from TemporalTable2 group by a, c, TUMBLE(rowtime, INTERVAL '15' MINUTE)
  protected lazy val (
    logicalWindowAggWithAuxGroup,
    flinkLogicalWindowAggWithAuxGroup,
    batchLocalWindowAggWithAuxGroup,
    batchGlobalWindowAggWithLocalAggWithAuxGroup,
    batchGlobalWindowAggWithoutLocalAggWithAuxGroup) = {
    relBuilder.scan("TemporalTable2")
    val ts = relBuilder.peek()
    val project = relBuilder.project(relBuilder.fields(Seq[Integer](0, 2, 4, 1).toList))
      .build().asInstanceOf[Project]
    val program = RexProgram.create(
      ts.getRowType, project.getProjects, null, project.getRowType, rexBuilder)
    val aggCallOfWindowAgg = Lists.newArrayList(
      AggregateCall.create(FlinkSqlOperatorTable.AUXILIARY_GROUP, false, false,
        List[Integer](1), -1, 1, project, null, "c"),
      AggregateCall.create(new SqlCountAggFunction("COUNT"), false, false,
        List[Integer](3), -1, 2, project, null, "s"))
    // TUMBLE(rowtime, INTERVAL '15' MINUTE))
    val logicalWindowAggWithAuxGroup = new LogicalWindowAggregate(
      ts.getCluster,
      ts.getTraitSet,
      project,
      ImmutableBitSet.of(0),
      aggCallOfWindowAgg,
      tumblingGroupWindow,
      namedPropertiesOfWindowAgg)

    val flinkLogicalTs: FlinkLogicalDataStreamTableScan =
      createDataStreamScan(ImmutableList.of("TemporalTable2"), flinkLogicalTraits)
    val flinkLogicalWindowAggWithAuxGroup = new FlinkLogicalWindowAggregate(
      ts.getCluster,
      logicalTraits,
      new FlinkLogicalCalc(ts.getCluster, flinkLogicalTraits, flinkLogicalTs, program),
      ImmutableBitSet.of(0),
      aggCallOfWindowAgg,
      tumblingGroupWindow,
      namedPropertiesOfWindowAgg)

    val batchTs: BatchPhysicalBoundedStreamScan =
      createDataStreamScan(ImmutableList.of("TemporalTable2"), batchPhysicalTraits)
    val batchCalc = new BatchPhysicalCalc(
      cluster, batchPhysicalTraits, batchTs, program, program.getOutputRowType)
    val hash0 = FlinkRelDistribution.hash(Array(0), requireStrict = true)
    val batchExchange1 = new BatchPhysicalExchange(
      cluster, batchPhysicalTraits.replace(hash0), batchCalc, hash0)
    val aggCallsWithoutAuxGroup = flinkLogicalWindowAggWithAuxGroup.getAggCallList.drop(1)
    val (_, _, aggregates) =
      AggregateUtil.transformToBatchAggregateFunctions(
        FlinkTypeFactory.toLogicalRowType(batchExchange1.getRowType),
        aggCallsWithoutAuxGroup)
    val aggCallToAggFunction = aggCallsWithoutAuxGroup.zip(aggregates)

    val localWindowAggTypes =
      (Array(batchCalc.getRowType.getFieldList.get(0).getType) ++ // grouping
        Array(longType) ++ // assignTs
        Array(batchCalc.getRowType.getFieldList.get(1).getType) ++ // auxGrouping
        aggCallsWithoutAuxGroup.map(_.getType)).toList // agg calls
    val localWindowAggNames =
      (Array(batchCalc.getRowType.getFieldNames.get(0)) ++ // grouping
        Array("assignedWindow$") ++ // assignTs
        Array(batchCalc.getRowType.getFieldNames.get(1)) ++ // auxGrouping
        Array("count$0")).toList // agg calls
    val localWindowAggRowType = typeFactory.createStructType(
      localWindowAggTypes, localWindowAggNames)
    val batchLocalWindowAggWithAuxGroup = new BatchPhysicalLocalHashWindowAggregate(
      batchCalc.getCluster,
      batchPhysicalTraits,
      batchCalc,
      localWindowAggRowType,
      batchCalc.getRowType,
      Array(0),
      Array(1),
      aggCallToAggFunction,
      tumblingGroupWindow,
      inputTimeFieldIndex = 2,
      inputTimeIsDate = false,
      namedPropertiesOfWindowAgg,
      enableAssignPane = false)
    val batchExchange2 = new BatchPhysicalExchange(
      cluster, batchPhysicalTraits.replace(hash0), batchLocalWindowAggWithAuxGroup, hash0)
    val batchWindowAggWithLocalWithAuxGroup = new BatchPhysicalHashWindowAggregate(
      cluster,
      batchPhysicalTraits,
      batchExchange2,
      flinkLogicalWindowAggWithAuxGroup.getRowType,
      batchCalc.getRowType,
      Array(0),
      Array(2), // local output grouping keys: grouping + assignTs + auxGrouping
      aggCallToAggFunction,
      tumblingGroupWindow,
      inputTimeFieldIndex = 2,
      inputTimeIsDate = false,
      namedPropertiesOfWindowAgg,
      enableAssignPane = false,
      isMerge = true
    )

    val batchWindowAggWithoutLocalWithAuxGroup = new BatchPhysicalHashWindowAggregate(
      batchExchange1.getCluster,
      batchPhysicalTraits,
      batchExchange1,
      flinkLogicalWindowAggWithAuxGroup.getRowType,
      batchExchange1.getRowType,
      Array(0),
      Array(1),
      aggCallToAggFunction,
      tumblingGroupWindow,
      inputTimeFieldIndex = 2,
      inputTimeIsDate = false,
      namedPropertiesOfWindowAgg,
      enableAssignPane = false,
      isMerge = false
    )

    (logicalWindowAggWithAuxGroup, flinkLogicalWindowAggWithAuxGroup,
      batchLocalWindowAggWithAuxGroup, batchWindowAggWithLocalWithAuxGroup,
      batchWindowAggWithoutLocalWithAuxGroup)
  }

  // equivalent SQL is
  // select id, name, score, age, class,
  //  row_number() over(partition by class order by name) as rn,
  //  rank() over (partition by class order by score) as rk,
  //  dense_rank() over (partition by class order by score) as drk,
  //  avg(score) over (partition by class order by score) as avg_score,
  //  max(score) over (partition by age) as max_score,
  //  count(id) over (partition by age) as cnt
  //  from student
  protected lazy val (flinkLogicalOverAgg, batchOverAgg) = {
    val types = Map(
      "id" -> longType,
      "name" -> stringType,
      "score" -> doubleType,
      "age" -> intType,
      "class" -> intType,
      "rn" -> longType,
      "rk" -> longType,
      "drk" -> longType,
      "avg_score" -> doubleType,
      "count$0_score" -> longType,
      "sum$0_score" -> doubleType,
      "max_score" -> doubleType,
      "cnt" -> longType
    )

    def createRowType(selectFields: String*): RelDataType = {
      val builder = typeFactory.builder
      selectFields.foreach { f =>
        builder.add(f, types.getOrElse(f, throw new IllegalArgumentException(s"$f does not exist")))
      }
      builder.build()
    }

    val rowTypeOfCalc = createRowType("id", "name", "score", "age", "class")
    val rexProgram = RexProgram.create(
      studentFlinkLogicalScan.getRowType,
      Array(0, 1, 2, 3, 6).map(i => RexInputRef.of(i, studentFlinkLogicalScan.getRowType)).toList,
      null,
      rowTypeOfCalc,
      rexBuilder
    )

    val rowTypeOfWindowAgg = createRowType(
      "id", "name", "score", "age", "class", "rn", "rk", "drk",
      "count$0_score", "sum$0_score", "max_score", "cnt")
    val flinkLogicalOverAgg = new FlinkLogicalOverAggregate(
      cluster,
      flinkLogicalTraits,
      new FlinkLogicalCalc(cluster, flinkLogicalTraits, studentFlinkLogicalScan, rexProgram),
      ImmutableList.of(),
      rowTypeOfWindowAgg,
      overAggGroups
    )

    val rowTypeOfWindowAggOutput = createRowType(
      "id", "name", "score", "age", "class", "rn", "rk", "drk", "avg_score", "max_score", "cnt")
    val projectProgram = RexProgram.create(
      flinkLogicalOverAgg.getRowType,
      (0 until flinkLogicalOverAgg.getRowType.getFieldCount).flatMap { i =>
        if (i < 8 || i >= 10) {
          Array[RexNode](RexInputRef.of(i, flinkLogicalOverAgg.getRowType))
        } else if (i == 8) {
          Array[RexNode](rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE,
            RexInputRef.of(8, flinkLogicalOverAgg.getRowType),
            RexInputRef.of(9, flinkLogicalOverAgg.getRowType)))
        } else {
          Array.empty[RexNode]
        }
      }.toList,
      null,
      rowTypeOfWindowAggOutput,
      rexBuilder
    )

    val flinkLogicalOverAggOutput = new FlinkLogicalCalc(
      cluster,
      flinkLogicalTraits,
      flinkLogicalOverAgg,
      projectProgram
    )

    val calc = new BatchPhysicalCalc(
      cluster, batchPhysicalTraits, studentBatchScan, rexProgram, rowTypeOfCalc)
    val hash4 = FlinkRelDistribution.hash(Array(4), requireStrict = true)
    val exchange1 = new BatchPhysicalExchange(cluster, calc.getTraitSet.replace(hash4), calc, hash4)
    // sort class, name
    val collection1 = RelCollations.of(
      FlinkRelOptUtil.ofRelFieldCollation(4), FlinkRelOptUtil.ofRelFieldCollation(1))
    val newSortTrait1 = exchange1.getTraitSet.replace(collection1)
    val sort1 = new BatchPhysicalSort(cluster, newSortTrait1, exchange1,
      newSortTrait1.getTrait(RelCollationTraitDef.INSTANCE))

    val outputRowType1 = createRowType("id", "name", "score", "age", "class", "rn")
    val innerWindowAgg1 = new BatchPhysicalOverAggregate(
      cluster,
      batchPhysicalTraits,
      sort1,
      outputRowType1,
      sort1.getRowType,
      Seq(overAggGroups(0)),
      flinkLogicalOverAgg
    )

    // sort class, score
    val collation2 = RelCollations.of(
      FlinkRelOptUtil.ofRelFieldCollation(4), FlinkRelOptUtil.ofRelFieldCollation(2))
    val newSortTrait2 = innerWindowAgg1.getTraitSet.replace(collation2)
    val sort2 = new BatchPhysicalSort(cluster, newSortTrait2, innerWindowAgg1,
      newSortTrait2.getTrait(RelCollationTraitDef.INSTANCE))

    val outputRowType2 = createRowType(
      "id", "name", "score", "age", "class", "rn", "rk", "drk", "count$0_score", "sum$0_score")
    val innerWindowAgg2 = new BatchPhysicalOverAggregate(
      cluster,
      batchPhysicalTraits,
      sort2,
      outputRowType2,
      sort2.getRowType,
      Seq(overAggGroups(1)),
      flinkLogicalOverAgg
    )

    val hash3 = FlinkRelDistribution.hash(Array(3), requireStrict = true)
    val exchange2 = new BatchPhysicalExchange(
      cluster, innerWindowAgg2.getTraitSet.replace(hash3), innerWindowAgg2, hash3)

    val outputRowType3 = createRowType(
      "id", "name", "score", "age", "class", "rn", "rk", "drk",
      "count$0_score", "sum$0_score", "max_score", "cnt")
    val batchWindowAgg = new BatchPhysicalOverAggregate(
      cluster,
      batchPhysicalTraits,
      exchange2,
      outputRowType3,
      exchange2.getRowType,
      Seq(overAggGroups(2)),
      flinkLogicalOverAgg
    )

    val batchWindowAggOutput = new BatchPhysicalCalc(
      cluster,
      batchPhysicalTraits,
      batchWindowAgg,
      projectProgram,
      projectProgram.getOutputRowType
    )

    (flinkLogicalOverAggOutput, batchWindowAggOutput)
  }

  // equivalent SQL is
  // select id, name, score, age, class,
  //  rank() over (partition by class order by score) as rk,
  //  dense_rank() over (partition by class order by score) as drk,
  //  avg(score) over (partition by class order by score) as avg_score
  //  from student
  protected lazy val streamOverAgg: StreamPhysicalRel = createStreamOverAgg(overAggGroups.get(1), 4)

  protected lazy val streamOverAggById: StreamPhysicalRel = createStreamOverAgg(
    new Window.Group(
      ImmutableBitSet.of(0),
      true,
      RexWindowBound.create(SqlWindow.createUnboundedPreceding(new SqlParserPos(0, 0)), null),
      RexWindowBound.create(SqlWindow.createCurrentRow(new SqlParserPos(0, 0)), null),
      RelCollationImpl.of(new RelFieldCollation(
        1, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.FIRST)),
      ImmutableList.of(
        new Window.RexWinAggCall(
          SqlStdOperatorTable.ROW_NUMBER,
          longType,
          ImmutableList.of[RexNode](),
          0,
          false
        )
      )
    ), 0
  )

  protected def createStreamOverAgg(group: Window.Group, hash: Int): StreamPhysicalRel = {
    val types = Map(
      "id" -> longType,
      "name" -> stringType,
      "score" -> doubleType,
      "age" -> intType,
      "class" -> intType,
      "rk" -> longType,
      "drk" -> longType,
      "avg_score" -> doubleType,
      "count$0_score" -> longType,
      "sum$0_score" -> doubleType
    )

    def createRowType(selectFields: String*): RelDataType = {
      val builder = typeFactory.builder
      selectFields.foreach { f =>
        builder.add(f, types.getOrElse(f, throw new IllegalArgumentException(s"$f does not exist")))
      }
      builder.build()
    }

    val rowTypeOfCalc = createRowType("id", "name", "score", "age", "class")
    val rexProgram = RexProgram.create(
      studentFlinkLogicalScan.getRowType,
      Array(0, 1, 2, 3, 6).map(i => RexInputRef.of(i, studentFlinkLogicalScan.getRowType)).toList,
      null,
      rowTypeOfCalc,
      rexBuilder
    )

    val rowTypeOfWindowAgg = createRowType(
      "id", "name", "score", "age", "class", "rk", "drk", "count$0_score", "sum$0_score")
    val flinkLogicalOverAgg = new FlinkLogicalOverAggregate(
      cluster,
      flinkLogicalTraits,
      new FlinkLogicalCalc(cluster, flinkLogicalTraits, studentFlinkLogicalScan, rexProgram),
      ImmutableList.of(),
      rowTypeOfWindowAgg,
      util.Arrays.asList(group)
    )

    val streamScan: StreamPhysicalDataStreamScan =
      createDataStreamScan(ImmutableList.of("student"), streamPhysicalTraits)
    val calc = new StreamPhysicalCalc(
      cluster, streamPhysicalTraits, streamScan, rexProgram, rowTypeOfCalc)
    val hash4 = FlinkRelDistribution.hash(Array(hash), requireStrict = true)
    val exchange = new StreamPhysicalExchange(cluster, calc.getTraitSet.replace(hash4), calc, hash4)

    val windowAgg = new StreamPhysicalOverAggregate(
      cluster,
      streamPhysicalTraits,
      exchange,
      rowTypeOfWindowAgg,
      flinkLogicalOverAgg
    )

    val rowTypeOfWindowAggOutput = createRowType(
      "id", "name", "score", "age", "class", "rk", "drk", "avg_score")
    val projectProgram = RexProgram.create(
      flinkLogicalOverAgg.getRowType,
      (0 until flinkLogicalOverAgg.getRowType.getFieldCount).flatMap { i =>
        if (i < 7) {
          Array[RexNode](RexInputRef.of(i, flinkLogicalOverAgg.getRowType))
        } else if (i == 7) {
          Array[RexNode](rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE,
            RexInputRef.of(7, flinkLogicalOverAgg.getRowType),
            RexInputRef.of(8, flinkLogicalOverAgg.getRowType)))
        } else {
          Array.empty[RexNode]
        }
      }.toList,
      null,
      rowTypeOfWindowAggOutput,
      rexBuilder
    )
    val streamWindowAggOutput = new StreamPhysicalCalc(
      cluster,
      streamPhysicalTraits,
      windowAgg,
      projectProgram,
      projectProgram.getOutputRowType
    )

    streamWindowAggOutput
  }

  //  row_number() over(partition by class order by name) as rn,
  //  rank() over (partition by class order by score) as rk,
  //  dense_rank() over (partition by class order by score) as drk,
  //  avg(score) over (partition by class order by score) as avg_score,
  //  max(score) over (partition by age) as max_score,
  //  count(id) over (partition by age) as cnt
  private lazy val overAggGroups = {
    ImmutableList.of(
      new Window.Group(
        ImmutableBitSet.of(5),
        true,
        RexWindowBound.create(SqlWindow.createUnboundedPreceding(new SqlParserPos(0, 0)), null),
        RexWindowBound.create(SqlWindow.createCurrentRow(new SqlParserPos(0, 0)), null),
        RelCollationImpl.of(new RelFieldCollation(
          1, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.FIRST)),
        ImmutableList.of(
          new Window.RexWinAggCall(
            SqlStdOperatorTable.ROW_NUMBER,
            longType,
            ImmutableList.of[RexNode](),
            0,
            false
          )
        )
      ),
      new Window.Group(
        ImmutableBitSet.of(5),
        false,
        RexWindowBound.create(SqlWindow.createUnboundedPreceding(new SqlParserPos(4, 15)), null),
        RexWindowBound.create(SqlWindow.createCurrentRow(new SqlParserPos(0, 0)), null),
        RelCollationImpl.of(new RelFieldCollation(
          2, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.FIRST)),
        ImmutableList.of(
          new Window.RexWinAggCall(
            SqlStdOperatorTable.RANK,
            longType,
            ImmutableList.of[RexNode](),
            1,
            false
          ),
          new Window.RexWinAggCall(
            SqlStdOperatorTable.DENSE_RANK,
            longType,
            ImmutableList.of[RexNode](),
            2,
            false
          ),
          new Window.RexWinAggCall(
            SqlStdOperatorTable.COUNT,
            longType,
            util.Arrays.asList(new RexInputRef(2, longType)),
            3,
            false
          ),
          new Window.RexWinAggCall(
            SqlStdOperatorTable.SUM,
            doubleType,
            util.Arrays.asList(new RexInputRef(2, doubleType)),
            4,
            false
          )
        )
      ),
      new Window.Group(
        ImmutableBitSet.of(),
        false,
        RexWindowBound.create(SqlWindow.createUnboundedPreceding(new SqlParserPos(7, 19)), null),
        RexWindowBound.create(SqlWindow.createUnboundedFollowing(new SqlParserPos(0, 0)), null),
        RelCollations.EMPTY,
        ImmutableList.of(
          new Window.RexWinAggCall(
            SqlStdOperatorTable.MAX,
            doubleType,
            util.Arrays.asList(new RexInputRef(2, doubleType)),
            5,
            false
          ),
          new Window.RexWinAggCall(
            SqlStdOperatorTable.COUNT,
            longType,
            util.Arrays.asList(new RexInputRef(0, longType)),
            6,
            false
          )
        )
      )
    )
  }

  protected lazy val flinkLogicalSnapshot: FlinkLogicalSnapshot = {
    val temporalTableRelType = relBuilder.scan("TemporalTable1").build().getRowType
    val correlVar = rexBuilder.makeCorrel(temporalTableRelType, new CorrelationId(0))
    val rowtimeField = rexBuilder.makeFieldAccess(correlVar, 4)
    new FlinkLogicalSnapshot(
      cluster,
      flinkLogicalTraits,
      studentFlinkLogicalScan,
      rowtimeField)
  }

  // SELECT * FROM student AS T JOIN TemporalTable
  // FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id
  protected lazy val (batchLookupJoin, streamLookupJoin) = {
    val temporalTableSource = new TestTemporalTable
    val batchSourceOp = new TableSourceQueryOperation[RowData](temporalTableSource, true)
    val batchScan = relBuilder.queryOperation(batchSourceOp).build().asInstanceOf[TableScan]
    val batchLookupJoin = new BatchPhysicalLookupJoin(
      cluster,
      batchPhysicalTraits,
      studentBatchScan,
      batchScan.getTable,
      None,
      JoinInfo.of(ImmutableIntList.of(0), ImmutableIntList.of(0)),
      JoinRelType.INNER
    )
    val streamSourceOp = new TableSourceQueryOperation[RowData](temporalTableSource, false)
    val streamScan = relBuilder.queryOperation(streamSourceOp).build().asInstanceOf[TableScan]
    val streamLookupJoin = new StreamPhysicalLookupJoin(
      cluster,
      streamPhysicalTraits,
      studentBatchScan,
      streamScan.getTable,
      None,
      JoinInfo.of(ImmutableIntList.of(0), ImmutableIntList.of(0)),
      JoinRelType.INNER
    )
    (batchLookupJoin, streamLookupJoin)
  }

  // select * from MyTable1 join MyTable4 on MyTable1.b = MyTable4.a
  protected lazy val logicalInnerJoinOnUniqueKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable4")
    .join(JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 0)))
    .build

  // select * from MyTable1 join MyTable2 on MyTable1.a = MyTable2.a
  protected lazy val logicalInnerJoinNotOnUniqueKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from MyTable1 join MyTable2 on MyTable1.b = MyTable2.b
  protected lazy val logicalInnerJoinOnLHSUniqueKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))
    .build

  // select * from MyTable2 join MyTable1 on MyTable2.b = MyTable1.b
  protected lazy val logicalInnerJoinOnRHSUniqueKeys: RelNode = relBuilder
    .scan("MyTable2")
    .scan("MyTable1")
    .join(JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))
    .build

  // select * from MyTable1 join MyTable2 on MyTable1.b = MyTable2.b and MyTable1.a > MyTable2.a
  protected lazy val logicalInnerJoinWithEquiAndNonEquiCond: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.INNER, relBuilder.call(AND,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))))
    .build

  // select * from MyTable1 join MyTable2 on MyTable1.a > MyTable2.a
  protected lazy val logicalInnerJoinWithoutEquiCond: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.INNER,
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from MyTable1 join MyTable2 on MyTable1.e = MyTable2.e
  protected lazy val logicalInnerJoinOnDisjointKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 4), relBuilder.field(2, 1, 4)))
    .build

  // select * from MyTable1 left join MyTable4 on MyTable1.b = MyTable4.a
  protected lazy val logicalLeftJoinOnUniqueKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable4")
    .join(JoinRelType.LEFT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 0)))
    .build

  // select * from MyTable1 left join MyTable2 on MyTable1.a = MyTable2.a
  protected lazy val logicalLeftJoinNotOnUniqueKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.LEFT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from MyTable1 left join MyTable2 on MyTable1.b = MyTable2.b
  protected lazy val logicalLeftJoinOnLHSUniqueKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.LEFT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))
    .build

  // select * from MyTable2 left join MyTable1 on MyTable2.b = MyTable1.b
  protected lazy val logicalLeftJoinOnRHSUniqueKeys: RelNode = relBuilder
    .scan("MyTable2")
    .scan("MyTable1")
    .join(JoinRelType.LEFT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))
    .build

  // select * from MyTable1 left join MyTable2 on
  // MyTable1.b = MyTable2.b and MyTable1.a > MyTable2.a
  protected lazy val logicalLeftJoinWithEquiAndNonEquiCond: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.LEFT, relBuilder.call(AND,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))))
    .build

  // select * from MyTable1 left join MyTable2 on MyTable1.a > MyTable2.a
  protected lazy val logicalLeftJoinWithoutEquiCond: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.LEFT,
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from MyTable1 left join MyTable2 on MyTable1.e = MyTable2.e
  protected lazy val logicalLeftJoinOnDisjointKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.LEFT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 4), relBuilder.field(2, 1, 4)))
    .build

  // select * from MyTable1 right join MyTable4 on MyTable1.b = MyTable4.a
  protected lazy val logicalRightJoinOnUniqueKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable4")
    .join(JoinRelType.RIGHT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 0)))
    .build

  // select * from MyTable1 right join MyTable2 on MyTable1.a = MyTable2.a
  protected lazy val logicalRightJoinNotOnUniqueKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.RIGHT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from MyTable1 right join MyTable2 on MyTable1.b = MyTable2.b
  protected lazy val logicalRightJoinOnLHSUniqueKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.RIGHT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))
    .build

  // select * from MyTable2 right join MyTable1 on MyTable2.b = MyTable1.b
  protected lazy val logicalRightJoinOnRHSUniqueKeys: RelNode = relBuilder
    .scan("MyTable2")
    .scan("MyTable1")
    .join(JoinRelType.RIGHT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))
    .build

  // select * from MyTable1 right join MyTable2 on
  // MyTable1.b = MyTable2.b and MyTable1.a > MyTable2.a
  protected lazy val logicalRightJoinWithEquiAndNonEquiCond: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.RIGHT, relBuilder.call(AND,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))))
    .build

  // select * from MyTable1 right join MyTable2 on MyTable1.a > MyTable2.a
  protected lazy val logicalRightJoinWithoutEquiCond: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.RIGHT,
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from MyTable1 right join MyTable2 on MyTable1.e = MyTable2.e
  protected lazy val logicalRightJoinOnDisjointKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.RIGHT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 4), relBuilder.field(2, 1, 4)))
    .build

  // select * from MyTable1 full join MyTable4 on MyTable1.b = MyTable4.a
  protected lazy val logicalFullJoinOnUniqueKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable4")
    .join(JoinRelType.FULL,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 0)))
    .build

  // select * from MyTable1 full join MyTable2 on MyTable1.a = MyTable2.a
  protected lazy val logicalFullJoinNotOnUniqueKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.FULL,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from MyTable1 full join MyTable2 on MyTable1.b = MyTable2.b
  protected lazy val logicalFullJoinOnLHSUniqueKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.FULL,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))
    .build

  // select * from MyTable2 full join MyTable1 on MyTable2.b = MyTable1.b
  protected lazy val logicalFullJoinOnRHSUniqueKeys: RelNode = relBuilder
    .scan("MyTable2")
    .scan("MyTable1")
    .join(JoinRelType.FULL,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))
    .build

  // select * from MyTable1 full join MyTable2 on MyTable1.b = MyTable2.b and MyTable1.a >
  // MyTable2.a
  protected lazy val logicalFullJoinWithEquiAndNonEquiCond: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.FULL, relBuilder.call(AND,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))))
    .build

  // select * from MyTable1 full join MyTable2 on MyTable1.a > MyTable2.a
  protected lazy val logicalFullJoinWithoutEquiCond: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.FULL,
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from MyTable1 full join MyTable2 on MyTable1.e = MyTable2.e
  protected lazy val logicalFullJoinOnDisjointKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.FULL,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 4), relBuilder.field(2, 1, 4)))
    .build

  // select * from MyTable1 full join MyTable2 on true
  protected lazy val logicalFullJoinWithoutCond: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.FULL, relBuilder.literal(true))
    .build

  // select * from MyTable1 b in (select a from MyTable4)
  protected lazy val logicalSemiJoinOnUniqueKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable4")
    .join(JoinRelType.SEMI,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 0)))
    .build()

  // select * from MyTable1 a in (select a from MyTable2)
  protected lazy val logicalSemiJoinNotOnUniqueKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.SEMI,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build()

  // select * from MyTable1 b in (select b from MyTable2)
  protected lazy val logicalSemiJoinOnLHSUniqueKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.SEMI,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))
    .build()

  // select * from MyTable2 a in (select b from MyTable1)
  protected lazy val logicalSemiJoinOnRHSUniqueKeys: RelNode = relBuilder
    .scan("MyTable2")
    .scan("MyTable1")
    .join(JoinRelType.SEMI,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))
    .build()

  // select * from MyTable1 b in (select b from MyTable2 where MyTable1.a > MyTable2.a)
  protected lazy val logicalSemiJoinWithEquiAndNonEquiCond: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.SEMI, relBuilder.call(AND,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))))
    .build

  // select * from MyTable1 exists (select * from MyTable2 where MyTable1.a > MyTable2.a)
  protected lazy val logicalSemiJoinWithoutEquiCond: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.SEMI,
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build()

  // select * from MyTable1 where e in (select e from MyTable2)
  protected lazy val logicalSemiJoinOnDisjointKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.SEMI,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 4), relBuilder.field(2, 1, 4)))
    .build

  // select * from MyTable1 not exists (select * from MyTable4 where MyTable1.b = MyTable4.a)
  protected lazy val logicalAntiJoinOnUniqueKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable4")
    .join(JoinRelType.ANTI,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 0)))
    .build()

  // select * from MyTable1 not exists (select * from MyTable2 where MyTable1.a = MyTable2.a)
  protected lazy val logicalAntiJoinNotOnUniqueKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.ANTI,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build()

  // select * from MyTable1 not exists (select * from MyTable2 where MyTable1.b = MyTable2.b)
  protected lazy val logicalAntiJoinOnLHSUniqueKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.ANTI,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))
    .build()

  // select * from MyTable2 not exists (select * from MyTable1 where MyTable1.b = MyTable2.b)
  protected lazy val logicalAntiJoinOnRHSUniqueKeys: RelNode = relBuilder
    .scan("MyTable2")
    .scan("MyTable1")
    .join(JoinRelType.ANTI,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))
    .build()

  // select * from MyTable1 b not in (select b from MyTable2 where MyTable1.a = MyTable2.a)
  // notes: the nullable of b is true
  protected lazy val logicalAntiJoinWithEquiAndNonEquiCond: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.ANTI, relBuilder.call(AND,
      relBuilder.call(OR,
        relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)),
        relBuilder.isNull(
          relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))),
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))))
    .build

  // select * from MyTable1 b not in (select b from MyTable2)
  // notes: the nullable of b is true
  protected lazy val logicalAntiJoinWithoutEquiCond: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.ANTI, relBuilder.call(OR,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)),
      relBuilder.isNull(
        relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))))
    .build

  // select * from MyTable1 where not exists (select e from MyTable2 where MyTable1.e = MyTable2.e)
  protected lazy val logicalAntiJoinOnDisjointKeys: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.ANTI,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 4), relBuilder.field(2, 1, 4)))
    .build

  // SELECT * FROM MyTable1 UNION ALL SELECT * MyTable2
  protected lazy val logicalUnionAll: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .union(true).build()

  // SELECT * FROM MyTable1 UNION ALL SELECT * MyTable2
  protected lazy val logicalUnion: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .union(false).build()

  // SELECT * FROM MyTable1 INTERSECT ALL SELECT * MyTable2
  protected lazy val logicalIntersectAll: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .intersect(true).build()

  // SELECT * FROM MyTable1 INTERSECT SELECT * MyTable2
  protected lazy val logicalIntersect: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .intersect(false).build()

  // SELECT * FROM MyTable1 MINUS ALL SELECT * MyTable2
  protected lazy val logicalMinusAll: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .minus(true).build()

  // SELECT * FROM MyTable1 MINUS SELECT * MyTable2
  protected lazy val logicalMinus: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .minus(false).build()

  // select * from TableSourceTable1
  // left join TableSourceTable2 on TableSourceTable1.b = TableSourceTable2.b
  protected lazy val logicalLeftJoinOnContainedUniqueKeys: RelNode = relBuilder
    .scan("TableSourceTable1")
    .scan("TableSourceTable2")
    .join(
      JoinRelType.LEFT,
      relBuilder.call(
        EQUALS,
        relBuilder.field(2, 0, 1),
        relBuilder.field(2, 1, 1)
      )
    )
    .build

  // select * from TableSourceTable1
  // left join TableSourceTable2 on TableSourceTable1.a = TableSourceTable2.a
  protected lazy val logicalLeftJoinOnDisjointUniqueKeys: RelNode = relBuilder
    .scan("TableSourceTable1")
    .scan("TableSourceTable2")
    .join(
      JoinRelType.LEFT,
      relBuilder.call(
        EQUALS,
        relBuilder.field(2, 0, 0),
        relBuilder.field(2, 1, 0)
      )
    )
    .build

  // select * from TableSourceTable1
  // left join TableSourceTable3 on TableSourceTable1.a = TableSourceTable3.a
  protected lazy val logicalLeftJoinWithNoneKeyTableUniqueKeys: RelNode = relBuilder
    .scan("TableSourceTable1")
    .scan("TableSourceTable3")
    .join(
      JoinRelType.LEFT,
      relBuilder.call(
        EQUALS,
        relBuilder.field(2, 0, 0),
        relBuilder.field(2, 1, 0)
      )
    )
    .build

  protected def createDataStreamScan[T](
      tableNames: util.List[String], traitSet: RelTraitSet): T = {
    val table = relBuilder
      .getRelOptSchema
      .asInstanceOf[CalciteCatalogReader]
      .getTable(tableNames)
      .asInstanceOf[FlinkPreparingTableBase]
    val conventionTrait = traitSet.getTrait(ConventionTraitDef.INSTANCE)
    val scan = conventionTrait match {
      case Convention.NONE =>
        relBuilder.clear()
        val scan = relBuilder.scan(tableNames).build()
        scan.copy(traitSet, scan.getInputs)
      case FlinkConventions.LOGICAL =>
        new FlinkLogicalDataStreamTableScan(
          cluster, traitSet, Collections.emptyList[RelHint](), table)
      case FlinkConventions.BATCH_PHYSICAL =>
        new BatchPhysicalBoundedStreamScan(
          cluster, traitSet, Collections.emptyList[RelHint](), table, table.getRowType)
      case FlinkConventions.STREAM_PHYSICAL =>
        new StreamPhysicalDataStreamScan(
          cluster, traitSet, Collections.emptyList[RelHint](), table, table.getRowType)
      case _ => throw new TableException(s"Unsupported convention trait: $conventionTrait")
    }
    scan.asInstanceOf[T]
  }

  protected def createTableSourceTable[T](
      tableNames: util.List[String], traitSet: RelTraitSet): T = {
    val table = relBuilder
      .getRelOptSchema
      .asInstanceOf[CalciteCatalogReader]
      .getTable(tableNames)
      .asInstanceOf[TableSourceTable]
    val conventionTrait = traitSet.getTrait(ConventionTraitDef.INSTANCE)
    val scan = conventionTrait match {
      case Convention.NONE =>
        relBuilder.clear()
        val scan = relBuilder.scan(tableNames).build()
        scan.copy(traitSet, scan.getInputs)
      case FlinkConventions.LOGICAL =>
        new FlinkLogicalDataStreamTableScan(
          cluster, traitSet, Collections.emptyList[RelHint](), table)
      case FlinkConventions.BATCH_PHYSICAL =>
        new BatchPhysicalBoundedStreamScan(
          cluster, traitSet, Collections.emptyList[RelHint](), table, table.getRowType)
      case FlinkConventions.STREAM_PHYSICAL =>
        new StreamPhysicalDataStreamScan(
          cluster, traitSet, Collections.emptyList[RelHint](), table, table.getRowType)
      case _ => throw new TableException(s"Unsupported convention trait: $conventionTrait")
    }
    scan.asInstanceOf[T]
  }

  protected def createLiteralList(
      rowType: RelDataType,
      literalValues: Seq[String]): util.List[RexLiteral] = {
    require(literalValues.length == rowType.getFieldCount)
    val rexBuilder = relBuilder.getRexBuilder
    literalValues.zipWithIndex.map {
      case (v, index) =>
        val fieldType = rowType.getFieldList.get(index).getType
        if (v == null) {
          rexBuilder.makeNullLiteral(fieldType)
        } else {
          fieldType.getSqlTypeName match {
            case BIGINT => rexBuilder.makeLiteral(v.toLong, fieldType, true)
            case INTEGER => rexBuilder.makeLiteral(v.toInt, fieldType, true)
            case BOOLEAN => rexBuilder.makeLiteral(v.toBoolean)
            case DATE => rexBuilder.makeDateLiteral(new DateString(v))
            case TIME => rexBuilder.makeTimeLiteral(new TimeString(v), 0)
            case TIMESTAMP => rexBuilder.makeTimestampLiteral(new TimestampString(v), 0)
            case DOUBLE => rexBuilder.makeApproxLiteral(BigDecimal.valueOf(v.toDouble))
            case FLOAT => rexBuilder.makeApproxLiteral(BigDecimal.valueOf(v.toFloat))
            case VARCHAR => rexBuilder.makeLiteral(v)
            case _ => throw new TableException(s"${fieldType.getSqlTypeName} is not supported!")
          }
        }.asInstanceOf[RexLiteral]
    }.toList
  }

  protected def createLogicalCalc(
      input: RelNode,
      outputRowType: RelDataType,
      projects: util.List[RexNode],
      conditions: util.List[RexNode]): Calc = {
    val predicate: RexNode = if (conditions == null || conditions.isEmpty) {
      null
    } else {
      RexUtil.composeConjunction(rexBuilder, conditions, true)
    }
    val program = RexProgram.create(
      input.getRowType,
      projects,
      predicate,
      outputRowType,
      rexBuilder)
    FlinkLogicalCalc.create(input, program)
  }

  protected def makeLiteral(
      value: Any,
      internalType: LogicalType,
      isNullable: Boolean = false,
      allowCast: Boolean = true): RexNode = {
    rexBuilder.makeLiteral(
      value,
      typeFactory.createFieldTypeFromLogicalType(internalType.copy(isNullable)),
      allowCast
    )
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
    RelMetadataQueryBase
      .THREAD_PROVIDERS
      .set(JaninoRelMetadataProvider.of(FlinkDefaultRelMetadataProvider.INSTANCE))
  }
}
