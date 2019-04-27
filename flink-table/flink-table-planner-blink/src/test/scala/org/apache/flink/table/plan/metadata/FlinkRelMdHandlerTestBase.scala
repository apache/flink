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

import org.apache.flink.table.`type`.{InternalType, InternalTypes}
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.calcite.{FlinkCalciteCatalogReader, FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.functions.aggfunctions.SumAggFunction.DoubleSumAggFunction
import org.apache.flink.table.functions.aggfunctions.{DenseRankAggFunction, RankAggFunction, RowNumberAggFunction}
import org.apache.flink.table.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.plan.PartialFinalType
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.calcite.{LogicalExpand, LogicalRank}
import org.apache.flink.table.plan.nodes.logical._
import org.apache.flink.table.plan.nodes.physical.batch._
import org.apache.flink.table.plan.nodes.physical.stream._
import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.table.plan.util.AggregateUtil.transformToStreamAggregateInfoList
import org.apache.flink.table.plan.util.{AggFunctionFactory, AggregateUtil, ExpandUtil, FlinkRelOptUtil, SortUtil}
import org.apache.flink.table.runtime.rank.{ConstantRankRange, RankType, VariableRankRange}
import org.apache.flink.table.util.CountAggFunction

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.{Convention, ConventionTraitDef, RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFieldImpl}
import org.apache.calcite.rel.core.{AggregateCall, Calc, JoinRelType, Window}
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalProject, LogicalSort, LogicalTableScan, LogicalValues}
import org.apache.calcite.rel.metadata.{JaninoRelMetadataProvider, RelMetadataQuery}
import org.apache.calcite.rel.{RelCollationImpl, RelCollationTraitDef, RelCollations, RelFieldCollation, RelNode, SingleRel}
import org.apache.calcite.rex.{RexBuilder, RexInputRef, RexLiteral, RexNode, RexProgram, RexUtil, RexWindowBound}
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.SqlWindow
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName.{BIGINT, BOOLEAN, DATE, DOUBLE, FLOAT, TIME, TIMESTAMP, VARCHAR}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{AND, CASE, DIVIDE, EQUALS, GREATER_THAN, LESS_THAN, MINUS, MULTIPLY, PLUS}
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.tools.FrameworkConfig
import org.apache.calcite.util.{DateString, ImmutableBitSet, TimeString, TimestampString}
import org.junit.{Before, BeforeClass}

import java.math.BigDecimal
import java.util

import scala.collection.JavaConversions._

class FlinkRelMdHandlerTestBase {

  val tableConfig = new TableConfig()
  val rootSchema: SchemaPlus = MetadataTestUtil.initRootSchema()
  val frameworkConfig: FrameworkConfig =
    MetadataTestUtil.createFrameworkConfig(rootSchema, tableConfig)
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
    relBuilder = FlinkRelBuilder.create(frameworkConfig)

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

  protected val intType: RelDataType = typeFactory.createTypeFromInternalType(
    InternalTypes.INT, isNullable = false)

  protected val doubleType: RelDataType = typeFactory.createTypeFromInternalType(
    InternalTypes.DOUBLE, isNullable = false)

  protected val longType: RelDataType = typeFactory.createTypeFromInternalType(
    InternalTypes.LONG, isNullable = false)

  protected val stringType: RelDataType = typeFactory.createTypeFromInternalType(
    InternalTypes.STRING, isNullable = false)

  protected lazy val testRel = new TestRel(
    cluster, logicalTraits, createDataStreamScan(ImmutableList.of("student"), logicalTraits))

  protected lazy val studentLogicalScan: LogicalTableScan =
    createDataStreamScan(ImmutableList.of("student"), logicalTraits)
  protected lazy val studentFlinkLogicalScan: FlinkLogicalDataStreamTableScan =
    createDataStreamScan(ImmutableList.of("student"), flinkLogicalTraits)
  protected lazy val studentBatchScan: BatchExecBoundedStreamScan =
    createDataStreamScan(ImmutableList.of("student"), batchPhysicalTraits)
  protected lazy val studentStreamScan: StreamExecDataStreamScan =
    createDataStreamScan(ImmutableList.of("student"), streamPhysicalTraits)

  protected lazy val empLogicalScan: LogicalTableScan =
    createDataStreamScan(ImmutableList.of("emp"), logicalTraits)
  protected lazy val empFlinkLogicalScan: FlinkLogicalDataStreamTableScan =
    createDataStreamScan(ImmutableList.of("emp"), flinkLogicalTraits)
  protected lazy val empBatchScan: BatchExecBoundedStreamScan =
    createDataStreamScan(ImmutableList.of("emp"), batchPhysicalTraits)
  protected lazy val empStreamScan: StreamExecDataStreamScan =
    createDataStreamScan(ImmutableList.of("emp"), streamPhysicalTraits)

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

  // id, name, score, age, height, sex, class, 1
  // id, null, score, age, height, sex, class, 4
  // id, null, score, age, height, null, class, 5
  protected lazy val (logicalExpand, flinkLogicalExpand, batchExpand, streamExpand) = {
    val cluster = studentLogicalScan.getCluster
    val expandOutputType = ExpandUtil.buildExpandRowType(
      cluster.getTypeFactory, studentLogicalScan.getRowType, Array.empty[Integer])
    val expandProjects = ExpandUtil.createExpandProjects(
      studentLogicalScan.getCluster.getRexBuilder,
      studentLogicalScan.getRowType,
      expandOutputType,
      ImmutableBitSet.of(1, 3, 5),
      ImmutableList.of(
        ImmutableBitSet.of(1, 3, 5),
        ImmutableBitSet.of(3, 5),
        ImmutableBitSet.of(3)),
      Array.empty[Integer])
    val logicalExpand = new LogicalExpand(cluster, studentLogicalScan.getTraitSet,
      studentLogicalScan, expandOutputType, expandProjects, 7)

    val flinkLogicalExpand = new FlinkLogicalExpand(cluster, flinkLogicalTraits,
      studentFlinkLogicalScan, expandOutputType, expandProjects, 7)

    val batchExpand = new BatchExecExpand(cluster, batchPhysicalTraits,
      studentBatchScan, expandOutputType, expandProjects, 7)

    val streamExecExpand = new StreamExecExpand(cluster, streamPhysicalTraits,
      studentStreamScan, expandOutputType, expandProjects, 7)

    (logicalExpand, flinkLogicalExpand, batchExpand, streamExecExpand)
  }

  // hash exchange on class
  protected lazy val (batchExchange, streamExchange) = {
    val hash6 = FlinkRelDistribution.hash(Array(6), requireStrict = true)
    val batchExchange = new BatchExecExchange(
      cluster,
      batchPhysicalTraits.replace(hash6),
      studentBatchScan,
      hash6
    )
    val streamExchange = new StreamExecExchange(
      cluster,
      streamPhysicalTraits.replace(hash6),
      studentStreamScan,
      hash6
    )
    (batchExchange, streamExchange)
  }

  // equivalent SQL is
  // select * from student order by class asc, score desc
  protected lazy val (logicalSort, flinkLogicalSort, batchSort, streamSort) = {
    val logicalSort = relBuilder.scan("student").sort(
      relBuilder.field("class"),
      relBuilder.desc(relBuilder.field("score")))
      .build.asInstanceOf[LogicalSort]
    val collation = logicalSort.getCollation
    val flinkLogicalSort = new FlinkLogicalSort(cluster, flinkLogicalTraits.replace(collation),
      studentFlinkLogicalScan, collation, null, null)
    val batchSort = new BatchExecSort(cluster,
      batchPhysicalTraits.replace(collation).replace(FlinkRelDistribution.SINGLETON),
      studentBatchScan, collation)
    val streamSort = new StreamExecSort(cluster,
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

    val batchSort = new BatchExecLimit(cluster, batchPhysicalTraits.replace(collation),
      new BatchExecExchange(
        cluster, batchPhysicalTraits.replace(FlinkRelDistribution.SINGLETON), studentBatchScan,
        FlinkRelDistribution.SINGLETON),
      logicalSort.offset, logicalSort.fetch, true)

    val batchSortLocal = new BatchExecLimit(cluster, batchPhysicalTraits.replace(collation),
      studentBatchScan,
      relBuilder.literal(0),
      relBuilder.literal(SortUtil.getLimitEnd(logicalSort.offset, logicalSort.fetch)),
      false)
    val batchSortGlobal = new BatchExecLimit(cluster, batchPhysicalTraits.replace(collation),
      new BatchExecExchange(
        cluster, batchPhysicalTraits.replace(FlinkRelDistribution.SINGLETON), batchSortLocal,
        FlinkRelDistribution.SINGLETON),
      logicalSort.offset, logicalSort.fetch, true)

    val streamSort = new StreamExecLimit(cluster, streamPhysicalTraits.replace(collation),
      studentStreamScan, logicalSort.offset, logicalSort.fetch)

    (logicalSort, flinkLogicalSort, batchSort, batchSortLocal, batchSortGlobal, streamSort)
  }

  // equivalent SQL is
  // select * from student order by class asc, score desc limit 20 offset 10
  protected lazy val (
    logicalSortLimit,
    flinkLogicalSortLimit,
    batchSortLimit,
    batchLocalSortLimit,
    batchGlobalSortLimit,
    streamSortLimit) = {
    val logicalSortLimit = relBuilder.scan("student").sort(
      relBuilder.field("class"),
      relBuilder.desc(relBuilder.field("score")))
      .limit(10, 20).build.asInstanceOf[LogicalSort]

    val collection = logicalSortLimit.collation
    val offset = logicalSortLimit.offset
    val fetch = logicalSortLimit.fetch

    val flinkLogicalSortLimit = new FlinkLogicalSort(cluster,
      flinkLogicalTraits.replace(collection), studentFlinkLogicalScan, collection, offset, fetch)

    val batchSortLimit = new BatchExecSortLimit(cluster, batchPhysicalTraits.replace(collection),
      new BatchExecExchange(
        cluster, batchPhysicalTraits.replace(FlinkRelDistribution.SINGLETON), studentBatchScan,
        FlinkRelDistribution.SINGLETON),
      collection, offset, fetch, true)

    val batchSortLocalLimit = new BatchExecSortLimit(cluster,
      batchPhysicalTraits.replace(collection), studentBatchScan, collection,
      relBuilder.literal(0),
      relBuilder.literal(SortUtil.getLimitEnd(offset, fetch)),
      false)
    val batchSortGlobal = new BatchExecSortLimit(cluster, batchPhysicalTraits.replace(collection),
      new BatchExecExchange(
        cluster, batchPhysicalTraits.replace(FlinkRelDistribution.SINGLETON), batchSortLocalLimit,
        FlinkRelDistribution.SINGLETON),
      collection, offset, fetch, true)

    val streamSort = new StreamExecSortLimit(cluster, streamPhysicalTraits.replace(collection),
      studentStreamScan, collection, offset, fetch)

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
    streamRank) = {
    val logicalRank = new LogicalRank(
      cluster,
      logicalTraits,
      studentLogicalScan,
      ImmutableBitSet.of(6),
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
      ImmutableBitSet.of(6),
      RelCollations.of(2),
      RankType.RANK,
      new ConstantRankRange(1, 5),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = true
    )

    val batchLocalRank = new BatchExecRank(
      cluster,
      batchPhysicalTraits,
      studentBatchScan,
      ImmutableBitSet.of(6),
      RelCollations.of(2),
      RankType.RANK,
      new ConstantRankRange(1, 5),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = false,
      isGlobal = false
    )

    val hash6 = FlinkRelDistribution.hash(Array(6), requireStrict = true)
    val batchExchange = new BatchExecExchange(
      cluster, batchLocalRank.getTraitSet.replace(hash6), batchLocalRank, hash6)
    val batchGlobalRank = new BatchExecRank(
      cluster,
      batchPhysicalTraits,
      batchExchange,
      ImmutableBitSet.of(6),
      RelCollations.of(2),
      RankType.RANK,
      new ConstantRankRange(1, 5),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = true,
      isGlobal = true
    )

    val streamExchange = new BatchExecExchange(cluster,
      studentStreamScan.getTraitSet.replace(hash6), studentStreamScan, hash6)
    val streamRank = new StreamExecRank(
      cluster,
      streamPhysicalTraits,
      streamExchange,
      ImmutableBitSet.of(6),
      RelCollations.of(2),
      RankType.RANK,
      new ConstantRankRange(1, 5),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = true
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

    val batchLocalRank = new BatchExecRank(
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
    val batchExchange = new BatchExecExchange(
      cluster, batchLocalRank.getTraitSet.replace(hash6), batchLocalRank, hash6)
    val batchGlobalRank = new BatchExecRank(
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

    val streamExchange = new BatchExecExchange(cluster,
      studentStreamScan.getTraitSet.replace(hash6), studentStreamScan, hash6)
    val streamRank = new StreamExecRank(
      cluster,
      streamPhysicalTraits,
      streamExchange,
      ImmutableBitSet.of(3),
      RelCollations.of(2),
      RankType.RANK,
      new ConstantRankRange(3, 5),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = true
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
    val streamExchange = new BatchExecExchange(cluster,
      studentStreamScan.getTraitSet.replace(singleton), studentStreamScan, singleton)
    val streamRowNumber = new StreamExecRank(
      cluster,
      streamPhysicalTraits,
      streamExchange,
      ImmutableBitSet.of(),
      RelCollations.of(4),
      RankType.ROW_NUMBER,
      new ConstantRankRange(3, 6),
      new RelDataTypeFieldImpl("rn", 7, longType),
      outputRankNumber = true
    )

    (logicalRowNumber, flinkLogicalRowNumber, streamRowNumber)
  }

  // equivalent SQL is
  // select a, b, c from (
  //  select a, b, c, proctime
  //  ROW_NUMBER() over (partition by b order by proctime) rn from TemporalTable
  // ) t where rn <= 1
  //
  // select a, b, c from (
  //  select a, b, c, proctime
  //  ROW_NUMBER() over (partition by b, c order by proctime desc) rn from TemporalTable
  // ) t where rn <= 1
  protected lazy val (streamDeduplicateFirstRow, streamDeduplicateLastRow) = {
    val scan: StreamExecDataStreamScan =
      createDataStreamScan(ImmutableList.of("TemporalTable"), streamPhysicalTraits)
    val hash1 = FlinkRelDistribution.hash(Array(1), requireStrict = true)
    val streamExchange1 = new StreamExecExchange(
      cluster, scan.getTraitSet.replace(hash1), scan, hash1)
    val firstRow = new StreamExecDeduplicate(
      cluster,
      streamPhysicalTraits,
      streamExchange1,
      Array(1),
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
    val calcOfFirstRow = new StreamExecCalc(
      cluster,
      streamPhysicalTraits,
      firstRow,
      projectProgram,
      projectProgram.getOutputRowType
    )

    val hash12 = FlinkRelDistribution.hash(Array(1, 2), requireStrict = true)
    val streamExchange2 = new BatchExecExchange(cluster,
      scan.getTraitSet.replace(hash12), scan, hash12)
    val lastRow = new StreamExecDeduplicate(
      cluster,
      streamPhysicalTraits,
      streamExchange2,
      Array(1, 2),
      keepLastRow = true
    )
    val calcOfLastRow = new StreamExecCalc(
      cluster,
      streamPhysicalTraits,
      lastRow,
      projectProgram,
      projectProgram.getOutputRowType
    )

    (calcOfFirstRow, calcOfLastRow)
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

    val streamRankWithVariableRange = new StreamExecRank(
      cluster,
      logicalTraits,
      studentStreamScan,
      ImmutableBitSet.of(6),
      RelCollations.of(2),
      RankType.RANK,
      new VariableRankRange(3),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = true
    )

    (logicalRankWithVariableRange, flinkLogicalRankWithVariableRange, streamRankWithVariableRange)
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
      logicalAgg.indicator,
      logicalAgg.getGroupSet,
      logicalAgg.getGroupSets,
      logicalAgg.getAggCallList
    )

    val aggCalls = logicalAgg.getAggCallList
    val aggFunctionFactory = new AggFunctionFactory(
      studentBatchScan.getRowType, Array.empty[Int], Array.fill(aggCalls.size())(false))
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

    val batchLocalAgg = new BatchExecLocalHashAggregate(
      cluster,
      relBuilder,
      batchPhysicalTraits,
      studentBatchScan,
      rowTypeOfLocalAgg,
      studentBatchScan.getRowType,
      Array(3),
      auxGrouping = Array(),
      aggCallToAggFunction)

    val batchExchange1 = new BatchExecExchange(
      cluster, batchLocalAgg.getTraitSet.replace(hash0), batchLocalAgg, hash0)
    val batchGlobalAgg = new BatchExecHashAggregate(
      cluster,
      relBuilder,
      batchPhysicalTraits,
      batchExchange1,
      rowTypeOfGlobalAgg,
      batchExchange1.getRowType,
      batchLocalAgg.getInput.getRowType,
      Array(0),
      auxGrouping = Array(),
      aggCallToAggFunction,
      isMerge = true)

    val batchExchange2 = new BatchExecExchange(cluster,
      studentBatchScan.getTraitSet.replace(hash3), studentBatchScan, hash3)
    val batchGlobalAggWithoutLocal = new BatchExecHashAggregate(
      cluster,
      relBuilder,
      batchPhysicalTraits,
      batchExchange2,
      rowTypeOfGlobalAgg,
      batchExchange2.getRowType,
      batchExchange2.getRowType,
      Array(3),
      auxGrouping = Array(),
      aggCallToAggFunction,
      isMerge = false)

    val needRetractionArray = AggregateUtil.getNeedRetractions(
      1, needRetraction = false, null, aggCalls)

    val localAggInfoList = transformToStreamAggregateInfoList(
      aggCalls,
      studentStreamScan.getRowType,
      needRetractionArray,
      needInputCount = false,
      isStateBackendDataViews = false)
    val streamLocalAgg = new StreamExecLocalGroupAggregate(
      cluster,
      streamPhysicalTraits,
      studentStreamScan,
      rowTypeOfLocalAgg,
      Array(3),
      aggCalls,
      localAggInfoList,
      PartialFinalType.NONE)

    val streamExchange1 = new StreamExecExchange(
      cluster, streamLocalAgg.getTraitSet.replace(hash0), streamLocalAgg, hash0)
    val globalAggInfoList = transformToStreamAggregateInfoList(
      aggCalls,
      streamExchange1.getRowType,
      needRetractionArray,
      needInputCount = false,
      isStateBackendDataViews = true)
    val streamGlobalAgg = new StreamExecGlobalGroupAggregate(
      cluster,
      streamPhysicalTraits,
      streamExchange1,
      streamExchange1.getRowType,
      rowTypeOfGlobalAgg,
      Array(0),
      localAggInfoList,
      globalAggInfoList,
      PartialFinalType.NONE)

    val streamExchange2 = new StreamExecExchange(cluster,
      studentStreamScan.getTraitSet.replace(hash3), studentStreamScan, hash3)
    val streamGlobalAggWithoutLocal = new StreamExecGroupAggregate(
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
      logicalAggWithAuxGroup.indicator,
      logicalAggWithAuxGroup.getGroupSet,
      logicalAggWithAuxGroup.getGroupSets,
      logicalAggWithAuxGroup.getAggCallList
    )

    val aggCalls = logicalAggWithAuxGroup.getAggCallList.filter {
      call => call.getAggregation != FlinkSqlOperatorTable.AUXILIARY_GROUP
    }
    val aggFunctionFactory = new AggFunctionFactory(
      studentBatchScan.getRowType, Array.empty[Int], Array.fill(aggCalls.size())(false))
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

    val batchLocalAggWithAuxGroup = new BatchExecLocalHashAggregate(
      cluster,
      relBuilder,
      batchPhysicalTraits,
      studentBatchScan,
      rowTypeOfLocalAgg,
      studentBatchScan.getRowType,
      Array(0),
      auxGrouping = Array(1, 4),
      aggCallToAggFunction)

    val hash0 = FlinkRelDistribution.hash(Array(0), requireStrict = true)
    val batchExchange = new BatchExecExchange(cluster,
      batchLocalAggWithAuxGroup.getTraitSet.replace(hash0), batchLocalAggWithAuxGroup, hash0)

    val rowTypeOfGlobalAgg = typeFactory.builder
      .add("id", intType)
      .add("name", stringType)
      .add("height", doubleType)
      .add("avg_score", doubleType)
      .add("sum_score", doubleType)
      .add("cnt", longType).build()
    val batchGlobalAggWithAuxGroup = new BatchExecHashAggregate(
      cluster,
      relBuilder,
      batchPhysicalTraits,
      batchExchange,
      rowTypeOfGlobalAgg,
      batchExchange.getRowType,
      batchLocalAggWithAuxGroup.getInput.getRowType,
      Array(0),
      auxGrouping = Array(1, 2),
      aggCallToAggFunction,
      isMerge = true)

    val batchExchange2 = new BatchExecExchange(cluster,
      studentBatchScan.getTraitSet.replace(hash0), studentBatchScan, hash0)
    val batchGlobalAggWithoutLocalWithAuxGroup = new BatchExecHashAggregate(
      cluster,
      relBuilder,
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

  // equivalent SQL is
  // select id, name, score, age, class,
  //  row_number() over(partition by class order by name) as rn,
  //  rank() over (partition by class order by score) as rk,
  //  dense_rank() over (partition by class order by score) as drk,
  //  avg(score) over (partition by class order by score) as avg_score,
  //  max(score) over (partition by age) as max_score,
  //  count(id) over (partition by age) as cnt
  //  from student
  protected lazy val (flinkLogicalOverWindow, batchOverWindowAgg) = {
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
    val flinkLogicalOverWindow = new FlinkLogicalOverWindow(
      cluster,
      flinkLogicalTraits,
      new FlinkLogicalCalc(cluster, flinkLogicalTraits, studentFlinkLogicalScan, rexProgram),
      ImmutableList.of(),
      rowTypeOfWindowAgg,
      overWindowGroups
    )

    val rowTypeOfWindowAggOutput = createRowType(
      "id", "name", "score", "age", "class", "rn", "rk", "drk", "avg_score", "max_score", "cnt")
    val projectProgram = RexProgram.create(
      flinkLogicalOverWindow.getRowType,
      (0 until flinkLogicalOverWindow.getRowType.getFieldCount).flatMap { i =>
        if (i < 8 || i >= 10) {
          Array[RexNode](RexInputRef.of(i, flinkLogicalOverWindow.getRowType))
        } else if (i == 8) {
          Array[RexNode](rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE,
            RexInputRef.of(8, flinkLogicalOverWindow.getRowType),
            RexInputRef.of(9, flinkLogicalOverWindow.getRowType)))
        } else {
          Array.empty[RexNode]
        }
      }.toList,
      null,
      rowTypeOfWindowAggOutput,
      rexBuilder
    )

    val flinkLogicalOverWindowOutput = new FlinkLogicalCalc(
      cluster,
      flinkLogicalTraits,
      flinkLogicalOverWindow,
      projectProgram
    )

    val calc = new BatchExecCalc(
      cluster, batchPhysicalTraits, studentBatchScan, rexProgram, rowTypeOfCalc)
    val hash4 = FlinkRelDistribution.hash(Array(4), requireStrict = true)
    val exchange1 = new BatchExecExchange(cluster, calc.getTraitSet.replace(hash4), calc, hash4)
    // sort class, name
    val collection1 = RelCollations.of(
      FlinkRelOptUtil.ofRelFieldCollation(4), FlinkRelOptUtil.ofRelFieldCollation(1))
    val newSortTrait1 = exchange1.getTraitSet.replace(collection1)
    val sort1 = new BatchExecSort(cluster, newSortTrait1, exchange1,
      newSortTrait1.getTrait(RelCollationTraitDef.INSTANCE))

    val outputRowType1 = createRowType("id", "name", "score", "age", "class", "rn")
    val innerWindowAgg1 = new BatchExecOverAggregate(
      cluster,
      relBuilder,
      batchPhysicalTraits,
      sort1,
      outputRowType1,
      sort1.getRowType,
      Array(4),
      Array(1),
      Array(true),
      Array(false),
      Seq((overWindowGroups(0), Seq(
        (AggregateCall.create(SqlStdOperatorTable.ROW_NUMBER, false, ImmutableList.of(), -1,
          longType, "rn"),
          new RowNumberAggFunction())))),
      flinkLogicalOverWindow
    )

    // sort class, score
    val collation2 = RelCollations.of(
      FlinkRelOptUtil.ofRelFieldCollation(4), FlinkRelOptUtil.ofRelFieldCollation(2))
    val newSortTrait2 = innerWindowAgg1.getTraitSet.replace(collation2)
    val sort2 = new BatchExecSort(cluster, newSortTrait2, innerWindowAgg1,
      newSortTrait2.getTrait(RelCollationTraitDef.INSTANCE))

    val outputRowType2 = createRowType(
      "id", "name", "score", "age", "class", "rn", "rk", "drk", "count$0_score", "sum$0_score")
    val innerWindowAgg2 = new BatchExecOverAggregate(
      cluster,
      relBuilder,
      batchPhysicalTraits,
      sort2,
      outputRowType2,
      sort2.getRowType,
      Array(4),
      Array(2),
      Array(true),
      Array(false),
      Seq((overWindowGroups(1), Seq(
        (AggregateCall.create(SqlStdOperatorTable.RANK, false, ImmutableList.of(), -1, longType,
          "rk"),
          new RankAggFunction(Array(InternalTypes.STRING))),
        (AggregateCall.create(SqlStdOperatorTable.DENSE_RANK, false, ImmutableList.of(), -1,
          longType, "drk"),
          new DenseRankAggFunction(Array(InternalTypes.STRING))),
        (AggregateCall.create(SqlStdOperatorTable.COUNT, false,
          ImmutableList.of(Integer.valueOf(2)), -1, longType, "count$0_socre"),
          new CountAggFunction()),
        (AggregateCall.create(SqlStdOperatorTable.SUM, false,
          ImmutableList.of(Integer.valueOf(2)), -1, doubleType, "sum$0_score"),
          new DoubleSumAggFunction())
      ))),
      flinkLogicalOverWindow
    )

    val hash3 = FlinkRelDistribution.hash(Array(3), requireStrict = true)
    val exchange2 = new BatchExecExchange(
      cluster, innerWindowAgg2.getTraitSet.replace(hash3), innerWindowAgg2, hash3)

    val outputRowType3 = createRowType(
      "id", "name", "score", "age", "class", "rn", "rk", "drk",
      "count$0_score", "sum$0_score", "max_score", "cnt")
    val batchWindowAgg = new BatchExecOverAggregate(
      cluster,
      relBuilder,
      batchPhysicalTraits,
      exchange2,
      outputRowType3,
      exchange2.getRowType,
      Array(3),
      Array.empty,
      Array.empty,
      Array.empty,
      Seq((overWindowGroups(2), Seq(
        (AggregateCall.create(SqlStdOperatorTable.MAX, false,
          ImmutableList.of(Integer.valueOf(2)), -1, longType, "max_score"),
          new CountAggFunction()),
        (AggregateCall.create(SqlStdOperatorTable.COUNT, false,
          ImmutableList.of(Integer.valueOf(0)), -1, doubleType, "cnt"),
          new DoubleSumAggFunction())
      ))),
      flinkLogicalOverWindow
    )

    val batchWindowAggOutput = new BatchExecCalc(
      cluster,
      batchPhysicalTraits,
      batchWindowAgg,
      projectProgram,
      projectProgram.getOutputRowType
    )

    (flinkLogicalOverWindowOutput, batchWindowAggOutput)
  }

  // equivalent SQL is
  // select id, name, score, age, class,
  //  rank() over (partition by class order by score) as rk,
  //  dense_rank() over (partition by class order by score) as drk,
  //  avg(score) over (partition by class order by score) as avg_score
  //  from student
  protected lazy val streamOverWindowAgg: StreamPhysicalRel = {
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
    val flinkLogicalOverWindow = new FlinkLogicalOverWindow(
      cluster,
      flinkLogicalTraits,
      new FlinkLogicalCalc(cluster, flinkLogicalTraits, studentFlinkLogicalScan, rexProgram),
      ImmutableList.of(),
      rowTypeOfWindowAgg,
      util.Arrays.asList(overWindowGroups.get(1))
    )

    val streamScan: StreamExecDataStreamScan =
      createDataStreamScan(ImmutableList.of("student"), streamPhysicalTraits)
    val calc = new StreamExecCalc(
      cluster, streamPhysicalTraits, streamScan, rexProgram, rowTypeOfCalc)
    val hash4 = FlinkRelDistribution.hash(Array(4), requireStrict = true)
    val exchange = new StreamExecExchange(cluster, calc.getTraitSet.replace(hash4), calc, hash4)

    val windowAgg = new StreamExecOverAggregate(
      cluster,
      streamPhysicalTraits,
      exchange,
      rowTypeOfWindowAgg,
      exchange.getRowType,
      flinkLogicalOverWindow
    )

    val rowTypeOfWindowAggOutput = createRowType(
      "id", "name", "score", "age", "class", "rk", "drk", "avg_score")
    val projectProgram = RexProgram.create(
      flinkLogicalOverWindow.getRowType,
      (0 until flinkLogicalOverWindow.getRowType.getFieldCount).flatMap { i =>
        if (i < 7) {
          Array[RexNode](RexInputRef.of(i, flinkLogicalOverWindow.getRowType))
        } else if (i == 7) {
          Array[RexNode](rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE,
            RexInputRef.of(7, flinkLogicalOverWindow.getRowType),
            RexInputRef.of(8, flinkLogicalOverWindow.getRowType)))
        } else {
          Array.empty[RexNode]
        }
      }.toList,
      null,
      rowTypeOfWindowAggOutput,
      rexBuilder
    )
    val streamWindowAggOutput = new StreamExecCalc(
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
  private lazy val overWindowGroups = {
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
  protected lazy val logicalFullWithoutCond: RelNode = relBuilder
    .scan("MyTable1")
    .scan("MyTable2")
    .join(JoinRelType.FULL, relBuilder.literal(true))
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

  protected def createDataStreamScan[T](
      tableNames: util.List[String], traitSet: RelTraitSet): T = {
    val table = catalogReader.getTable(tableNames).asInstanceOf[FlinkRelOptTable]
    val conventionTrait = traitSet.getTrait(ConventionTraitDef.INSTANCE)
    val scan = conventionTrait match {
      case Convention.NONE =>
        relBuilder.clear()
        val scan = relBuilder.scan(tableNames).build()
        scan.copy(traitSet, scan.getInputs)
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

  private def createLiteralList(
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
      internalType: InternalType,
      isNullable: Boolean = false,
      allowCast: Boolean = true): RexNode = {
    rexBuilder.makeLiteral(
      value,
      typeFactory.createTypeFromInternalType(internalType, isNullable),
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
    RelMetadataQuery
      .THREAD_PROVIDERS
      .set(JaninoRelMetadataProvider.of(FlinkDefaultRelMetadataProvider.INSTANCE))
  }
}
