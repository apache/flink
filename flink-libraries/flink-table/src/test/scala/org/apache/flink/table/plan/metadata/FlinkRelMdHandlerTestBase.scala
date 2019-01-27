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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, SqlTimeTypeInfo}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.{FlinkCalciteCatalogReader, FlinkRelBuilder, FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.codegen.expr
import org.apache.flink.table.expressions.{ProctimeAttribute, RowtimeAttribute, WindowReference, WindowStart}
import org.apache.flink.table.functions.sql.ScalarSqlFunctions
import org.apache.flink.table.functions.sql.internal.SqlAuxiliaryGroupAggFunction
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.plan.logical.{LogicalWindow, TumblingGroupWindow}
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.calcite.LogicalWindowAggregate
import org.apache.flink.table.plan.nodes.logical._
import org.apache.flink.table.plan.nodes.physical.batch._
import org.apache.flink.table.plan.nodes.physical.stream.{StreamExecRank, StreamExecTableSourceScan}
import org.apache.flink.table.plan.rules.logical.DecomposeGroupingSetsRule._
import org.apache.flink.table.plan.schema.{BaseRowSchema, FlinkRelOptTable}
import org.apache.flink.table.plan.util.{AggregateUtil, ConstantRankRange, VariableRankRange}
import com.google.common.collect.{ImmutableList, Lists}
import org.apache.calcite.plan.{Convention, ConventionTraitDef, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.logical.LogicalCalc
import org.apache.calcite.rel.metadata.{JaninoRelMetadataProvider, RelMetadataQuery}
import org.apache.calcite.rex._
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.`type`.{ReturnTypes, SqlTypeName}
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.sql.fun.{SqlCountAggFunction, SqlMinMaxAggFunction, SqlStdOperatorTable}
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.{SqlKind, SqlRankFunction, SqlWindow}
import org.apache.calcite.tools.FrameworkConfig
import org.apache.calcite.util.{DateString, ImmutableBitSet, TimeString, TimestampString}
import org.junit.{Before, BeforeClass}
import java.math.BigDecimal
import java.util.{List => JList}

import org.apache.flink.table.catalog.CatalogManager

import scala.collection.JavaConversions._

class FlinkRelMdHandlerTestBase {

  val rootSchema: SchemaPlus = MetadataTestUtil.createRootSchemaWithCommonTable()
  val frameworkConfig: FrameworkConfig = MetadataTestUtil.createFrameworkConfig(rootSchema)
  val typeFactory = new FlinkTypeFactory(frameworkConfig.getTypeSystem)
  val catalogReader: FlinkCalciteCatalogReader =
    MetadataTestUtil.createCatalogReader(rootSchema, typeFactory)
  val mq: FlinkRelMetadataQuery = FlinkRelMetadataQuery.instance()

  var batchExecTraits: RelTraitSet = _
  var logicalTraits: RelTraitSet = _
  var cluster: RelOptCluster = _
  var relBuilder: FlinkRelBuilder = _
  var rexBuilder: RexBuilder = _

  @Before
  def setUp(): Unit = {
    val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem)

    relBuilder = FlinkRelBuilder.create(
      frameworkConfig, new TableConfig, typeFactory, Array(
        ConventionTraitDef.INSTANCE,
        FlinkRelDistributionTraitDef.INSTANCE,
        RelCollationTraitDef.INSTANCE),
      new CatalogManager()
    )
    batchExecTraits = relBuilder
      .getCluster
      .traitSetOf(Convention.NONE)
      .replace(FlinkConventions.BATCH_PHYSICAL)
    logicalTraits = relBuilder
      .getCluster
      .traitSetOf(Convention.NONE)
      .replace(FlinkConventions.LOGICAL)
    cluster = relBuilder.getCluster
    rexBuilder = cluster.getRexBuilder
  }

  protected def createTableSourceScanBatchExec(
      tableNames: JList[String]): BatchExecTableSourceScan = {
    new BatchExecTableSourceScan(
      cluster,
      batchExecTraits,
      catalogReader.getTable(tableNames).asInstanceOf[FlinkRelOptTable])
  }

  protected def createTableSourceScanStreamExec(
      tableNames: JList[String]): StreamExecTableSourceScan = {
    new StreamExecTableSourceScan(
      cluster,
      batchExecTraits,
      catalogReader.getTable(tableNames).asInstanceOf[FlinkRelOptTable])
  }

  protected def createLogicalTemporalTableSourceScan(
      tableNames: JList[String]): FlinkLogicalSnapshot = {
    val table = catalogReader.getTable(tableNames).asInstanceOf[FlinkRelOptTable]

    val rexProgramBuilder = new RexProgramBuilder(
      typeFactory.createStructType(
        table.getRowType.getFieldList.map(_.getType).toList,
        table.getRowType.getFieldNames),
      rexBuilder
    )
    rexProgramBuilder.getInputRowType.getFieldList.indices.foreach {
      i => rexProgramBuilder.addProject(i, rexProgramBuilder.getInputRowType.getFieldNames.get(i))
    }

    val scan = new FlinkLogicalTableSourceScan(
      cluster,
      logicalTraits,
      table)

    val calc = FlinkLogicalCalc.create(scan, rexProgramBuilder.getProgram)

    new FlinkLogicalSnapshot(
      cluster,
      logicalTraits,
      calc,
      relBuilder.call(ScalarSqlFunctions.PROCTIME))
  }

  // scan on table t1
  protected lazy val scanOfT1: BatchExecTableSourceScan =
    createTableSourceScanBatchExec(ImmutableList.of("t1"))

  // scan on table t2
  protected lazy val scanOfT2: BatchExecTableSourceScan =
    createTableSourceScanBatchExec(ImmutableList.of("t2"))

  // scan on table student
  protected lazy val scanOfTStudent: BatchExecTableSourceScan =
    createTableSourceScanBatchExec(ImmutableList.of("student"))

  // scan on table bigT1
  protected lazy val scanOfBigT1: BatchExecTableSourceScan =
    createTableSourceScanBatchExec(ImmutableList.of("bigT1"))

  // temporal table source with calc
  protected lazy val temporalTableSourceScanWithCalc: FlinkLogicalSnapshot =
    createLogicalTemporalTableSourceScan(ImmutableList.of("t1"))

  // SELECT * FROM t3 INTERSECT ALL SELECT id, score FROM student
  protected lazy val intersectAll: RelNode = relBuilder.scan("t3")
    .scan("student").project(relBuilder.field(0), relBuilder.field(1))
    .intersect(true).build()

  // SELECT * FROM t3 INTERSECT SELECT id, score FROM student
  protected lazy val intersect: RelNode = relBuilder.scan("t3")
    .scan("student").project(relBuilder.field(0), relBuilder.field(1))
    .intersect(false).build()

  // select * from t3 join student on t3.id = student.id
  protected lazy val innerJoin: RelNode = relBuilder.scan("t3").scan("student")
    .join(JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from t2 join t1 on t2.id = t1.id
  protected lazy val innerJoinOnNoneUniqueKeys: RelNode = relBuilder.scan("t2").scan("t1")
    .join(JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from t2 join student on t2.id = student.id
  protected lazy val innerJoinOnOneSideUniqueKeys: RelNode = relBuilder.scan("t2").scan("student")
    .join(JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from t3 join student on t3.score = student.score
  protected lazy val innerJoinOnScore: RelNode = relBuilder.scan("t3").scan("student")
    .join(JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 3)))
    .build

  // select * from t3 join t4 on t3.id = t4.id and t3.score > t4.score
  protected lazy val innerJoinWithNonEquiCond: RelNode = relBuilder.scan("t3").scan("t4")
    .join(JoinRelType.INNER, relBuilder.call(AND,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))
    ).build

  // select * from t3 join student on t3.id > student.id
  protected lazy val innerJoinWithoutEquiCond: RelNode = relBuilder.scan("t3").scan("student")
    .join(JoinRelType.INNER,
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from t5 join t6 on t5.id = t6.id
  protected lazy val innerJoinDisjoint: RelNode = relBuilder.scan("t5").scan("t6")
    .join(JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from t3 full join student on t3.id = student.id
  protected lazy val fullJoin: RelNode = relBuilder.scan("t3").scan("student").
    join(JoinRelType.FULL,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from t2 full join student on t2.id = student.id
  protected lazy val fullJoinOnOneSideUniqueKeys: RelNode = relBuilder.scan("t2").scan("student")
    .join(JoinRelType.FULL,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from t3 full join student on t3.score = student.score
  protected lazy val fullJoinOnScore: RelNode = relBuilder.scan("t3").scan("student")
    .join(JoinRelType.FULL,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 3)))
    .build

  // select * from t2 full join t1 on t2.id = t1.id
  protected lazy val fullJoinOnNoneUniqueKeys: RelNode = relBuilder.scan("t2").scan("t1")
    .join(JoinRelType.FULL,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from t3 full join student
  protected lazy val fullJoinWithoutEquiCond: RelNode = relBuilder.scan("t3").scan("student")
    .join(JoinRelType.FULL, relBuilder.literal(true)).build

  // select * from t3 full join t4 on t3.id = t4.id and t3.score > t4.score
  protected lazy val fullJoinWithNonEquiCond: RelNode = relBuilder.scan("t3").scan("t4")
    .join(JoinRelType.FULL, relBuilder.call(AND,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)),
      relBuilder.call(GREATER_THAN, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1)))
    ).build

  // select * from t5 full join t6 on t5.id = t6.id
  protected lazy val fullJoinDisjoint: RelNode = relBuilder.scan("t5").scan("t6")
    .join(JoinRelType.FULL,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from t3 left join student on t3.id = student.id
  protected lazy val leftJoin: RelNode = relBuilder.scan("t3").scan("student")
    .join(JoinRelType.LEFT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from t2 left join student on t2.id = student.id
  protected lazy val leftJoinOnRHSUniqueKeys: RelNode = relBuilder.scan("t2").scan("student")
    .join(JoinRelType.LEFT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from student left join t2 on student.id = t2.id
  protected lazy val leftJoinOnLHSUniqueKeys: RelNode = relBuilder.scan("student").scan("t2")
    .join(JoinRelType.LEFT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from t3 left join student on t3.score = student.score
  protected lazy val leftJoinOnScore: RelNode = relBuilder.scan("t3").scan("student")
    .join(JoinRelType.LEFT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 3)))
    .build

  // select * from 2 left join t1 on t2.id = t1.id
  protected lazy val leftJoinOnNoneUniqueKeys: RelNode = relBuilder.scan("t2").scan("t1")
    .join(JoinRelType.LEFT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from t3 left join student on t3.id >= student.id
  protected lazy val leftJoinWithoutEquiCond: RelNode = relBuilder.scan("t3").scan("student")
    .join(JoinRelType.LEFT,
      relBuilder.call(GREATER_THAN_OR_EQUAL, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from t3 left join student on t3.id = t4.id and t3.score < t4.score
  protected lazy val leftJoinWithNonEquiCond: RelNode = relBuilder.scan("t3").scan("t4")
    .join(JoinRelType.LEFT, relBuilder.call(AND,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)),
      relBuilder.call(LESS_THAN, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1))
    )).build

  // select * from t5 left join t6 on t5.id = t6.id
  protected lazy val leftJoinDisjoint: RelNode = relBuilder.scan("t5").scan("t6")
    .join(JoinRelType.LEFT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from t3 left join student on t3.id = student.id
  protected lazy val rightJoin: RelNode = relBuilder.scan("t3").scan("student")
    .join(JoinRelType.RIGHT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from t2 right join student on t2.id = student.id
  protected lazy val rightJoinOnRHSUniqueKeys: RelNode = relBuilder.scan("t2").scan("student")
    .join(JoinRelType.RIGHT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from student right join t2 on student.id = t2.id
  protected lazy val rightJoinOnLHSUniqueKeys: RelNode = relBuilder.scan("student").scan("t2")
    .join(JoinRelType.RIGHT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from t3 right join student on t3.id < student.id
  protected lazy val rightJoinWithoutEquiCond: RelNode = relBuilder.scan("t3").scan("student")
    .join(JoinRelType.RIGHT,
      relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from t3 right join t4 on t3.id = t4.id and t3.score < t4.score
  protected lazy val rightJoinWithNonEquiCond: RelNode = relBuilder.scan("t3").scan("t4")
    .join(JoinRelType.RIGHT, relBuilder.call(AND,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)),
      relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1))))
    .build

  // select * from t5 right join t6 on t5.id = t6.id
  protected lazy val rightJoinDisjoint: RelNode = relBuilder.scan("t5").scan("t6")
    .join(JoinRelType.RIGHT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0)))
    .build

  // select * from t3 where id in (select id from student)
  protected lazy val semiJoin: RelNode = relBuilder.scan("t3").scan("student").semiJoin(
    relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))).build

  // select * from t3 where id not in (select id from student)
  protected lazy val antiJoin: RelNode = relBuilder.scan("t3").scan("student").antiJoin(
    relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))).build

  // select id == 1, id, score, true, 2.1, 2 from t1
  protected lazy val project: RelNode = {
    val ts = relBuilder.scan("t1").build()
    relBuilder.push(ts)
    // projects: $0==1, $0, $1, true, 2.1, 2
    val projects = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      relBuilder.field(1),
      relBuilder.literal(true),
      relBuilder.getRexBuilder.makeLiteral(2.1D, typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.DOUBLE_TYPE_INFO, isNullable = false), true),
      relBuilder.getRexBuilder.makeLiteral(2L, typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.LONG_TYPE_INFO, isNullable = false), true))
    relBuilder.project(projects).build()
  }

  // equivalent SQL is
  // select count(id) as c from t1 group by score
  protected lazy val (
    unSplittableLocalAgg,
    unSplittableGlobalAggWithLocalAgg,
    unSplittableGlobalAggWithoutLocalAgg) = {
    // aggCall: count(id) as c
    val aggCall = AggregateCall.create(
      SqlStdOperatorTable.COUNT,
      false,
      ImmutableList.of(Integer.valueOf(0)),
      -1,
      typeFactory.createTypeFromTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, isNullable = false),
      "c")
    // local aggregate: count(id) as c group by score
    val rowTypeOfAgg = typeFactory.builder
      .add("score", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.DOUBLE_TYPE_INFO, isNullable = true))
      .add("c", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.LONG_TYPE_INFO, isNullable = false)).build()
    val grouping = Array(1)
    val aggCallToAggFunction = Seq((aggCall, new expr.CountAggFunction))
    val localAgg = new BatchExecLocalHashAggregate(
      cluster,
      relBuilder,
      batchExecTraits,
      scanOfT1,
      aggCallToAggFunction,
      rowTypeOfAgg,
      scanOfT1.getRowType,
      grouping,
      auxGrouping = Array())
    val toTrait = localAgg.getTraitSet.replace(
      FlinkRelDistribution.hash(ImmutableList.of(Integer.valueOf(0)), requireStrict = false))
    val exchange = new BatchExecExchange(
      cluster,
      toTrait,
      localAgg,
      FlinkRelDistribution.hash(ImmutableList.of(Integer.valueOf(0)), requireStrict = false))

    // global aggregate: count(id) as c group by score
    val globalAgg = new BatchExecHashAggregate(
      cluster,
      relBuilder,
      toTrait,
      exchange,
      aggCallToAggFunction,
      rowTypeOfAgg,
      exchange.getRowType,
      Array(0),
      auxGrouping = Array(),
      true)

    val globalWithoutAgg = new BatchExecHashAggregate(
      cluster,
      relBuilder,
      toTrait,
      scanOfT1,
      aggCallToAggFunction,
      rowTypeOfAgg,
      scanOfT1.getRowType,
      grouping,
      auxGrouping = Array(),
      false)
    (localAgg, globalAgg, globalWithoutAgg)
  }

  // equivalent SQL is
  // select max(score) as s from student group by id, age
  protected lazy val (
    unSplittableLocalAggWithAuxGrouping,
    unSplittableGlobalAggWithLocalAggAndAuxGrouping,
    unSplittableGlobalAggWithoutLocalAggWithAuxGrouping) = {
    // aggCall: max(score) as s
    val aggCall = AggregateCall.create(
      SqlStdOperatorTable.MAX,
      false,
      ImmutableList.of(Integer.valueOf(1)),
      -1,
      typeFactory.createTypeFromTypeInfo(BasicTypeInfo.DOUBLE_TYPE_INFO, isNullable = false),
      "s")
    // local aggregate: count(id) as s group by id, age
    val rowTypeOfAgg = typeFactory.builder
      .add("id", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.INT_TYPE_INFO, isNullable = false))
      .add("age", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.INT_TYPE_INFO, isNullable = false))
      .add("s", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.DOUBLE_TYPE_INFO, isNullable = false)).build()
    val grouping = Array(0)
    val auxGrouping = Array(2)
    val aggCallToAggFunction = Seq((aggCall, new expr.DoubleMaxAggFunction))
    val localAggWithAuxGrouping = new BatchExecLocalHashAggregate(
      cluster,
      relBuilder,
      batchExecTraits,
      scanOfTStudent,
      aggCallToAggFunction,
      rowTypeOfAgg,
      scanOfTStudent.getRowType,
      grouping,
      auxGrouping)
    val toTrait = localAggWithAuxGrouping.getTraitSet.replace(
      FlinkRelDistribution.hash(ImmutableList.of(Integer.valueOf(0)), requireStrict = false))
    val exchange = new BatchExecExchange(
      cluster,
      toTrait,
      localAggWithAuxGrouping,
      FlinkRelDistribution.hash(ImmutableList.of(Integer.valueOf(0)), requireStrict = false))

    // global aggregate: max(score) as s group by id, age
    val globalAggWithAuxGrouping = new BatchExecHashAggregate(
      cluster,
      relBuilder,
      toTrait,
      exchange,
      aggCallToAggFunction,
      rowTypeOfAgg,
      exchange.getRowType,
      Array(0),
      Array(1),
      true)

    val globalWithoutAggWithAuxGrouping = new BatchExecHashAggregate(
      cluster,
      relBuilder,
      toTrait,
      scanOfTStudent,
      aggCallToAggFunction,
      rowTypeOfAgg,
      scanOfTStudent.getRowType,
      grouping,
      auxGrouping,
      false)
    (localAggWithAuxGrouping, globalAggWithAuxGrouping, globalWithoutAggWithAuxGrouping)
  }

  // equivalent SQL is
  // select min(age) as min_age from student group by id
  protected lazy val (
    unSplittableLocalAgg2,
    unSplittableGlobalAgg2WithLocalAgg,
    unSplittableGlobalAgg2WithoutLocalAgg) = {
    // aggCall: count(age) as min_age
    val countAgeAggCall = AggregateCall.create(
      SqlStdOperatorTable.COUNT,
      false,
      ImmutableList.of(Integer.valueOf(2)),
      -1,
      typeFactory.createTypeFromTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, isNullable = false),
      "min_age")
    // local aggregate: count(age) as min_age group by id
    val rowTypeOfAgg = typeFactory.builder
      .add("id", typeFactory.createTypeFromTypeInfo(BasicTypeInfo.INT_TYPE_INFO, isNullable = true))
      .add("min_age", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.INT_TYPE_INFO, isNullable = false))
      .build()
    val aggCallToAggFunction = Seq((countAgeAggCall, new expr.Count1AggFunction))
    val localAgg = new BatchExecLocalHashAggregate(
      cluster,
      relBuilder,
      batchExecTraits,
      scanOfTStudent,
      aggCallToAggFunction,
      rowTypeOfAgg,
      scanOfTStudent.getRowType,
      Array(0),
      auxGrouping = Array())

    val toTrait = localAgg.getTraitSet.replace(
      FlinkRelDistribution.hash(ImmutableList.of(Integer.valueOf(0)), requireStrict = false))
    val exchange = new BatchExecExchange(
      cluster,
      toTrait,
      localAgg,
      FlinkRelDistribution.hash(ImmutableList.of(Integer.valueOf(0)), requireStrict = false))

    // global aggregate: count(age) as min_age group by id, which has local aggregate
    val globalAggWithLocalAgg = new BatchExecHashAggregate(
      cluster,
      relBuilder,
      toTrait,
      exchange,
      aggCallToAggFunction,
      rowTypeOfAgg,
      exchange.getRowType,
      Array(0),
      auxGrouping = Array(),
      true)

    // global aggregate: count(age) as min_age group by id, which has no local aggregate
    val globalAggWithoutLocalAgg = new BatchExecHashAggregate(
      cluster,
      relBuilder,
      toTrait,
      scanOfTStudent,
      aggCallToAggFunction,
      rowTypeOfAgg,
      scanOfTStudent.getRowType,
      Array(0),
      auxGrouping = Array(),
      false)
    (localAgg, globalAggWithLocalAgg, globalAggWithoutLocalAgg)
  }

  // equivalent SQL is
  // select avg(score) as c from t1 group by id
  protected lazy val (
    splittableLocalAgg,
    splittableGlobalAggWithLocalAgg,
    splittableGlobalAggWithoutLocalAgg) = {
    val avgCall = AggregateCall.create(
      SqlStdOperatorTable.AVG,
      false,
      ImmutableList.of(Integer.valueOf(1)),
      -1,
      typeFactory.createTypeFromTypeInfo(BasicTypeInfo.DOUBLE_TYPE_INFO, isNullable = false),
      "avg_score")
    val rowTypeOfLocalAgg = typeFactory.builder
      .add("id", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.INT_TYPE_INFO, isNullable = false))
      .add("sum$0", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.LONG_TYPE_INFO, isNullable = true))
      .add("count$1", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.LONG_TYPE_INFO, isNullable = true))
      .build()
    val grouping = Array(0)
    val aggCallToAggFunction = Seq((avgCall, new expr.DoubleAvgAggFunction))
    val localAgg = new BatchExecLocalHashAggregate(
      cluster,
      relBuilder,
      batchExecTraits,
      scanOfT1,
      aggCallToAggFunction,
      rowTypeOfLocalAgg,
      scanOfT1.getRowType,
      grouping,
      auxGrouping = Array())
    val toTrait = localAgg.getTraitSet.replace(
      FlinkRelDistribution.hash(ImmutableList.of(Integer.valueOf(0)), requireStrict = false))
    val exchange = new BatchExecExchange(
      cluster,
      toTrait,
      localAgg,
      FlinkRelDistribution.hash(ImmutableList.of(Integer.valueOf(0)), requireStrict = false))
    val rowTypeOfglobalAgg = typeFactory.builder
      .add("id", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.INT_TYPE_INFO, isNullable = false))
      .add("avg_score", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.DOUBLE_TYPE_INFO, isNullable = true))
      .build()
    // global aggregate: avg(id) as c group by score
    val globalAgg = new BatchExecHashAggregate(
      cluster,
      relBuilder,
      toTrait,
      exchange,
      aggCallToAggFunction,
      rowTypeOfglobalAgg,
      exchange.getRowType,
      grouping,
      auxGrouping = Array(),
      true)

    val globalAggWithoutLocalAgg = new BatchExecHashAggregate(
      cluster,
      relBuilder,
      toTrait,
      scanOfT1,
      aggCallToAggFunction,
      rowTypeOfglobalAgg,
      scanOfT1.getRowType,
      grouping,
      auxGrouping = Array(),
      false)
    (localAgg, globalAgg, globalAggWithoutLocalAgg)
  }

  // equivalent SQL is
  // select avg(score) as c from student group by id, age
  protected lazy val (
    splittableLocalAggWithAuxGrouping,
    splittableGlobalAggWithLocalAggWithAuxGrouping,
    splittableGlobalAggWithoutLocalAggWithAuxGrouping) = {
    val avgCall = AggregateCall.create(
      SqlStdOperatorTable.AVG,
      false,
      ImmutableList.of(Integer.valueOf(1)),
      -1,
      typeFactory.createTypeFromTypeInfo(BasicTypeInfo.DOUBLE_TYPE_INFO, isNullable = false),
      "avg_score")
    val rowTypeOfLocalAgg = typeFactory.builder
      .add("id", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.INT_TYPE_INFO, isNullable = false))
      .add("age", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.INT_TYPE_INFO, isNullable = false))
      .add("sum$0", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.LONG_TYPE_INFO, isNullable = true))
      .add("count$1", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.LONG_TYPE_INFO, isNullable = true))
      .build()
    val grouping = Array(0)
    val auxGrouping = Array(2)
    val aggCallToAggFunction = Seq((avgCall, new expr.DoubleAvgAggFunction))
    val localAggWithAuxGrouping = new BatchExecLocalHashAggregate(
      cluster,
      relBuilder,
      batchExecTraits,
      scanOfTStudent,
      aggCallToAggFunction,
      rowTypeOfLocalAgg,
      scanOfTStudent.getRowType,
      grouping,
      auxGrouping)
    val toTrait = localAggWithAuxGrouping.getTraitSet.replace(
      FlinkRelDistribution.hash(ImmutableList.of(Integer.valueOf(0)), requireStrict = false))
    val exchange = new BatchExecExchange(
      cluster,
      toTrait,
      localAggWithAuxGrouping,
      FlinkRelDistribution.hash(ImmutableList.of(Integer.valueOf(0)), requireStrict = false))
    val rowTypeOfglobalAgg = typeFactory.builder
      .add("id", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.INT_TYPE_INFO, isNullable = false))
      .add("age", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.INT_TYPE_INFO, isNullable = false))
      .add("avg_score", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.DOUBLE_TYPE_INFO, isNullable = true))
      .build()
    // global aggregate: avg(score) as avg_score group by id, age
    val globalAggWithAuxGrouping = new BatchExecHashAggregate(
      cluster,
      relBuilder,
      toTrait,
      exchange,
      aggCallToAggFunction,
      rowTypeOfglobalAgg,
      exchange.getRowType,
      grouping,
      auxGrouping = Array(1),
      true)

    val globalAggWithoutLocalAggWithAuxGrouping = new BatchExecHashAggregate(
      cluster,
      relBuilder,
      toTrait,
      scanOfTStudent,
      aggCallToAggFunction,
      rowTypeOfglobalAgg,
      scanOfTStudent.getRowType,
      grouping,
      auxGrouping,
      false)
    (localAggWithAuxGrouping, globalAggWithAuxGrouping, globalAggWithoutLocalAggWithAuxGrouping)
  }

  // equivalent SQL is
  // select avg(score) as c from bigT1 group by id
  protected lazy val localAggOnBigTable: BatchExecLocalHashAggregate = {
    val avgCall = AggregateCall.create(
      SqlStdOperatorTable.AVG,
      false,
      ImmutableList.of(Integer.valueOf(1)),
      -1,
      typeFactory.createTypeFromTypeInfo(BasicTypeInfo.DOUBLE_TYPE_INFO, isNullable = false),
      "avg_score")
    val rowTypeOfLocalAgg = typeFactory.builder
      .add("id", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.INT_TYPE_INFO, isNullable = false))
      .add("sum$0", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.LONG_TYPE_INFO, isNullable = true))
      .add("count$1", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.LONG_TYPE_INFO, isNullable = true))
      .build()
    val grouping = Array(0)
    val aggCallToAggFunction = Seq((avgCall, new expr.DoubleAvgAggFunction))
    new BatchExecLocalHashAggregate(
      cluster,
      relBuilder,
      batchExecTraits,
      scanOfBigT1,
      aggCallToAggFunction,
      rowTypeOfLocalAgg,
      scanOfBigT1.getRowType,
      grouping,
      Array())
  }

  private lazy val overWindowGroups = {
    ImmutableList.of(
      new Window.Group(
        ImmutableBitSet.of(),
        false,
        RexWindowBound.create(SqlWindow.createUnboundedPreceding(new SqlParserPos(1, 44)), null),
        RexWindowBound.create(SqlWindow.createCurrentRow(new SqlParserPos(1, 44)), null),
        RelCollationImpl.of(
          new RelFieldCollation(
            1, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST)),
        ImmutableList.of(
          new Window.RexWinAggCall(
            new SqlRankFunction(SqlKind.RANK, ReturnTypes.INTEGER, true),
            typeFactory.createTypeFromTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, isNullable = false),
            ImmutableList.of[RexNode](),
            0,
            false
          )
        )
      ),
      new Window.Group(
        ImmutableBitSet.of(2),
        false,
        RexWindowBound.create(SqlWindow.createUnboundedPreceding(new SqlParserPos(1, 94)), null),
        RexWindowBound.create(SqlWindow.createUnboundedFollowing(new SqlParserPos(0, 0)), null),
        RelCollationImpl.of(),
        ImmutableList.of(
          new Window.RexWinAggCall(
            new SqlMinMaxAggFunction(SqlKind.MAX),
            typeFactory.createTypeFromTypeInfo(BasicTypeInfo.DOUBLE_TYPE_INFO, isNullable = true),
            ImmutableList.of[RexNode](),
            1,
            false
          )
        )
      )
    )
  }

  // equivalent SQL is
  // select id, score, age, height ,
  // rank() over (partition by id order by score),
  // max(height) over(partition by age) from student
  protected lazy val (logicalOverWindow, overWindowAgg) = {
    val scan = createTableSourceScanBatchExec(ImmutableList.of("student"))
    val rankAggCall = AggregateCall.create(
      SqlStdOperatorTable.RANK,
      false,
      ImmutableList.of(),
      -1,
      typeFactory.createTypeFromTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, isNullable = false),
      "score_rank")
    val maxAggCall = AggregateCall.create(
      SqlStdOperatorTable.MAX,
      false,
      ImmutableList.of(Integer.valueOf(3)),
      -1,
      typeFactory.createTypeFromTypeInfo(BasicTypeInfo.DOUBLE_TYPE_INFO, isNullable = true),
      "max_height"
    )
    val (rowTypeOfInnerWindowAgg, rowTypeOfOuterWindowAgg) = {
      val innerRowTypeBuilder = typeFactory.builder
      val outerRowTypeBuilder = typeFactory.builder
      scan.getRowType.getFieldList.foreach { f =>
        innerRowTypeBuilder.add(f.getName, f.getType)
        outerRowTypeBuilder.add(f.getName, f.getType)
      }
      val typeOfRankAggCall = typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.LONG_TYPE_INFO, isNullable = false)
      innerRowTypeBuilder.add("score_rank", typeOfRankAggCall)
      outerRowTypeBuilder.add("score_rank", typeOfRankAggCall)
      outerRowTypeBuilder.add("max_height", typeOfRankAggCall)
      (innerRowTypeBuilder.build(), outerRowTypeBuilder.build())
    }

    val overWindow = new FlinkLogicalOverWindow(
      cluster,
      logicalTraits,
      scan,
      ImmutableList.of(),
      rowTypeOfOuterWindowAgg,
      overWindowGroups
    )
    val innerWindowAgg = new BatchExecOverAggregate(
      cluster,
      relBuilder,
      batchExecTraits,
      scan,
      Seq((overWindowGroups(0),
        Seq((rankAggCall, new expr.RankFunction(Array(DataTypes.DOUBLE)))))),
      rowTypeOfInnerWindowAgg,
      scan.getRowType,
      grouping = Array(0),
      orderKeyIdxs = Array(1),
      orders = Array(true),
      nullIsLasts = Array(false),
      logicWindow = overWindow
    )
    val overWindowAggBatchExec = new BatchExecOverAggregate(
      cluster,
      relBuilder,
      batchExecTraits,
      innerWindowAgg,
      Seq((overWindowGroups(1), Seq((maxAggCall, new expr.DoubleMaxAggFunction)))),
      rowTypeOfOuterWindowAgg,
      rowTypeOfInnerWindowAgg,
      grouping = Array(2),
      orderKeyIdxs = Array(),
      orders = Array(),
      nullIsLasts = Array(),
      logicWindow = overWindow
    )
    (overWindow, overWindowAggBatchExec)
  }

  protected lazy val sortCollation: RelCollation = sort.asInstanceOf[Sort].getCollation

  // equivalent SQL is
  // select * from t1 order by score desc, id
  protected lazy val sortBatchExec: BatchExecSort = {
    new BatchExecSort(
      cluster,
      scanOfT1.getTraitSet.replace(FlinkRelDistribution.SINGLETON).replace(sortCollation),
      scanOfT1,
      sortCollation)
  }

  // equivalent SQL is
  // select * from t1 order by score desc, id
  protected lazy val sortLimitBatchExec: BatchExecSortLimit = {
    new BatchExecSortLimit(
      cluster,
      scanOfT1.getTraitSet.replace(FlinkRelDistribution.SINGLETON).replace(sortCollation),
      scanOfT1,
      sortCollation,
      relBuilder.literal(1),
      relBuilder.literal(10),
      true,
      "")
  }

  protected lazy val sort: RelNode = relBuilder.scan("t1").sortLimit(
    1,
    10,
    relBuilder.desc(relBuilder.nullsFirst(relBuilder.field("id"))),
    relBuilder.nullsLast(relBuilder.field("score"))).build()

  // equivalent SQL is
  // select * from t1
  protected lazy val limitBatchExec = new BatchExecLimit(
    cluster,
    batchExecTraits,
    scanOfT1,
    relBuilder.literal(1),
    relBuilder.literal(10),
    true,
    "")

  // union t1 and t2
  protected lazy val unionBatchExec = new BatchExecUnion(
    cluster,
    batchExecTraits,
    Array(scanOfT1, scanOfT2).toList,
    scanOfT1.getRowType,
    true)

  // union all t1 and t2
  protected lazy val unionAll: RelNode = relBuilder.scan("t1").scan("t2").union(true).build()

  // union t1 and t2
  protected lazy val union: RelNode = relBuilder.scan("t1").scan("t2").union(false).build()

  // select id, age, count(score) from student group by id, age
  protected lazy val aggWithAuxGroup: Aggregate = {
    val ts = relBuilder.scan("student").build()
    val agg = relBuilder.push(ts).aggregate(
      relBuilder.groupKey(relBuilder.field(0)),
      Lists.newArrayList(
        AggregateCall.create(
          SqlAuxiliaryGroupAggFunction, false, false, List[Integer](2), -1, 1, ts, null, "age"),
        AggregateCall.create(
          new SqlCountAggFunction("COUNT"), false, false, List[Integer](1), -1, 1, ts, null, "c")
      )).build()
    agg.asInstanceOf[Aggregate]
  }

  protected lazy val aggWithExpand: Aggregate = {
    val ts = relBuilder.scan("t1").build()
    val expandOutputType = buildExpandRowType(
      ts.getCluster.getTypeFactory, ts.getRowType, Array.empty[Integer])
    val expandProjects = createExpandProjects(
      ts.getCluster.getRexBuilder,
      ts.getRowType,
      expandOutputType,
      ImmutableBitSet.of(0, 1),
      ImmutableList.of(ImmutableBitSet.of(0), ImmutableBitSet.of(1)), Array.empty[Integer])
    val expand = new FlinkLogicalExpand(
      ts.getCluster, ts.getTraitSet, ts, expandOutputType, expandProjects, 2)

    val agg = relBuilder.push(expand).aggregate(
      relBuilder.groupKey(relBuilder.fields()),
      relBuilder.count(false, "c", relBuilder.field("id"))).build()
    agg.asInstanceOf[Aggregate]
  }

  // select id, score, age, count(height) from student
  // group by grouping sets((id, score), (id, age))
  protected lazy val aggWithAuxGroupAndExpand: Aggregate = {
    val ts = relBuilder.scan("student").build()
    val expandOutputType = buildExpandRowType(
      ts.getCluster.getTypeFactory, ts.getRowType, Array.empty[Integer])
    val expandProjects = createExpandProjects(
      ts.getCluster.getRexBuilder,
      ts.getRowType,
      expandOutputType,
      ImmutableBitSet.of(0, 1, 2),
      ImmutableList.of(ImmutableBitSet.of(0, 1), ImmutableBitSet.of(0, 2)), Array.empty[Integer])
    val expand = new FlinkLogicalExpand(
      ts.getCluster, ts.getTraitSet, ts, expandOutputType, expandProjects, 4)

    // agg output type: id, $e, score, age, count(height)
    val agg = relBuilder.push(expand).aggregate(
      relBuilder.groupKey(relBuilder.fields(Seq[Integer](0, 4).toList)),
      Lists.newArrayList(
        AggregateCall.create(SqlAuxiliaryGroupAggFunction, false, false,
          List[Integer](1), -1, 1, ts, null, "score"),
        AggregateCall.create(SqlAuxiliaryGroupAggFunction, false, false,
          List[Integer](2), -1, 1, ts, null, "age"),
        AggregateCall.create(
          new SqlCountAggFunction("COUNT"), false, false, List[Integer](3), -1, 2, ts, null, "a")
      )).build()
    agg.asInstanceOf[Aggregate]
  }

  // For window start/end/proc_time the windowAttribute inferred type is a hard code val,
  // only for row_time we distinguish by batch row time, for what we hard code DataTypes.TIMESTAMP,
  // which is ok here for testing.
  private lazy val windowRef: WindowReference = WindowReference.apply("w$",
    Some(DataTypes.TIMESTAMP))

  private lazy val tublingGroupWindow: LogicalWindow =
    TumblingGroupWindow(windowRef, 'rowtime, 900000.millis)

  private lazy val namedPropertiesOfWindowAgg =
    Seq(NamedWindowProperty("w$start", WindowStart(windowRef)),
      NamedWindowProperty("w$end", WindowStart(windowRef)),
      NamedWindowProperty("w$rowtime", RowtimeAttribute(windowRef)),
      NamedWindowProperty("w$proctime", ProctimeAttribute(windowRef)))

  // equivalent SQL is
  // select a, b, count(c) as s, TUMBLE_START(rowtime, INTERVAL '15' MINUTE) as ping_start from
  // temporalTable group by a, b, TUMBLE(rowtime, INTERVAL '15' MINUTE)
  protected lazy val (logicalWindowAgg, flinkLogicalWindowAgg):
  (LogicalWindowAggregate, FlinkLogicalWindowAggregate) = {
    relBuilder.scan("temporalTable")
    val ts = relBuilder.peek()
    val project = relBuilder.project(relBuilder.fields(Seq[Integer](0, 1, 4, 2).toList)).build()
    val aggCallOfWindowAgg = Lists.newArrayList(AggregateCall.create(
      new SqlCountAggFunction("COUNT"), false, false, List[Integer](3), -1, 2, project, null, "s"))
    // TUMBLE(rowtime, INTERVAL '15' MINUTE))
    val logicalAgg = new LogicalWindowAggregate(
      tublingGroupWindow,
      namedPropertiesOfWindowAgg,
      ts.getCluster,
      ts.getTraitSet,
      project,
      false,
      ImmutableBitSet.of(0, 1),
      ImmutableList.of(ImmutableBitSet.of(0, 1)),
      aggCallOfWindowAgg)
    val flinkLogicalAgg = new FlinkLogicalWindowAggregate(
      tublingGroupWindow,
      namedPropertiesOfWindowAgg,
      ts.getCluster,
      logicalTraits,
      project,
      false,
      ImmutableBitSet.of(0, 1),
      ImmutableList.of(ImmutableBitSet.of(0, 1)),
      aggCallOfWindowAgg)
    (logicalAgg, flinkLogicalAgg)
  }

  // equivalent SQL is
  // select a, c, count(b) as s, TUMBLE_START(rowtime, INTERVAL '15' MINUTE) as ping_start from
  // temporalTable1 group by a, c, TUMBLE(rowtime, INTERVAL '15' MINUTE)
  protected lazy val (logicalWindowAggWithAuxGroup, flinkLogicalWindowAggWithAuxGroup):
  (LogicalWindowAggregate, FlinkLogicalWindowAggregate) = {
    relBuilder.scan("temporalTable1")
    val ts = relBuilder.peek()
    val project = relBuilder.project(relBuilder.fields(Seq[Integer](0, 2, 4, 1).toList)).build()
    val aggCallOfWindowAggWithAuxGroup = Lists.newArrayList(
      AggregateCall.create(
        SqlAuxiliaryGroupAggFunction, false, false, List[Integer](1), -1, 1, project, null, "c"),
      AggregateCall.create(new SqlCountAggFunction("COUNT"), false, false,
        List[Integer](3), -1, 1, project, null, "s"))

    // TUMBLE(rowtime, INTERVAL '15' MINUTE))
    val logicalAggWithAuxGroup = new LogicalWindowAggregate(
      tublingGroupWindow,
      namedPropertiesOfWindowAgg,
      ts.getCluster,
      ts.getTraitSet,
      project,
      false,
      ImmutableBitSet.of(0),
      ImmutableList.of(ImmutableBitSet.of(0)),
      aggCallOfWindowAggWithAuxGroup)
    val flinkLogicalAggWithAuxGroup = new FlinkLogicalWindowAggregate(
      tublingGroupWindow,
      namedPropertiesOfWindowAgg,
      ts.getCluster,
      logicalTraits,
      project,
      false,
      ImmutableBitSet.of(0),
      ImmutableList.of(ImmutableBitSet.of(0)),
      aggCallOfWindowAggWithAuxGroup)
    (logicalAggWithAuxGroup, flinkLogicalAggWithAuxGroup)
  }

  // equivalent SQL is
  // select a, b, count(c) as s, TUMBLE_START(rowtime, INTERVAL '15' MINUTE) as ping_start from
  // temporalTable group by a, b, TUMBLE(rowtime, INTERVAL '15' MINUTE)
  protected lazy val (
    localWindowAgg,
    globalWindowAggWithLocalAgg,
    globalWindowAggWithoutLocalAgg) = {
    val scanOfTemporalTable: BatchExecTableSourceScan =
      createTableSourceScanBatchExec(ImmutableList.of("temporalTable"))
    relBuilder.push(scanOfTemporalTable)
    val projects = relBuilder.fields(Seq[Integer](0, 1, 4, 2).toList)
    val project = relBuilder.project(projects).build()
    val outputRowType = project.getRowType
    val program = RexProgram.create(
      scanOfTemporalTable.getRowType,
      projects,
      null,
      outputRowType,
      relBuilder.getRexBuilder)
    val calc = new BatchExecCalc(cluster, batchExecTraits,
      scanOfTemporalTable, outputRowType, program, "")
    val (_, _, aggregates) =
      AggregateUtil.transformToBatchAggregateFunctions(
        flinkLogicalWindowAgg.getAggCallList, calc.getRowType)
    val aggCallToAggFunction = flinkLogicalWindowAgg.getAggCallList.zip(aggregates)
    val tsType = typeFactory.createTypeFromTypeInfo(SqlTimeTypeInfo.TIMESTAMP, isNullable = false)
    val localAggRowType = typeFactory.builder
      .add("a", typeFactory.createTypeFromTypeInfo(BasicTypeInfo.INT_TYPE_INFO, isNullable = true))
      .add("b", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.STRING_TYPE_INFO, isNullable = true))
      .add("assignedWindow$", tsType)
      .add("count$0", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.LONG_TYPE_INFO, isNullable = true))
      .build()
    val localAgg = new BatchExecLocalHashWindowAggregate(
      tublingGroupWindow,
      2,
      tsType,
      namedPropertiesOfWindowAgg,
      cluster,
      relBuilder,
      batchExecTraits,
      calc,
      aggCallToAggFunction,
      localAggRowType,
      calc.getRowType,
      Array(0, 1),
      Array())
    val toTrait = localAgg.getTraitSet.replace(
      FlinkRelDistribution.hash(ImmutableList.of(Integer.valueOf(0), Integer.valueOf(1)),
        requireStrict = false))
    val exchange = new BatchExecExchange(
      cluster,
      toTrait,
      localAgg,
      FlinkRelDistribution.hash(ImmutableList.of(Integer.valueOf(0), Integer.valueOf(1)),
        requireStrict = false))
    val globalAgg = new BatchExecHashWindowAggregate(
      tublingGroupWindow,
      2,
      tsType,
      namedPropertiesOfWindowAgg,
      cluster,
      relBuilder,
      exchange.getTraitSet,
      exchange,
      aggCallToAggFunction,
      flinkLogicalWindowAgg.getRowType,
      exchange.getRowType,
      Array(0, 1),
      Array(),
      false,
      true)
    val exchange2 = new BatchExecExchange(
      cluster,
      toTrait,
      calc,
      FlinkRelDistribution.hash(ImmutableList.of(Integer.valueOf(0), Integer.valueOf(1)),
        requireStrict = false))
    val globalAggWithoutLocal = new BatchExecHashWindowAggregate(
      tublingGroupWindow,
      2,
      tsType,
      namedPropertiesOfWindowAgg,
      cluster,
      relBuilder,
      exchange2.getTraitSet,
      exchange2,
      aggCallToAggFunction,
      flinkLogicalWindowAgg.getRowType,
      exchange2.getRowType,
      Array(0, 1),
      Array(),
      false,
      false)
    (localAgg, globalAgg, globalAggWithoutLocal)
  }

  // equivalent SQL is
  // select a, c, count(b) as s, TUMBLE_START(rowtime, INTERVAL '15' MINUTE) as ping_start from
  // temporalTable1 group by a, c, TUMBLE(rowtime, INTERVAL '15' MINUTE)
  protected lazy val (
    localWindowAggWithAuxGrouping,
    globalWindowAggWithLocalAggWithAuxGrouping,
    globalWindowAggWithoutLocalAggWithAuxGrouping) = {
    val scanOfTemporalTable: BatchExecTableSourceScan =
      createTableSourceScanBatchExec(ImmutableList.of("temporalTable1"))
    relBuilder.push(scanOfTemporalTable)
    val projects = relBuilder.fields(Seq[Integer](0, 2, 4, 1).toList)
    val project = relBuilder.project(projects).build()
    val outputRowType = project.getRowType
    val program = RexProgram.create(
      scanOfTemporalTable.getRowType,
      projects,
      null,
      outputRowType,
      relBuilder.getRexBuilder)
    val calc = new BatchExecCalc(cluster, batchExecTraits,
      scanOfTemporalTable, outputRowType, program, "")
    val aggCallsWithoutAuxGroup = Lists.newArrayList(
      AggregateCall.create(new SqlCountAggFunction("COUNT"), false, false,
        List[Integer](3), -1, 1, project, null, "s"))

    val (_, _, aggregates) =
      AggregateUtil.transformToBatchAggregateFunctions(aggCallsWithoutAuxGroup, calc.getRowType)
    val aggCallToAggFunction = aggCallsWithoutAuxGroup.zip(aggregates)
    val tsType = typeFactory.createTypeFromTypeInfo(SqlTimeTypeInfo.TIMESTAMP, isNullable = false)
    val localAggRowType = typeFactory.builder
      .add("a", typeFactory.createTypeFromTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, isNullable = true))
      .add("assignedWindow$", tsType)
      .add("c", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.INT_TYPE_INFO, isNullable = true))
      .add("count$0", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.LONG_TYPE_INFO, isNullable = true))
      .build()
    val localAggWithAuxGrouping = new BatchExecLocalHashWindowAggregate(
      tublingGroupWindow,
      2,
      tsType,
      namedPropertiesOfWindowAgg,
      cluster,
      relBuilder,
      batchExecTraits,
      calc,
      aggCallToAggFunction,
      localAggRowType,
      calc.getRowType,
      Array(0),
      Array(1))
    val toTrait = localAggWithAuxGrouping.getTraitSet.replace(
      FlinkRelDistribution.hash(ImmutableList.of(Integer.valueOf(0)), requireStrict = false))
    val exchange = new BatchExecExchange(
      cluster,
      toTrait,
      localAggWithAuxGrouping,
      FlinkRelDistribution.hash(ImmutableList.of(Integer.valueOf(0)), requireStrict = false))
    val globalAggWithAuxGrouping = new BatchExecHashWindowAggregate(
      tublingGroupWindow,
      2,
      tsType,
      namedPropertiesOfWindowAgg,
      cluster,
      relBuilder,
      exchange.getTraitSet,
      exchange,
      aggCallToAggFunction,
      flinkLogicalWindowAggWithAuxGroup.getRowType,
      exchange.getRowType,
      Array(0),
      Array(2),
      false,
      true)
    val exchange2 = new BatchExecExchange(
      cluster,
      toTrait,
      calc,
      FlinkRelDistribution.hash(ImmutableList.of(Integer.valueOf(0)), requireStrict = false))
    val globalAggWithoutLocalWithAuxGrouping = new BatchExecHashWindowAggregate(
      tublingGroupWindow,
      2,
      tsType,
      namedPropertiesOfWindowAgg,
      cluster,
      relBuilder,
      exchange2.getTraitSet,
      exchange2,
      aggCallToAggFunction,
      flinkLogicalWindowAggWithAuxGroup.getRowType,
      exchange2.getRowType,
      Array(0),
      Array(1),
      false,
      false)
    (localAggWithAuxGrouping, globalAggWithAuxGrouping, globalAggWithoutLocalWithAuxGrouping)
  }

  protected lazy val logicalWindowAggOnBigTimeTable: BatchExecLocalHashWindowAggregate = {
    val scanOfBigTemporalTable: BatchExecTableSourceScan =
      createTableSourceScanBatchExec(ImmutableList.of("bigTemporalTable"))
    relBuilder.push(scanOfBigTemporalTable)
    val projects = relBuilder.fields(Seq[Integer](0, 1, 4, 2).toList)
    val project = relBuilder.project(projects).build()
    val outputRowType = project.getRowType
    val program = RexProgram.create(
      scanOfBigTemporalTable.getRowType,
      projects,
      null,
      outputRowType,
      relBuilder.getRexBuilder)
    val calc = new BatchExecCalc(cluster, batchExecTraits,
      scanOfBigTemporalTable, outputRowType, program, "")
    val (_, _, aggregates) =
      AggregateUtil.transformToBatchAggregateFunctions(
        flinkLogicalWindowAgg.getAggCallList, calc.getRowType)
    val aggCallToAggFunction = flinkLogicalWindowAgg.getAggCallList.zip(aggregates)
    val tsType = typeFactory.createTypeFromTypeInfo(SqlTimeTypeInfo.TIMESTAMP, isNullable = false)
    val localAggRowType = typeFactory.builder
      .add("a", typeFactory.createTypeFromTypeInfo(BasicTypeInfo.INT_TYPE_INFO, isNullable = true))
      .add("b", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.STRING_TYPE_INFO, isNullable = true))
      .add("assignedWindow$", tsType)
      .add("count$0", typeFactory.createTypeFromTypeInfo(
        BasicTypeInfo.LONG_TYPE_INFO, isNullable = true))
      .build()
    new BatchExecLocalHashWindowAggregate(
      tublingGroupWindow,
      2,
      tsType,
      namedPropertiesOfWindowAgg,
      cluster,
      relBuilder,
      batchExecTraits,
      calc,
      aggCallToAggFunction,
      localAggRowType,
      calc.getRowType,
      Array(0, 1),
      Array())
  }

  // equivalent SQL is
  // select * from (
  //  select id, score, age, height, RANK() over (partition by age order by score) rk from student
  // ) t where rk <= 5
  protected lazy val flinkLogicalRank: FlinkLogicalRank = {
    val scan = relBuilder.scan("student").build()

    new FlinkLogicalRank(
      cluster,
      logicalTraits,
      scan,
      SqlStdOperatorTable.RANK,
      ImmutableBitSet.of(2),
      RelCollations.of(1),
      ConstantRankRange(1, 5),
      outputRankFunColumn = true
    )
  }

  // equivalent SQL is
  // select * from (
  //  select id, score, age, height, RANK() over (partition by age order by score) rk from student
  // ) t where rk <= height
  protected lazy val flinkLogicalRankWithVariableRankRange: FlinkLogicalRank = {
    val scan = relBuilder.scan("student").build()

    new FlinkLogicalRank(
      cluster,
      logicalTraits,
      scan,
      SqlStdOperatorTable.RANK,
      ImmutableBitSet.of(2),
      RelCollations.of(1),
      VariableRankRange(3),
      outputRankFunColumn = true
    )
  }

  // equivalent SQL is
  // select * from student order by score limit 3
  protected lazy val flinkLogicalRowNumber: FlinkLogicalRank = {
    val scan = relBuilder.scan("student").build()
    new FlinkLogicalRank(
      cluster,
      logicalTraits,
      scan,
      SqlStdOperatorTable.ROW_NUMBER,
      ImmutableBitSet.of(),
      RelCollations.of(1),
      ConstantRankRange(1, 3),
      outputRankFunColumn = false
    )
  }

  // equivalent SQL is
  // select * from (
  //  select id, score, age, height,
  //   ROW_NUMBER() over (partition by age order by score) rk from student
  // ) t where rn <= 3
  protected lazy val flinkLogicalRowNumberWithOutput: FlinkLogicalRank = {
    val scan = relBuilder.scan("student").build()
    new FlinkLogicalRank(
      cluster,
      logicalTraits,
      scan,
      SqlStdOperatorTable.ROW_NUMBER,
      ImmutableBitSet.of(2),
      RelCollations.of(1),
      ConstantRankRange(1, 3),
      outputRankFunColumn = true
    )
  }

  // equivalent SQL is
  // select * from (
  //  select id, score, age, height, RANK() over (partition by age order by score) rk from student
  // ) t where rk <= 5 and rk >= 3
  protected lazy val (globalBatchExecRank, localBatchExecRank) = {
    val scan = createTableSourceScanBatchExec(ImmutableList.of("student"))
    val localBatchExecRank = new BatchExecRank(
      cluster,
      batchExecTraits,
      scan,
      SqlStdOperatorTable.RANK,
      ImmutableBitSet.of(2),
      RelCollations.of(1),
      ConstantRankRange(1, 5),
      outputRankFunColumn = false,
      isGlobal = false
    )

    val globalBatchExecRank = new BatchExecRank(
      cluster,
      batchExecTraits,
      scan,
      SqlStdOperatorTable.RANK,
      ImmutableBitSet.of(2),
      RelCollations.of(1),
      ConstantRankRange(3, 5),
      outputRankFunColumn = true,
      isGlobal = true
    )
    (globalBatchExecRank, localBatchExecRank)
  }

  // equivalent SQL is
  // select * from student order by score limit 3
  protected lazy val streamExecRowNumber: StreamExecRank = {
    val scan = createTableSourceScanStreamExec(ImmutableList.of("student"))
    new StreamExecRank(
      cluster,
      batchExecTraits,
      scan,
      new BaseRowSchema(scan.getRowType),
      new BaseRowSchema(scan.getRowType),
      SqlStdOperatorTable.ROW_NUMBER,
      Array(),
      RelCollations.of(1),
      ConstantRankRange(1, 3),
      outputRankFunColumn = false
    )
  }

  private lazy val valuesType = relBuilder.getTypeFactory.builder()
    .add("col1", SqlTypeName.BIGINT)
    .add("col2", SqlTypeName.BOOLEAN)
    .add("col3", SqlTypeName.DATE)
    .add("col4", SqlTypeName.TIME)
    .add("col5", SqlTypeName.TIMESTAMP)
    .add("col6", SqlTypeName.DOUBLE)
    .add("col7", SqlTypeName.FLOAT)
    .build()

  protected lazy val emptyValues: RelNode = {
    relBuilder.values(valuesType)
    relBuilder.build()
  }

  protected lazy val values: RelNode = {
    relBuilder.values(
      List(
        List("1", "true", "2017-10-01", "10:00:00", "2017-10-01 00:00:00", "2.12", ""),
        List("", "false", "2017-09-01", "10:00:01", "", "3.12", ""),
        List("3", "true", "", "10:00:02", "2017-10-01 01:00:00", "3.0", ""),
        List("2", "true", "2017-10-02", "09:59:59", "2017-07-01 01:00:00", "-1", "")
      ).map(createLiteralList(valuesType, _)),
      valuesType)
    relBuilder.build()
  }

  private def createLiteralList(
      rowType: RelDataType,
      literalValues: Seq[String]): JList[RexLiteral] = {
    val rexBuilder = relBuilder.getRexBuilder
    require(literalValues.length == rowType.getFieldCount)
    literalValues.zipWithIndex.map {
      case (v, index) =>
        val fieldType = rowType.getFieldList.get(index).getType
        if (v.isEmpty) {
          rexBuilder.makeNullLiteral(fieldType)
        } else {
          fieldType.getSqlTypeName match {
            case BIGINT =>
              rexBuilder.makeLiteral(v.toLong, fieldType, true).asInstanceOf[RexLiteral]
            case BOOLEAN => rexBuilder.makeLiteral(v.toBoolean)
            case DATE => rexBuilder.makeDateLiteral(new DateString(v))
            case TIME => rexBuilder.makeTimeLiteral(new TimeString(v), 0)
            case TIMESTAMP => rexBuilder.makeTimestampLiteral(new TimestampString(v), 0)
            case DOUBLE => rexBuilder.makeApproxLiteral(BigDecimal.valueOf(v.toDouble))
            case FLOAT => rexBuilder.makeApproxLiteral(BigDecimal.valueOf(v.toFloat))
            case _ =>
              throw new IllegalArgumentException(s"${fieldType.getSqlTypeName} is not supported!")
          }
        }
    }.toList
  }

  protected def buildCalc(
      input: RelNode,
      outputRowType: RelDataType,
      projects: List[RexNode],
      conditions: List[RexNode]): Calc = {
    val rexBuilder = relBuilder.getRexBuilder
    val predicate = RexUtil.composeConjunction(rexBuilder, conditions, true)
    val program = RexProgram.create(
      input.getRowType,
      projects,
      predicate,
      outputRowType,
      rexBuilder)
    LogicalCalc.create(input, program)
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
