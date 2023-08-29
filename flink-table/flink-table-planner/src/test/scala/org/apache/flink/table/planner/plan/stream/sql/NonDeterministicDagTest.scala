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
package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.api.config.OptimizerConfigOptions.NonDeterministicUpdateStrategy
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.JBoolean
import org.apache.flink.table.planner.expressions.utils.{TestNonDeterministicUdaf, TestNonDeterministicUdf, TestNonDeterministicUdtf}
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.StringSplit
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.sinks.UpsertStreamTableSink
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{BigIntType, IntType, VarCharType}
import org.apache.flink.table.types.utils.TypeConversions

import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.util

@RunWith(classOf[Parameterized])
class NonDeterministicDagTest(nonDeterministicUpdateStrategy: NonDeterministicUpdateStrategy)
  extends TableTestBase {

  private val util: StreamTableTestUtil = streamTestUtil()
  private val tryResolve =
    nonDeterministicUpdateStrategy == NonDeterministicUpdateStrategy.TRY_RESOLVE

  @Before
  def before(): Unit = {
    util.tableConfig.getConfiguration.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY,
      nonDeterministicUpdateStrategy)

    // for json plan test
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, Int.box(4))

    util.addTableSource[(Int, Long, String, Boolean)]("T", 'a, 'b, 'c, 'd)
    util.addDataStream[(Int, String, Long)]("T1", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

    util.tableEnv.executeSql("""
                               |create temporary table src (
                               | a int,
                               | b bigint,
                               | c string,
                               | d bigint
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table cdc (
                               | a int,
                               | b bigint,
                               | c string,
                               | d bigint,
                               | primary key (a) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table upsert_src (
                               | a int,
                               | b bigint,
                               | c string,
                               | d boolean,
                               | primary key (a) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,D'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table cdc_with_computed_col (
                               |  a int,
                               |  b bigint,
                               |  c string,
                               |  d int,
                               |  `day` as DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd'),
                               |  primary key(a, c) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D'
                               |)""".stripMargin)
    util.tableEnv.executeSql(
      """
        |create temporary table cdc_with_meta (
        | a int,
        | b bigint,
        | c string,
        | d boolean,
        | metadata_1 int metadata,
        | metadata_2 string metadata,
        | metadata_3 bigint metadata,
        | primary key (a) not enforced
        |) with (
        | 'connector' = 'values',
        | 'changelog-mode' = 'I,UA,UB,D',
        | 'readable-metadata' = 'metadata_1:INT, metadata_2:STRING, metadata_3:BIGINT'
        |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table cdc_with_watermark (
                               | a int,
                               | b bigint,
                               | c string,
                               | d boolean,
                               | op_ts timestamp_ltz(3),
                               | primary key (a) not enforced,
                               | watermark for op_ts as op_ts - interval '5' second
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D',
                               | 'readable-metadata' = 'op_ts:timestamp_ltz(3)'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table cdc_with_meta_and_wm (
                               | a int,
                               | b bigint,
                               | c string,
                               | d boolean,
                               | op_ts timestamp_ltz(3) metadata,
                               | primary key (a) not enforced,
                               | watermark for op_ts as op_ts - interval '5' second
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D',
                               | 'readable-metadata' = 'op_ts:timestamp_ltz(3)'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table sink_with_composite_pk (
                               | a int,
                               | b bigint,
                               | c string,
                               | d bigint,
                               | primary key (a,d) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table sink_with_pk (
                               | a int,
                               | b bigint,
                               | c string,
                               | primary key (a) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table sink_without_pk (
                               | a int,
                               | b bigint,
                               | c string
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table dim_with_pk (
                               | a int,
                               | b bigint,
                               | c string,
                               | primary key (a) not enforced
                               |) with (
                               | 'connector' = 'values'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table dim_without_pk (
                               | a int,
                               | b bigint,
                               | c string
                               |) with (
                               | 'connector' = 'values'
                               |)""".stripMargin)

    // custom ND function
    util.tableEnv.createTemporaryFunction("ndFunc", new TestNonDeterministicUdf)
    util.tableEnv.createTemporaryFunction("ndTableFunc", new TestNonDeterministicUdtf)
    util.tableEnv.createTemporaryFunction("ndAggFunc", new TestNonDeterministicUdaf)
    // deterministic table function
    util.tableEnv.createTemporaryFunction("str_split", new StringSplit())
  }

  @Test
  def testCdcWithMetaSinkWithPk(): Unit = {
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select a, metadata_3, c
                                 |from cdc_with_meta
                                 |""".stripMargin)
  }
  @Test
  def testNonDeterministicProjectionWithSinkWithoutPk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "The column(s): d(generated by non-deterministic function: ndFunc ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert("""
                                |insert into sink_without_pk
                                |select
                                |  a,
                                |  if(a > 100, b+d, b) as b,
                                |  case when d > 100 then json_value(c, '$.count')
                                |  else cast(b as string) || '#' end as c
                                |from (
                                |select a, b, c, d from (
                                |  select *, row_number() over(partition by a order by d desc) as rn
                                |  from (
                                |    select a, d as b, c, ndFunc(b) as d from cdc
                                |  ) tmp
                                |) tmp where rn = 1) tmp
                                |""".stripMargin)
  }

  @Test
  def testCdcWithMetaSinkWithoutPk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select a, metadata_3, c
                                 |from cdc_with_meta
                                 |""".stripMargin)
  }

  @Test
  def testCdcWithMetaLegacySinkWithPk(): Unit = {
    val sinkWithPk = new TestingUpsertSink(
      Array("a"),
      Array("a", "b", "c"),
      // pk column requires non-null type
      Array(DataTypes.INT().notNull(), DataTypes.BIGINT(), DataTypes.VARCHAR(100)))

    util.tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .registerTableSinkInternal("legacy_upsert_sink", sinkWithPk)

    util.verifyExecPlanInsert(s"""
                                 |insert into legacy_upsert_sink
                                 |select a, metadata_3, c
                                 |from cdc_with_meta
                                 |""".stripMargin)
  }

  @Test
  def testCdcWithMetaLegacySinkWithoutPk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
      thrown.expect(classOf[TableException])
    }
    val retractSink =
      util.createRetractTableSink(
        Array("a", "b", "c"),
        Array(new IntType(), new BigIntType(), VarCharType.STRING_TYPE))
    util.tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .registerTableSinkInternal("legacy_retract_sink", retractSink)

    util.verifyExecPlanInsert(s"""
                                 |insert into legacy_retract_sink
                                 |select a, metadata_3, c
                                 |from cdc_with_meta
                                 |""".stripMargin)
  }

  @Test
  def testCdcWithMetaSinkWithCompositePk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_composite_pk
                                 |select a, b, c, metadata_3
                                 |from cdc_with_meta
                                 |""".stripMargin)
  }

  @Test
  def testCdcWithMetaRenameSinkWithCompositePk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
      thrown.expect(classOf[TableException])
    }
    util.tableEnv.executeSql("""
                               |create temporary table cdc_with_meta_rename (
                               | a int,
                               | b bigint,
                               | c string,
                               | d boolean,
                               | metadata_3 bigint metadata,
                               | e as metadata_3,
                               | primary key (a) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D',
                               | 'readable-metadata' = 'metadata_3:BIGINT'
                               |)""".stripMargin)

    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_composite_pk
                                 |select a, b, c, e from cdc_with_meta_rename
                                 |""".stripMargin)
  }

  @Test
  def testSourceWithComputedColumnSinkWithPk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }

    // can not infer pk from cdc source with computed column(s)
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select a, b, `day`
                                 |from cdc_with_computed_col
                                 |where b > 100
                                 |""".stripMargin)
  }

  @Test
  def testSourceWithComputedColumnMultiSink(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(s"""
                            |insert into sink_without_pk
                            |select a, sum(b), `day`
                            |from cdc_with_computed_col
                            |group by a, `day`
                            |""".stripMargin)
    stmtSet.addInsertSql(s"""
                            |insert into sink_with_pk
                            |select a, b, `day`
                            |from cdc_with_computed_col
                            |where b > 100
                            |""".stripMargin)
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testCdcCorrelateNonDeterministicFuncSinkWithPK(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): EXPR$0(generated by non-deterministic function: ndTableFunc ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select
                                 |  t1.a, t1.b, a1
                                 |from cdc t1, lateral table(ndTableFunc(a)) as T(a1)
                                 |""".stripMargin)
  }

  @Test
  def testCdcCorrelateNonDeterministicFuncNoLeftOutput(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): EXPR$0(generated by non-deterministic function: ndTableFunc ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk(a)
                                 |select
                                 |  cast(a1 as integer) a
                                 |from cdc t1, lateral table(ndTableFunc(a)) as T(a1)
                                 |""".stripMargin)
  }

  @Test
  def testCdcCorrelateNonDeterministicFuncNoRightOutput(): Unit = {
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select a, b, c
                                 |from cdc t1 join lateral table(ndTableFunc(a)) as T(a1) on true
                                 |""".stripMargin)
  }

  @Test
  def testCdcCorrelateOnNonDeterministicCondition(): Unit = {
    // TODO update this after FLINK-7865 was fixed
    thrown.expectMessage("unexpected correlate variable $cor0 in the plan")
    thrown.expect(classOf[TableException])
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select a, b, c
                                 |from cdc t1 join lateral table(str_split(c)) as T(c1)
                                 | -- the join predicate can only be empty or literal true for now
                                 |  on ndFunc(b) > 100
                                 |""".stripMargin)
  }

  @Test
  def testCdcWithMetaCorrelateSinkWithPk(): Unit = {
    // Under ignore mode, the generated execution plan may cause wrong result though
    // upsertMaterialize has been enabled in sink, because
    if (tryResolve) {
      thrown.expectMessage(
        "metadata column(s): 'metadata_1' in cdc source may cause wrong result or error on downstream operators")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select t1.a, t1.metadata_1, T.c1
                                 |from cdc_with_meta t1, lateral table(str_split(c)) as T(c1)
                                 |""".stripMargin)
  }

  @Test
  def testCdcWithNonDeterministicFuncSinkWithPk(): Unit = {
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select a, ndFunc(b), c
                                 |from cdc
                                 |""".stripMargin)
  }

  @Test
  def testCdcWithNonDeterministicFuncSinkWithoutPk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): EXPR$1(generated by non-deterministic function: ndFunc ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select a, ndFunc(b), c
                                 |from cdc
                                 |""".stripMargin)
  }

  @Test
  def testCdcWithNonDeterministicFilter(): Unit = {
    // TODO should throw error if tryResolve is true after FLINK-28737 was fixed
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select t1.a, t1.b, t1.c
                                 |from cdc t1
                                 |where t1.b > UNIX_TIMESTAMP() - 300
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinDimWithPkSinkWithPk(): Unit = {
    // The lookup key contains the dim table's pk, there will be no materialization.
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select t1.a, t1.b, t2.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinDimWithoutPkSinkWithPk(): Unit = {
    // This case shows how costly is if the dim table does not define a pk.
    // The lookup key doesn't contain the dim table's pk, there will be two more costly
    // materialization compare to the one with pk.
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select t1.a, t1.b, t2.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_without_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a
                                 |""".stripMargin)
  }

  @Test
  def testCdcLeftJoinDimWithPkSinkWithPk(): Unit = {
    // The lookup key contains the dim table's pk, there will be no materialization.
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select t1.a, t1.b, t2.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 left join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinDimWithPkSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select t1.a, t1.b, t2.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinDimWithoutPkSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select t1.a, t1.b, t2.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_without_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinDimWithPkOnlySinkWithoutPk(): Unit = {
    // only select lookup key field, expect not affect NDU
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select t1.a, t1.b, t1.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a
                                 |""".stripMargin)
  }

  @Test
  def testCdcLeftJoinDimWithoutPkSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert(
      s"""
         |insert into sink_without_pk
         |select t1.a, t1.b, t2.c
         |from (
         |  select *, proctime() proctime from cdc
         |) t1 left join dim_without_pk for system_time as of t1.proctime as t2
         |on t1.a = t2.a
         |""".stripMargin)
  }

  @Test
  def testCdcJoinDimWithPkOutputNoPkSinkWithoutPk(): Unit = {
    // non lookup pk selected, expect materialize if tryResolve
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select t1.a, t2.b, t1.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinDimWithPkNonDeterministicFuncSinkWithoutPk(): Unit = {
    if (tryResolve) {
      // only select lookup key field, but with ND-call, expect exception
      thrown.expectMessage(
        "column(s): a(generated by non-deterministic function: ndFunc ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select ndFunc(t2.a) a, t1.b, t1.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinDimWithPkNonDeterministicLocalCondition(): Unit = {
    // use user defined function
    if (tryResolve) {
      // not select lookup source field, but with NonDeterministicCondition, expect exception
      thrown.expectMessage(
        "exists non deterministic function: 'ndFunc' in condition: '>(ndFunc($1), 100)' which may cause wrong result")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select t1.a, t1.b, t1.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a and ndFunc(t2.b) > 100
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinDimWithPkNonDeterministicLocalCondition2(): Unit = {
    // use builtin temporal function
    if (tryResolve) {
      thrown.expectMessage(
        "exists non deterministic function: 'UNIX_TIMESTAMP' in condition: '>($1, -(UNIX_TIMESTAMP(), 300))' which may cause wrong result")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select t1.a, t2.b as version, t2.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a
                                 |  -- check dim table data's freshness
                                 |  and t2.b > UNIX_TIMESTAMP() - 300
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinDimNonDeterministicRemainingCondition(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "exists non deterministic function: 'ndFunc' in condition: '>($1, ndFunc($3))' which may cause wrong result")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select t1.a, t2.b, t2.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a
                                 |  -- non deterministic function in remaining condition
                                 |  and t1.b > ndFunc(t2.b)
                                 |""".stripMargin)
  }

  @Test
  def testCdcLeftJoinDimWithNonDeterministicPreFilter(): Unit = {
    // use builtin temporal function
    if (tryResolve) {
      thrown.expectMessage(
        "exists non deterministic function: 'UNIX_TIMESTAMP' in condition: '>($1, -(UNIX_TIMESTAMP(), 300))' which may cause wrong result")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select t1.a, t2.b as version, t2.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 left join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a
                                 |  and t1.b > UNIX_TIMESTAMP() - 300
                                 |""".stripMargin)
  }

  @Test
  def testGroupByNonDeterministicFuncWithCdcSource(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select
                                 |  a, count(*) cnt, `day`
                                 |from (
                                 |  select *, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') `day` from cdc
                                 |) t
                                 |group by `day`, a
                                 |""".stripMargin)
  }

  @Test
  def testGroupByNonDeterministicUdfWithCdcSource(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): EXPR$0(generated by non-deterministic function: ndFunc ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select
                                 |  ndFunc(a), count(*) cnt, c
                                 |from cdc
                                 |group by ndFunc(a), c
                                 |""".stripMargin)
  }

  @Test
  def testNestedAggWithNonDeterministicGroupingKeys(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(
      s"""
         |insert into sink_with_pk
         |select
         |  a, sum(b) qmt, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') `day`
         |from (
         |  select *, row_number() over (partition by a order by PROCTIME() desc) rn from src
         |) t
         |where rn = 1
         |group by a, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')
         |""".stripMargin)
  }

  @Test
  def testGroupAggNonDeterministicFuncOnSourcePk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlan(
      s"""
         |select
         |  `day`, count(*) cnt, sum(b) qmt
         |from (
         |  select *, concat(cast(a as varchar), DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) `day` from cdc
         |) t
         |group by `day`
         |""".stripMargin)
  }

  @Test
  def testAggWithNonDeterministicFilterArgs(): Unit = {
    util.verifyExecPlanInsert(
      s"""
         |insert into sink_with_pk
         |select
         |  a
         |  ,count(*) cnt
         |  ,cast(count(distinct c) filter (where b > UNIX_TIMESTAMP() - 180) as varchar) valid_uv
         |from T
         |group by a
         |""".stripMargin)
  }

  @Test
  def testAggWithNonDeterministicFilterArgsOnCdcSource(): Unit = {
    if (tryResolve) {
      // though original pk was selected and same as the sink's pk, but the valid_uv was
      // non-deterministic, will raise an error
      thrown.expectMessage(
        "column(s): $f2(generated by non-deterministic function: UNIX_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(
      s"""
         |insert into sink_with_pk
         |select
         |  a
         |  ,count(*) cnt
         |  ,cast(count(distinct c) filter (where b > UNIX_TIMESTAMP() - 180) as varchar) valid_uv
         |from cdc
         |group by a
         |""".stripMargin)
  }

  @Test
  def testAggWithNonDeterministicFilterArgsOnCdcSourceSinkWithoutPk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): $f2(generated by non-deterministic function: UNIX_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(
      s"""
         |insert into sink_without_pk
         |select
         |  a
         |  ,count(*) cnt
         |  ,cast(count(distinct c) filter (where b > UNIX_TIMESTAMP() - 180) as varchar) valid_uv
         |from cdc
         |group by a
         |""".stripMargin)
  }

  @Test
  def testNonDeterministicAggOnAppendSourceSinkWithPk(): Unit = {
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select
                                 |  a
                                 |  ,ndAggFunc(b) ndCnt
                                 |  ,max(c) mc
                                 |from T
                                 |group by a
                                 |""".stripMargin)
  }

  @Test
  def testNonDeterministicAggOnAppendSourceSinkWithoutPk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): ndCnt(generated by non-deterministic function: ndAggFunc ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select
                                 |  a
                                 |  ,ndAggFunc(b) ndCnt
                                 |  ,max(c) mc
                                 |from T
                                 |group by a
                                 |""".stripMargin)
  }

  @Test
  def testGlobalNonDeterministicAggOnAppendSourceSinkWithPk(): Unit = {
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select
                                 |  max(a)
                                 |  ,ndAggFunc(b) ndCnt
                                 |  ,max(c) mc
                                 |from T
                                 |""".stripMargin)
  }

  @Test
  def testGlobalNonDeterministicAggOnAppendSourceSinkWithoutPk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): ndCnt(generated by non-deterministic function: ndAggFunc ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select
                                 |  max(a)
                                 |  ,ndAggFunc(b) ndCnt
                                 |  ,max(c) mc
                                 |from T
                                 |""".stripMargin)
  }

  @Test
  def testUpsertSourceSinkWithPk(): Unit = {
    // contains normalize
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select a, b, c
                                 |from upsert_src
                                 |""".stripMargin)
  }

  @Test
  def testUpsertSourceSinkWithoutPk(): Unit = {
    // contains normalize
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select a, b, c
                                 |from upsert_src
                                 |""".stripMargin)
  }

  @Test
  def testMultiOverWithNonDeterministicUdafSinkWithPk(): Unit = {
    util.verifyExecPlanInsert(
      """
        |insert into sink_with_composite_pk
        |SELECT
        |  a
        |  ,COUNT(distinct b)  OVER (PARTITION BY a ORDER BY proctime
        |    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) uv
        |  ,b
        |  ,ndAggFunc(a) OVER (PARTITION BY a ORDER BY proctime
        |    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) nd
        |FROM T1
      """.stripMargin
    )
  }

  @Test
  def testOverWithNonDeterministicUdafSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert(
      """
        |insert into sink_without_pk
        |SELECT
        |  a
        |  ,ndAggFunc(a) OVER (PARTITION BY a ORDER BY proctime
        |    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
        |  ,b
        |FROM T1
      """.stripMargin
    )
  }

  @Test
  def testMultiOverWithNonDeterministicAggFilterSinkWithPk(): Unit = {
    // agg with filter is not supported currently, should update this after it is supported.
    thrown.expectMessage("OVER must be applied to aggregate function")
    thrown.expect(classOf[ValidationException])
    util.verifyExecPlanInsert(
      """
        |insert into sink_with_composite_pk
        |SELECT
        |  a
        |  ,COUNT(distinct b) OVER (PARTITION BY a ORDER BY proctime
        |    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) uv
        |  ,b
        |  ,SUM(a) filter (where b > UNIX_TIMESTAMP() - 180) OVER (PARTITION BY a ORDER BY proctime
        |    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) nd
        |FROM T1
      """.stripMargin
    )
  }

  @Test
  def testAppendRankOnMultiOverWithNonDeterministicUdafSinkWithPk(): Unit = {
    util.verifyExecPlanInsert(
      """
        |insert into sink_with_composite_pk
        |select a, uv, b, nd from (
        | select
        |  a, uv, b, nd,
        |  row_number() over (partition by a order by uv desc) rn
        | from (
        |  SELECT
        |    a
        |    ,COUNT(distinct b)  OVER (PARTITION BY a ORDER BY proctime
        |      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) uv
        |    ,b
        |    ,ndAggFunc(a) OVER (PARTITION BY a ORDER BY proctime
        |      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) nd
        |  FROM T1
        |  )
        |) where rn = 1
      """.stripMargin
    )
  }

  @Test
  def testAppendRankOnMultiOverWithNonDeterministicUdafSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert(
      """
        |insert into sink_without_pk
        |select a, nd, b from (
        | select
        |  a, uv, b, nd,
        |  row_number() over (partition by a order by uv desc) rn
        | from (
        |  SELECT
        |    a
        |    ,COUNT(distinct b)  OVER (PARTITION BY a ORDER BY proctime
        |      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) uv
        |    ,b
        |    ,ndAggFunc(a) OVER (PARTITION BY a ORDER BY proctime
        |      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) nd
        |  FROM T1
        |  )
        |) where rn = 1
      """.stripMargin
    )
  }

  @Test
  def testUpdateRankOutputRowNumberSinkWithPk(): Unit = {
    util.tableEnv.executeSql(s"""
                                | create temporary view v1 as
                                |  select a, max(c) c, sum(b) filter (where b > 0) cnt
                                |  from src
                                |  group by a
                                | """.stripMargin)

    util.verifyExecPlanInsert(
      s"""
         |insert into sink_with_composite_pk
         |select a, cnt, c, rn from (
         | select
         |  a, cnt, c, row_number() over (partition by a order by cnt desc) rn
         | from v1
         | ) t where t.rn <= 100
         |""".stripMargin)
  }

  @Test
  def testRetractRankOutputRowNumberSinkWithPk(): Unit = {
    util.tableEnv.executeSql(s"""
                                | create temporary view v1 as
                                |  select a, max(c) c, sum(b) cnt
                                |  from src
                                |  group by a
                                | """.stripMargin)

    util.verifyExecPlanInsert(
      s"""
         |insert into sink_with_composite_pk
         |select a, cnt, c, rn from (
         | select
         |  a, cnt, c, row_number() over (partition by a order by cnt desc) rn
         | from v1
         | ) t where t.rn <= 100
         |""".stripMargin)
  }

  @Test
  def testUnionSinkWithCompositePk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_composite_pk
                                 |select a, b, c, d
                                 |from src
                                 |union
                                 |select a, b, c, metadata_3
                                 |from cdc_with_meta
                                 |""".stripMargin)
  }

  @Test
  def testUnionAllSinkWithCompositePk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_composite_pk
                                 |select a, b, c, d
                                 |from src
                                 |union all
                                 |select a, b, c, metadata_3
                                 |from cdc_with_meta
                                 |""".stripMargin)
  }

  @Test
  def testUnionAllSinkWithoutPk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select a, b, c
                                 |from src
                                 |union all
                                 |select a, metadata_3, c
                                 |from cdc_with_meta
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinWithNonDeterministicCondition(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): $f4(generated by non-deterministic function: ndFunc ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select
                                 |  t1.a
                                 |  ,t2.b
                                 |  ,t1.c
                                 |from cdc t1 join cdc t2
                                 |  on ndFunc(t1.b) = ndFunc(t2.b)
                                 |""".stripMargin)
  }

  @Test
  def testProctimeIntervalJoinSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert("""
                                |insert into sink_without_pk
                                |SELECT t2.a, t2.c, t1.b FROM T1 t1 JOIN T1 t2 ON
                                |  t1.a = t2.a AND t1.proctime > t2.proctime - INTERVAL '5' SECOND
      """.stripMargin)
  }

  @Test
  def testCdcProctimeIntervalJoinOnPkSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert("""
                                |insert into sink_without_pk
                                |SELECT t2.a, t2.b, t1.c FROM (
                                | select *, proctime() proctime from cdc) t1 JOIN
                                | (select *, proctime() proctime from cdc) t2 ON
                                |  t1.a = t2.a AND t1.proctime > t2.proctime - INTERVAL '5' SECOND
      """.stripMargin)
  }

  @Test
  def testCdcProctimeIntervalJoinOnNonPkSinkWithoutPk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage("can not satisfy the determinism requirement")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert("""
                                |insert into sink_without_pk
                                |SELECT t2.a, t2.b, t1.c FROM (
                                | select *, proctime() proctime from cdc) t1 JOIN
                                | (select *, proctime() proctime from cdc) t2 ON
                                |  t1.b = t2.b AND t1.proctime > t2.proctime - INTERVAL '5' SECOND
      """.stripMargin)
  }

  @Test
  def testCdcRowtimeIntervalJoinSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert(
      """
        |insert into sink_without_pk
        |SELECT t2.a, t1.b, t2.c FROM cdc_with_watermark t1 JOIN cdc_with_watermark t2 ON
        |  t1.a = t2.a AND t1.op_ts > t2.op_ts - INTERVAL '5' SECOND
      """.stripMargin)
  }

  @Test
  def testCdcRowtimeIntervalJoinSinkWithPk(): Unit = {
    util.verifyExecPlanInsert(
      """
        |insert into sink_with_pk
        |SELECT t2.a, t1.b, t2.c FROM cdc_with_watermark t1 JOIN cdc_with_watermark t2 ON
        |  t1.a = t2.a AND t1.op_ts > t2.op_ts - INTERVAL '5' SECOND
      """.stripMargin)
  }

  @Test
  def testJoinKeyContainsUk(): Unit = {
    util.verifyExecPlan(
      s"""
         |select t1.a, t2.`c-day`, t2.b, t2.d
         |from (
         |  select a, b, c, d
         |  from cdc
         | ) t1
         |join (
         |  select a, b, CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `c-day`, d
         |  from cdc
         |) t2
         |  on t1.a = t2.a
         |""".stripMargin)
  }

  @Test
  def testJoinHasBothSidesUk(): Unit = {
    util.verifyExecPlan(
      s"""
         |select t1.a, t2.a, t2.`c-day`, t2.b, t2.d
         |from (
         |  select a, b, c, d
         |  from cdc
         | ) t1
         |join (
         |  select a, b, CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `c-day`, d
         |  from cdc
         |) t2
         |  on t1.b = t2.b
         |""".stripMargin)
  }

  @Test
  def testJoinHasBothSidesUkSinkWithoutPk(): Unit = {
    if (tryResolve) {
      // sink require all columns be deterministic though join has both side uk
      thrown.expectMessage(
        "column(s): c-day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(
      s"""
         |insert into sink_with_pk
         |select t1.a, t2.a, t2.`c-day`
         |from (
         |  select a, b, c, d
         |  from cdc
         | ) t1
         |join (
         |  select a, b, CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `c-day`, d
         |  from cdc
         |) t2
         |  on t1.b = t2.b
         |""".stripMargin)
  }

  @Test
  def testJoinHasSingleSideUk(): Unit = {
    if (tryResolve) {
      // the input side without uk requires all columns be deterministic
      thrown.expectMessage("can not satisfy the determinism requirement")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlan(
      s"""
         |select t1.a, t2.`c-day`, t2.b, t2.d
         |from (
         |  select a, b, c, d
         |  from cdc
         | ) t1
         |join (
         |  select a, b, CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `c-day`, d
         |  from cdc
         |) t2
         |  on t1.b = t2.b
         |""".stripMargin)
  }

  @Test
  def testSemiJoinKeyContainsUk(): Unit = {
    util.verifyExecPlan(
      s"""
         |select t1.a, t1.`c-day`, t1.b, t1.d
         |from (
         |  select a, b, CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `c-day`, d
         |  from cdc
         | ) t1
         |where t1.a in (
         |  select a from cdc where b > 100
         |)
         |""".stripMargin)
  }

  @Test
  def testAntiJoinKeyContainsUk(): Unit = {

    util.verifyExecPlan(
      s"""
         |select t1.a, t1.`c-day`, t1.b, t1.d
         |from (
         |  select a, b, CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `c-day`, d
         |  from cdc
         | ) t1
         |where t1.a not in (
         |  select a from cdc where b > 100
         |)
         |""".stripMargin)
  }

  @Test
  def testSemiJoinWithNonDeterministicConditionSingleSideHasUk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): c(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlan(
      s"""
         |select t1.a, t1.b, t1.c, t1.d
         |from (
         |  select a, b, c, d
         |  from cdc
         | ) t1
         |where t1.c in (
         |  select CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) c from cdc where b > 100
         |)
         |""".stripMargin)
  }

  @Test
  def testCdcJoinWithNonDeterministicOutputSinkWithPk(): Unit = {
    // a real case from FLINK-27369
    if (tryResolve) {
      thrown.expectMessage(
        "The column(s): logistics_time(generated by non-deterministic function: NOW ) can not satisfy the determinism requirement")
      thrown.expect(classOf[TableException])
    }
    util.tableEnv.executeSql(s"""
                                |CREATE TEMPORARY TABLE t_order (
                                | order_id INT,
                                | order_name STRING,
                                | product_id INT,
                                | user_id INT,
                                | PRIMARY KEY(order_id) NOT ENFORCED
                                |) WITH (
                                | 'connector' = 'values',
                                | 'changelog-mode' = 'I,UA,UB,D'
                                |)""".stripMargin)

    util.tableEnv.executeSql(s"""
                                |CREATE TEMPORARY TABLE t_logistics (
                                | logistics_id INT,
                                | logistics_target STRING,
                                | logistics_source STRING,
                                | logistics_time TIMESTAMP(0),
                                | order_id INT,
                                | PRIMARY KEY(logistics_id) NOT ENFORCED
                                |) WITH (
                                |  'connector' = 'values',
                                | 'changelog-mode' = 'I,UA,UB,D'
                                |)""".stripMargin)

    util.tableEnv.executeSql(s"""
                                |CREATE TEMPORARY TABLE t_join_sink (
                                | order_id INT,
                                | order_name STRING,
                                | logistics_id INT,
                                | logistics_target STRING,
                                | logistics_source STRING,
                                | logistics_time timestamp,
                                | PRIMARY KEY(order_id) NOT ENFORCED
                                |) WITH (
                                | 'connector' = 'values',
                                | 'sink-insert-only' = 'false'
                                |)""".stripMargin)

    util.verifyExecPlanInsert(
      s"""
         |INSERT INTO t_join_sink
         |SELECT ord.order_id,
         |ord.order_name,
         |logistics.logistics_id,
         |logistics.logistics_target,
         |logistics.logistics_source,
         |now()
         |FROM t_order AS ord
         |LEFT JOIN t_logistics AS logistics ON ord.order_id=logistics.order_id
         |""".stripMargin)
  }

  @Test
  def testProctimeDedupOnCdcWithMetadataSinkWithPk(): Unit = {
    // TODO this should be updated after StreamPhysicalDeduplicate supports consuming update
    thrown.expectMessage(
      "StreamPhysicalDeduplicate doesn't support consuming update and delete changes")
    thrown.expect(classOf[TableException])
    util.verifyExecPlanInsert(
      """
        |insert into sink_with_pk
        |SELECT a, metadata_3, c
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER (PARTITION BY a ORDER BY PROCTIME() ASC) as rowNum
        |  FROM cdc_with_meta
        |)
        |WHERE rowNum = 1
      """.stripMargin
    )
  }

  @Test
  def testProctimeDedupOnCdcWithMetadataSinkWithoutPk(): Unit = {
    // TODO this should be updated after StreamPhysicalDeduplicate supports consuming update
    thrown.expectMessage(
      "StreamPhysicalDeduplicate doesn't support consuming update and delete changes")
    thrown.expect(classOf[TableException])
    util.verifyExecPlanInsert(
      """
        |insert into sink_without_pk
        |SELECT a, metadata_3, c
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER (PARTITION BY a ORDER BY PROCTIME() ASC) as rowNum
        |  FROM cdc_with_meta
        |)
        |WHERE rowNum = 1
      """.stripMargin
    )
  }

  @Test
  def testRowtimeDedupOnCdcWithMetadataSinkWithPk(): Unit = {
    // TODO this should be updated after StreamPhysicalDeduplicate supports consuming update
    thrown.expectMessage(
      "StreamPhysicalDeduplicate doesn't support consuming update and delete changes")
    thrown.expect(classOf[TableException])
    util.verifyExecPlanInsert(
      """
        |insert into sink_with_pk
        |SELECT a, b, c
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER (PARTITION BY a ORDER BY op_ts ASC) as rowNum
        |  FROM cdc_with_meta_and_wm
        |)
        |WHERE rowNum = 1
      """.stripMargin
    )
  }

  @Test
  def testWindowDedupOnCdcWithMetadata(): Unit = {
    // TODO this should be updated after StreamPhysicalWindowDeduplicate supports consuming update
    thrown.expectMessage(
      "StreamPhysicalWindowDeduplicate doesn't support consuming update and delete changes")
    thrown.expect(classOf[TableException])

    util.tableEnv.executeSql("""
                               |create temporary table sink1 (
                               | a int,
                               | b bigint,
                               | c string,
                               | ts timestamp(3),
                               | primary key (a,ts) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)

    util.verifyExecPlanInsert(
      """
        |INSERT INTO sink1
        |SELECT a, b, c, window_start
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end
        |   ORDER BY op_ts DESC) as rownum
        |FROM TABLE(TUMBLE(TABLE cdc_with_meta_and_wm, DESCRIPTOR(op_ts), INTERVAL '1' MINUTE))
        |)
        |WHERE rownum <= 1""".stripMargin)

  }

  @Test
  def testNestedSourceWithMultiSink(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE nested_src (
         |  id int,
         |  deepNested row<nested1 row<name string, `value` int>,
         |    nested2 row<num int, flag boolean>>,
         |  name string,
         |  metadata_1 int metadata,
         |  metadata_2 string metadata,
         |  primary key(id, name) not enforced
         |) WITH (
         |  'connector' = 'values',
         |  'nested-projection-supported' = 'true',
         |  'changelog-mode' = 'I,UA,UB,D',
         |  'readable-metadata' = 'metadata_1:INT, metadata_2:STRING, metadata_3:BIGINT'
         |)
         |""".stripMargin
    util.tableEnv.executeSql(ddl)

    util.tableEnv.executeSql(
      """
        |create view v1 as
        |SELECT id,
        |       deepNested.nested2.num AS a,
        |       deepNested.nested1.name AS name,
        |       deepNested.nested1.`value` + deepNested.nested2.num + metadata_1 as b
        |FROM nested_src
        |""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table sink1 (
                               |  a int,
                               |  b string,
                               |  d bigint
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table sink2 (
                               |  a int,
                               |  b string,
                               |  d bigint
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)

    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(
      s"""
         |insert into sink1
         |select a, `day`, sum(b)
         |from (select a, b, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') as `day` from v1) t
         |group by a, `day`
         |""".stripMargin)
    stmtSet.addInsertSql(s"""
                            |insert into sink2
                            |select a, name, b
                            |from v1
                            |where b > 100
                            |""".stripMargin)
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testMultiSinkOnJoinedView(): Unit = {
    util.tableEnv.executeSql("""
                               |create temporary table src1 (
                               |  a int,
                               |  b bigint,
                               |  c string,
                               |  d int,
                               |  primary key(a, c) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table src2 (
                               |  a int,
                               |  b bigint,
                               |  c string,
                               |  d int,
                               |  primary key(a, c) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table sink1 (
                               |  a int,
                               |  b string,
                               |  c bigint,
                               |  d bigint
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table sink2 (
                               |  a int,
                               |  b string,
                               |  c bigint,
                               |  d string
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)

    util.tableEnv.executeSql(
      s"""
         |create temporary view v1 as
         |select
         |  t1.a as a, t1.`day` as `day`, t2.b as b, t2.c as c
         |from (
         |  select a, b, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') as `day`
         |  from src1
         | ) t1
         |join (
         |  select b, CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `day`, c, d
         |  from src2
         |) t2
         | on t1.a = t2.d
         |""".stripMargin)

    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(s"""
                            |insert into sink1
                            |select a, `day`, sum(b), count(distinct c)
                            |from v1
                            |group by a, `day`
                            |""".stripMargin)
    stmtSet.addInsertSql(s"""
                            |insert into sink2
                            |select a, `day`, b, c
                            |from v1
                            |where b > 100
                            |""".stripMargin)
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testMatchRecognizeSinkWithPk(): Unit = {
    util.tableEnv.executeSql(s"""
                                |create temporary view v1 as
                                |select *, PROCTIME() as proctime from src
                                |""".stripMargin)

    util.verifyExecPlanInsert(
      """
        |insert into sink_with_pk
        |SELECT T1.a, T1.b, cast(T1.matchProctime as varchar)
        |FROM v1
        |MATCH_RECOGNIZE (
        |PARTITION BY c
        |ORDER BY proctime
        |MEASURES
        |  A.a as a,
        |  A.b as b,
        |  MATCH_PROCTIME() as matchProctime
        |ONE ROW PER MATCH
        |PATTERN (A)
        |DEFINE
        |  A AS A.a > 1
        |) AS T1
        |""".stripMargin
    )
  }

  @Test
  def testMatchRecognizeWithNonDeterministicConditionOnCdcSinkWithPk(): Unit = {
    // TODO this should be updated after StreamPhysicalMatch supports consuming updates
    thrown.expectMessage("Match Recognize doesn't support consuming update and delete changes")
    thrown.expect(classOf[TableException])
    util.verifyExecPlanInsert(
      """
        |insert into sink_with_pk
        |SELECT T.a, T.b, cast(T.matchRowtime as varchar)
        |FROM cdc_with_meta_and_wm
        |MATCH_RECOGNIZE (
        |PARTITION BY c
        |ORDER BY op_ts
        |MEASURES
        |  A.a as a,
        |  A.b as b,
        |  MATCH_ROWTIME(op_ts) as matchRowtime
        |ONE ROW PER MATCH
        |PATTERN (A)
        |DEFINE
        |  A AS A.op_ts >= CURRENT_TIMESTAMP
        |) AS T
      """.stripMargin
    )
  }

  @Test
  def testMatchRecognizeOnCdcWithMetaDataSinkWithPk(): Unit = {
    // TODO this should be updated after StreamPhysicalMatch supports consuming updates
    thrown.expectMessage("Match Recognize doesn't support consuming update and delete changes")
    thrown.expect(classOf[TableException])
    util.verifyExecPlanInsert(
      """
        |insert into sink_with_pk
        |SELECT T.a, T.b, cast(T.ts as varchar)
        |FROM cdc_with_meta_and_wm
        |MATCH_RECOGNIZE (
        |PARTITION BY c
        |ORDER BY op_ts
        |MEASURES
        |  A.a as a,
        |  A.b as b,
        |  A.op_ts as ts,
        |  MATCH_ROWTIME(op_ts) as matchRowtime
        |ONE ROW PER MATCH
        |PATTERN (A)
        |DEFINE
        |  A AS A.a > 0
        |) AS T
      """.stripMargin
    )
  }

  /**
   * This upsert test sink does support getting primary key from table schema. We defined a similar
   * test sink here not using existing {@link TestingUpsertTableSink} in {@link StreamTestSink}
   * because it can not get a primary key via 'getTableSchema#getPrimaryKey' call.
   */
  final class TestingUpsertSink(
      val keys: Array[String],
      val fieldNames: Array[String],
      val fieldTypes: Array[DataType])
    extends UpsertStreamTableSink[RowData] {
    var expectedKeys: Option[Array[String]] = None
    var expectedIsAppendOnly: Option[Boolean] = None

    override def getTableSchema: TableSchema = {
      val builder = TableSchema.builder
      assert(fieldNames.length == fieldTypes.length)
      builder.fields(fieldNames, fieldTypes)
      if (null != keys && keys.nonEmpty) {
        builder.primaryKey(keys: _*)
      }
      builder.build()
    }

    override def setKeyFields(keys: Array[String]): Unit = {
      if (expectedKeys.isDefined && keys == null) {
        throw new AssertionError("Provided key fields should not be null.")
      } else if (expectedKeys.isEmpty) {
        return
      }
    }

    override def setIsAppendOnly(isAppendOnly: JBoolean): Unit = {
      if (expectedIsAppendOnly.isEmpty) {
        return
      }
    }

    override def getRecordType: TypeInformation[RowData] =
      InternalTypeInfo.ofFields(fieldTypes.map(_.getLogicalType), fieldNames)

    override def consumeDataStream(
        dataStream: DataStream[tuple.Tuple2[JBoolean, RowData]]): DataStreamSink[_] = ???

    override def configure(
        fNames: Array[String],
        fTypes: Array[TypeInformation[_]]): TestingUpsertSink = {
      new TestingUpsertSink(keys, fNames, fTypes.map(TypeConversions.fromLegacyInfoToDataType(_)))
    }
  }

}

object NonDeterministicDagTest {

  @Parameterized.Parameters(name = "nonDeterministicUpdateStrategy={0}")
  def parameters(): util.Collection[NonDeterministicUpdateStrategy] = {
    util.Arrays.asList(
      NonDeterministicUpdateStrategy.TRY_RESOLVE,
      NonDeterministicUpdateStrategy.IGNORE)
  }
}
