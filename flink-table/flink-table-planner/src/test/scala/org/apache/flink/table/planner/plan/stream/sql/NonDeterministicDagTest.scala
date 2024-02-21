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
import org.apache.flink.testutils.junit.extensions.parameterized.{ParameterizedTestExtension, Parameters}

import org.assertj.core.api.Assertions.{assertThatCode, assertThatThrownBy}
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.junit.jupiter.api.{BeforeEach, TestTemplate}
import org.junit.jupiter.api.extension.ExtendWith

import java.util

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class NonDeterministicDagTest(nonDeterministicUpdateStrategy: NonDeterministicUpdateStrategy)
  extends TableTestBase {

  private val util: StreamTableTestUtil = streamTestUtil()
  private val tryResolve =
    nonDeterministicUpdateStrategy == NonDeterministicUpdateStrategy.TRY_RESOLVE

  @BeforeEach
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
                               |create temporary table upsert_src_with_meta (
                               | a int,
                               | b bigint,
                               | c string,
                               | d boolean,
                               | metadata_1 int metadata,
                               | metadata_2 string metadata,
                               | primary key (a) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,D',
                               | 'readable-metadata' = 'metadata_1:INT, metadata_2:STRING'
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

  @TestTemplate
  def testCdcWithMetaSinkWithPk(): Unit = {
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select a, metadata_3, c
                                 |from cdc_with_meta
                                 |""".stripMargin)
  }
  @TestTemplate
  def testNonDeterministicProjectionWithSinkWithoutPk(): Unit = {
    val callable: ThrowingCallable = () =>
      util.verifyExecPlanInsert(
        """
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

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "The column(s): d(generated by non-deterministic function: ndFunc ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testCdcWithMetaSinkWithoutPk(): Unit = {
    val callable: ThrowingCallable = () => util.verifyExecPlanInsert(s"""
                                                                        |insert into sink_without_pk
                                                                        |select a, metadata_3, c
                                                                        |from cdc_with_meta
                                                                        |""".stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
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

  @TestTemplate
  def testCdcWithMetaLegacySinkWithoutPk(): Unit = {
    val retractSink =
      util.createRetractTableSink(
        Array("a", "b", "c"),
        Array(new IntType(), new BigIntType(), VarCharType.STRING_TYPE))
    util.tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .registerTableSinkInternal("legacy_retract_sink", retractSink)

    val callable: ThrowingCallable = () =>
      util.verifyExecPlanInsert(s"""
                                   |insert into legacy_retract_sink
                                   |select a, metadata_3, c
                                   |from cdc_with_meta
                                   |""".stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testCdcWithMetaSinkWithCompositePk(): Unit = {
    val callable: ThrowingCallable = () =>
      util.verifyExecPlanInsert(s"""
                                   |insert into sink_with_composite_pk
                                   |select a, b, c, metadata_3
                                   |from cdc_with_meta
                                   |""".stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testCdcWithMetaRenameSinkWithCompositePk(): Unit = {
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

    val callable: ThrowingCallable = () =>
      util.verifyExecPlanInsert(s"""
                                   |insert into sink_with_composite_pk
                                   |select a, b, c, e from cdc_with_meta_rename
                                   |""".stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testSourceWithComputedColumnSinkWithPk(): Unit = {
    // can not infer pk from cdc source with computed column(s)
    val callable: ThrowingCallable = () => util.verifyExecPlanInsert(s"""
                                                                        |insert into sink_with_pk
                                                                        |select a, b, `day`
                                                                        |from cdc_with_computed_col
                                                                        |where b > 100
                                                                        |""".stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
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

    val callable: ThrowingCallable = () => util.verifyExecPlan(stmtSet)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testCdcCorrelateNonDeterministicFuncSinkWithPK(): Unit = {
    val callable: ThrowingCallable = () =>
      util.verifyExecPlanInsert(s"""
                                   |insert into sink_with_pk
                                   |select
                                   |  t1.a, t1.b, a1
                                   |from cdc t1, lateral table(ndTableFunc(a)) as T(a1)
                                   |""".stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): EXPR$0(generated by non-deterministic function: ndTableFunc ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testCdcCorrelateNonDeterministicFuncNoLeftOutput(): Unit = {
    val callable: ThrowingCallable = () =>
      util.verifyExecPlanInsert(s"""
                                   |insert into sink_with_pk(a)
                                   |select
                                   |  cast(a1 as integer) a
                                   |from cdc t1, lateral table(ndTableFunc(a)) as T(a1)
                                   |""".stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): EXPR$0(generated by non-deterministic function: ndTableFunc ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testCdcCorrelateNonDeterministicFuncNoRightOutput(): Unit = {
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select a, b, c
                                 |from cdc t1 join lateral table(ndTableFunc(a)) as T(a1) on true
                                 |""".stripMargin)
  }

  @TestTemplate
  def testCdcCorrelateOnNonDeterministicCondition(): Unit = {
    // TODO update this after FLINK-7865 was fixed
    assertThatThrownBy(
      () =>
        util.verifyExecPlanInsert(
          s"""
             |insert into sink_with_pk
             |select a, b, c
             |from cdc t1 join lateral table(str_split(c)) as T(c1)
             | -- the join predicate can only be empty or literal true for now
             |  on ndFunc(b) > 100
             |""".stripMargin))
      .hasMessageContaining("unexpected correlate variable $cor0 in the plan")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testCdcWithMetaCorrelateSinkWithPk(): Unit = {
    val callable: ThrowingCallable = () =>
      util.verifyExecPlanInsert(s"""
                                   |insert into sink_with_pk
                                   |select t1.a, t1.metadata_1, T.c1
                                   |from cdc_with_meta t1, lateral table(str_split(c)) as T(c1)
                                   |""".stripMargin)

    // Under ignore mode, the generated execution plan may cause wrong result though
    // upsertMaterialize has been enabled in sink, because
    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "metadata column(s): 'metadata_1' in cdc source may cause wrong result or error on downstream operators")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testCdcWithNonDeterministicFuncSinkWithPk(): Unit = {
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select a, ndFunc(b), c
                                 |from cdc
                                 |""".stripMargin)
  }

  @TestTemplate
  def testCdcWithNonDeterministicFuncSinkWithoutPk(): Unit = {
    val callable: ThrowingCallable = () => util.verifyExecPlanInsert(s"""
                                                                        |insert into sink_without_pk
                                                                        |select a, ndFunc(b), c
                                                                        |from cdc
                                                                        |""".stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): EXPR$1(generated by non-deterministic function: ndFunc ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testCdcWithNonDeterministicFilter(): Unit = {
    // TODO should throw error if tryResolve is true after FLINK-28737 was fixed
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select t1.a, t1.b, t1.c
                                 |from cdc t1
                                 |where t1.b > UNIX_TIMESTAMP() - 300
                                 |""".stripMargin)
  }

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
  def testCdcJoinDimWithPkNonDeterministicFuncSinkWithoutPk(): Unit = {
    val callable: ThrowingCallable = () =>
      util.verifyExecPlanInsert(s"""
                                   |insert into sink_without_pk
                                   |select ndFunc(t2.a) a, t1.b, t1.c
                                   |from (
                                   |  select *, proctime() proctime from cdc
                                   |) t1 join dim_with_pk for system_time as of t1.proctime as t2
                                   |on t1.a = t2.a
                                   |""".stripMargin)

    if (tryResolve) {
      // only select lookup key field, but with ND-call, expect exception
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): a(generated by non-deterministic function: ndFunc ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testCdcJoinDimWithPkNonDeterministicLocalCondition(): Unit = {
    // use user defined function
    val callable: ThrowingCallable = () =>
      util.verifyExecPlanInsert(s"""
                                   |insert into sink_without_pk
                                   |select t1.a, t1.b, t1.c
                                   |from (
                                   |  select *, proctime() proctime from cdc
                                   |) t1 join dim_with_pk for system_time as of t1.proctime as t2
                                   |on t1.a = t2.a and ndFunc(t2.b) > 100
                                   |""".stripMargin)

    if (tryResolve) {
      // not select lookup source field, but with NonDeterministicCondition, expect exception
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "exists non deterministic function: 'ndFunc' in condition: '>(ndFunc($1), 100)' which may cause wrong result")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testCdcJoinDimWithPkNonDeterministicLocalCondition2(): Unit = {
    // use builtin temporal function
    val callable: ThrowingCallable = () =>
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

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "exists non deterministic function: 'UNIX_TIMESTAMP' in condition: '>($1, -(UNIX_TIMESTAMP(), 300))' which may cause wrong result")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testCdcJoinDimNonDeterministicRemainingCondition(): Unit = {
    val callable: ThrowingCallable = () =>
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

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "exists non deterministic function: 'ndFunc' in condition: '>($1, ndFunc($3))' which may cause wrong result")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testCdcLeftJoinDimWithNonDeterministicPreFilter(): Unit = {
    val callable: ThrowingCallable = () =>
      util.verifyExecPlanInsert(
        s"""
           |insert into sink_with_pk
           |select t1.a, t2.b as version, t2.c
           |from (
           |  select *, proctime() proctime from cdc
           |) t1 left join dim_with_pk for system_time as of t1.proctime as t2
           |on t1.a = t2.a
           |  and t1.b > UNIX_TIMESTAMP() - 300
           |""".stripMargin)

    // use builtin temporal function
    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "exists non deterministic function: 'UNIX_TIMESTAMP' in condition: '>($1, -(UNIX_TIMESTAMP(), 300))' which may cause wrong result")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testGroupByNonDeterministicFuncWithCdcSource(): Unit = {
    val callable: ThrowingCallable = () =>
      util.verifyExecPlanInsert(
        s"""
           |insert into sink_with_pk
           |select
           |  a, count(*) cnt, `day`
           |from (
           |  select *, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') `day` from cdc
           |) t
           |group by `day`, a
           |""".stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testGroupByNonDeterministicUdfWithCdcSource(): Unit = {
    val callable: ThrowingCallable = () => util.verifyExecPlanInsert(s"""
                                                                        |insert into sink_with_pk
                                                                        |select
                                                                        |  ndFunc(a), count(*) cnt, c
                                                                        |from cdc
                                                                        |group by ndFunc(a), c
                                                                        |""".stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): EXPR$0(generated by non-deterministic function: ndFunc ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testNestedAggWithNonDeterministicGroupingKeys(): Unit = {
    val callable: ThrowingCallable = () =>
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

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testGroupAggNonDeterministicFuncOnSourcePk(): Unit = {
    val callable: ThrowingCallable = () =>
      util.verifyExecPlan(
        s"""
           |select
           |  `day`, count(*) cnt, sum(b) qmt
           |from (
           |  select *, concat(cast(a as varchar), DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) `day` from cdc
           |) t
           |group by `day`
           |""".stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
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

  @TestTemplate
  def testAggWithNonDeterministicFilterArgsOnCdcSource(): Unit = {
    val callable: ThrowingCallable = () =>
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

    if (tryResolve) {
      // though original pk was selected and same as the sink's pk, but the valid_uv was
      // non-deterministic, will raise an error
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): $f2(generated by non-deterministic function: UNIX_TIMESTAMP ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testAggWithNonDeterministicFilterArgsOnCdcSourceSinkWithoutPk(): Unit = {
    val callable: ThrowingCallable = () =>
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

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): $f2(generated by non-deterministic function: UNIX_TIMESTAMP ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
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

  @TestTemplate
  def testNonDeterministicAggOnAppendSourceSinkWithoutPk(): Unit = {
    val callable: ThrowingCallable = () => util.verifyExecPlanInsert(s"""
                                                                        |insert into sink_without_pk
                                                                        |select
                                                                        |  a
                                                                        |  ,ndAggFunc(b) ndCnt
                                                                        |  ,max(c) mc
                                                                        |from T
                                                                        |group by a
                                                                        |""".stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): ndCnt(generated by non-deterministic function: ndAggFunc ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
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

  @TestTemplate
  def testGlobalNonDeterministicAggOnAppendSourceSinkWithoutPk(): Unit = {
    val callable: ThrowingCallable = () => util.verifyExecPlanInsert(s"""
                                                                        |insert into sink_without_pk
                                                                        |select
                                                                        |  max(a)
                                                                        |  ,ndAggFunc(b) ndCnt
                                                                        |  ,max(c) mc
                                                                        |from T
                                                                        |""".stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): ndCnt(generated by non-deterministic function: ndAggFunc ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testUpsertSourceSinkWithPk(): Unit = {
    // contains normalize
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select a, b, c
                                 |from upsert_src
                                 |""".stripMargin)
  }

  @TestTemplate
  def testUpsertSourceSinkWithoutPk(): Unit = {
    // contains normalize
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select a, b, c
                                 |from upsert_src
                                 |""".stripMargin)
  }

  @TestTemplate
  def testUpsertSourceWithMetaSinkWithPk(): Unit = {
    // contains normalize
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select metadata_1, b, metadata_2
                                 |from upsert_src_with_meta
                                 |""".stripMargin)
  }

  @TestTemplate
  def testUpsertSourceWithMetaSinkWithoutPk(): Unit = {
    // contains normalize
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select metadata_1, b, metadata_2
                                 |from upsert_src_with_meta
                                 |""".stripMargin)
  }

  @TestTemplate
  def testCdcSourceWithoutPkSinkWithoutPk(): Unit = {
    util.tableEnv.executeSql("""
                               |create temporary table cdc_without_pk (
                               | a int,
                               | b bigint,
                               | c string,
                               | d boolean,
                               | metadata_1 int metadata,
                               | metadata_2 string metadata
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D',
                               | 'readable-metadata' = 'metadata_1:INT, metadata_2:STRING'
                               |)""".stripMargin)

    // doesn't contain changelog normalize
    val callable: ThrowingCallable = () =>
      util.verifyExecPlanInsert(s"""
                                   |insert into sink_without_pk
                                   |select metadata_1, b, metadata_2
                                   |from cdc_without_pk
                                   |""".stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "The metadata column(s): 'metadata_1, metadata_2' in cdc source may cause wrong result or error on downstream operators")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testTemporalJoinSinkWithoutPk(): Unit = {
    util.addTable("""
                    |CREATE TABLE Orders (
                    | amount INT,
                    | currency STRING,
                    | rowtime TIMESTAMP(3),
                    | proctime AS PROCTIME(),
                    | WATERMARK FOR rowtime AS rowtime
                    |) WITH (
                    | 'connector' = 'values'
                    |)
      """.stripMargin)
    util.addTable("""
                    |CREATE TABLE UpsertRates (
                    | currency STRING,
                    | rate INT,
                    | valid VARCHAR,
                    | metadata_1 INT METADATA,
                    | rowtime TIMESTAMP(3),
                    | WATERMARK FOR rowtime AS rowtime,
                    | PRIMARY KEY(currency) NOT ENFORCED
                    |) WITH (
                    | 'connector' = 'values',
                    | 'changelog-mode' = 'I,UA,D',
                    | 'disable-lookup' = 'true',
                    | 'readable-metadata' = 'metadata_1:INT'
                    |)
      """.stripMargin)

    // temporal join output insert-only result, expect no NDU issues
    util.verifyExecPlanInsert(s"""
                                 |INSERT INTO sink_without_pk
                                 |SELECT metadata_1, rate, o.currency
                                 |FROM Orders AS o JOIN
                                 |  UpsertRates FOR SYSTEM_TIME AS OF o.rowtime AS r
                                 |    ON o.currency = r.currency WHERE valid = 'true'
                                 |""".stripMargin)
  }

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
  def testMultiOverWithNonDeterministicAggFilterSinkWithPk(): Unit = {
    // agg with filter is not supported currently, should update this after it is supported.
    assertThatThrownBy(
      () =>
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
      """.stripMargin))
      .hasMessageContaining("OVER must be applied to aggregate function")
      .isInstanceOf[ValidationException]
  }

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
  def testUnionSinkWithCompositePk(): Unit = {
    val callable: ThrowingCallable = () =>
      util.verifyExecPlanInsert(s"""
                                   |insert into sink_with_composite_pk
                                   |select a, b, c, d
                                   |from src
                                   |union
                                   |select a, b, c, metadata_3
                                   |from cdc_with_meta
                                   |""".stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testUnionAllSinkWithCompositePk(): Unit = {
    val callable: ThrowingCallable = () =>
      util.verifyExecPlanInsert(s"""
                                   |insert into sink_with_composite_pk
                                   |select a, b, c, d
                                   |from src
                                   |union all
                                   |select a, b, c, metadata_3
                                   |from cdc_with_meta
                                   |""".stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testUnionAllSinkWithoutPk(): Unit = {
    val callable: ThrowingCallable = () => util.verifyExecPlanInsert(s"""
                                                                        |insert into sink_without_pk
                                                                        |select a, b, c
                                                                        |from src
                                                                        |union all
                                                                        |select a, metadata_3, c
                                                                        |from cdc_with_meta
                                                                        |""".stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testCdcJoinWithNonDeterministicCondition(): Unit = {
    val callable: ThrowingCallable = () =>
      util.verifyExecPlanInsert(s"""
                                   |insert into sink_without_pk
                                   |select
                                   |  t1.a
                                   |  ,t2.b
                                   |  ,t1.c
                                   |from cdc t1 join cdc t2
                                   |  on ndFunc(t1.b) = ndFunc(t2.b)
                                   |""".stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): $f4(generated by non-deterministic function: ndFunc ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testProctimeIntervalJoinSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert("""
                                |insert into sink_without_pk
                                |SELECT t2.a, t2.c, t1.b FROM T1 t1 JOIN T1 t2 ON
                                |  t1.a = t2.a AND t1.proctime > t2.proctime - INTERVAL '5' SECOND
      """.stripMargin)
  }

  @TestTemplate
  def testCdcProctimeIntervalJoinOnPkSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert("""
                                |insert into sink_without_pk
                                |SELECT t2.a, t2.b, t1.c FROM (
                                | select *, proctime() proctime from cdc) t1 JOIN
                                | (select *, proctime() proctime from cdc) t2 ON
                                |  t1.a = t2.a AND t1.proctime > t2.proctime - INTERVAL '5' SECOND
      """.stripMargin)
  }

  @TestTemplate
  def testCdcProctimeIntervalJoinOnNonPkSinkWithoutPk(): Unit = {
    val callable: ThrowingCallable = () =>
      util.verifyExecPlanInsert("""
                                  |insert into sink_without_pk
                                  |SELECT t2.a, t2.b, t1.c FROM (
                                  | select *, proctime() proctime from cdc) t1 JOIN
                                  | (select *, proctime() proctime from cdc) t2 ON
                                  |  t1.b = t2.b AND t1.proctime > t2.proctime - INTERVAL '5' SECOND
      """.stripMargin)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining("can not satisfy the determinism requirement")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testCdcRowtimeIntervalJoinSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert(
      """
        |insert into sink_without_pk
        |SELECT t2.a, t1.b, t2.c FROM cdc_with_watermark t1 JOIN cdc_with_watermark t2 ON
        |  t1.a = t2.a AND t1.op_ts > t2.op_ts - INTERVAL '5' SECOND
      """.stripMargin)
  }

  @TestTemplate
  def testCdcRowtimeIntervalJoinSinkWithPk(): Unit = {
    util.verifyExecPlanInsert(
      """
        |insert into sink_with_pk
        |SELECT t2.a, t1.b, t2.c FROM cdc_with_watermark t1 JOIN cdc_with_watermark t2 ON
        |  t1.a = t2.a AND t1.op_ts > t2.op_ts - INTERVAL '5' SECOND
      """.stripMargin)
  }

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
  def testJoinHasBothSidesUkSinkWithoutPk(): Unit = {
    val callable: ThrowingCallable = () =>
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

    if (tryResolve) {
      // sink require all columns be deterministic though join has both side uk
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): c-day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testJoinHasSingleSideUk(): Unit = {
    val callable: ThrowingCallable = () =>
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

    if (tryResolve) {
      // the input side without uk requires all columns be deterministic
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): c-day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
  def testSemiJoinWithNonDeterministicConditionSingleSideHasUk(): Unit = {
    val callable: ThrowingCallable = () =>
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

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): c(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testCdcJoinWithNonDeterministicOutputSinkWithPk(): Unit = {
    // a real case from FLINK-27369
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

    val callable: ThrowingCallable = () =>
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

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "The column(s): logistics_time(generated by non-deterministic function: NOW ) can not satisfy the determinism requirement")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
  def testProctimeDedupOnCdcWithMetadataSinkWithPk(): Unit = {
    // TODO this should be updated after StreamPhysicalDeduplicate supports consuming update
    assertThatThrownBy(
      () =>
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
      """.stripMargin))
      .hasMessageContaining(
        "StreamPhysicalDeduplicate doesn't support consuming update and delete changes")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testProctimeDedupOnCdcWithMetadataSinkWithoutPk(): Unit = {
    // TODO this should be updated after StreamPhysicalDeduplicate supports consuming update
    assertThatThrownBy(
      () =>
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
        ))
      .hasMessageContaining(
        "StreamPhysicalDeduplicate doesn't support consuming update and delete changes")
      .isInstanceOf[TableException]

  }

  @TestTemplate
  def testRowtimeDedupOnCdcWithMetadataSinkWithPk(): Unit = {
    // TODO this should be updated after StreamPhysicalDeduplicate supports consuming update
    assertThatThrownBy(
      () =>
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
        ))
      .hasMessageContaining(
        "StreamPhysicalDeduplicate doesn't support consuming update and delete changes")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testWindowDedupOnCdcWithMetadata(): Unit = {
    // TODO this should be updated after StreamPhysicalWindowDeduplicate supports consuming update
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

    assertThatThrownBy(
      () =>
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
            |WHERE rownum <= 1""".stripMargin))
      .hasMessageContaining(
        "StreamPhysicalWindowDeduplicate doesn't support consuming update and delete changes")
      .isInstanceOf[TableException]
  }

  @TestTemplate
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

    val callable: ThrowingCallable = () => util.verifyExecPlan(stmtSet)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
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

    val callable: ThrowingCallable = () => util.verifyExecPlan(stmtSet)

    if (tryResolve) {
      assertThatThrownBy(callable)
        .hasMessageContaining(
          "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
        .isInstanceOf[TableException]
    } else {
      assertThatCode(callable).doesNotThrowAnyException()
    }
  }

  @TestTemplate
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

  @TestTemplate
  def testMatchRecognizeWithNonDeterministicConditionOnCdcSinkWithPk(): Unit = {
    // TODO this should be updated after StreamPhysicalMatch supports consuming updates
    assertThatThrownBy(
      () =>
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
        ))
      .hasMessageContaining("Match Recognize doesn't support consuming update and delete changes")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testMatchRecognizeOnCdcWithMetaDataSinkWithPk(): Unit = {
    // TODO this should be updated after StreamPhysicalMatch supports consuming updates
    assertThatThrownBy(
      () =>
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
        ))
      .hasMessageContaining("Match Recognize doesn't support consuming update and delete changes")
      .isInstanceOf[TableException]
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

  @Parameters(name = "nonDeterministicUpdateStrategy={0}")
  def parameters(): util.Collection[NonDeterministicUpdateStrategy] = {
    util.Arrays.asList(
      NonDeterministicUpdateStrategy.TRY_RESOLVE,
      NonDeterministicUpdateStrategy.IGNORE)
  }
}
