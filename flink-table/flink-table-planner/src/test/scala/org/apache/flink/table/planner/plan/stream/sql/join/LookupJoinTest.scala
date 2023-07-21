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
package org.apache.flink.table.planner.plan.stream.sql.join

import org.apache.flink.api.scala._
import org.apache.flink.core.testutils.FlinkMatchers.containsMessage
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.data.RowData
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE
import org.apache.flink.table.descriptors.DescriptorProperties
import org.apache.flink.table.factories.TableSourceFactory
import org.apache.flink.table.functions.{AsyncTableFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.table.planner.plan.utils._
import org.apache.flink.table.planner.utils.{TableTestBase, TestingTableEnvironment}
import org.apache.flink.table.planner.utils.TableTestUtil.{readFromResource, replaceNodeIdInOperator, replaceStageId, replaceStreamNodeId}
import org.apache.flink.table.sources._
import org.apache.flink.table.types.DataType
import org.apache.flink.table.utils.EncodingUtils

import _root_.java.lang.{Boolean => JBoolean}
import _root_.java.sql.Timestamp
import _root_.java.util
import _root_.java.util.{ArrayList => JArrayList, Collection => JCollection, HashMap => JHashMap, List => JList, Map => JMap}
import _root_.scala.collection.JavaConversions._
import org.junit.{Assume, Before, Test}
import org.junit.Assert.{assertEquals, assertThat, assertTrue, fail}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

/**
 * The physical plans for legacy [[org.apache.flink.table.sources.LookupableTableSource]] and new
 * [[org.apache.flink.table.connector.source.LookupTableSource]] should be identical.
 */
@RunWith(classOf[Parameterized])
class LookupJoinTest(legacyTableSource: Boolean) extends TableTestBase with Serializable {

  private val util = streamTestUtil()

  @Before
  def before(): Unit = {
    util.addDataStream[(Int, String, Long)](
      "MyTable",
      'a,
      'b,
      'c,
      'proctime.proctime,
      'rowtime.rowtime)
    util.addDataStream[(Int, String, Long, Double)]("T1", 'a, 'b, 'c, 'd)
    util.addDataStream[(Int, String, Int)]("nonTemporal", 'id, 'name, 'age)

    if (legacyTableSource) {
      TestTemporalTable.createTemporaryTable(util.tableEnv, "LookupTable")
      TestTemporalTable.createTemporaryTable(util.tableEnv, "AsyncLookupTable", async = true)
    } else {
      util.addTable("""
                      |CREATE TABLE LookupTable (
                      |  `id` INT,
                      |  `name` STRING,
                      |  `age` INT
                      |) WITH (
                      |  'connector' = 'values'
                      |)
                      |""".stripMargin)
      util.addTable("""
                      |CREATE TABLE AsyncLookupTable (
                      |  `id` INT,
                      |  `name` STRING,
                      |  `age` INT
                      |) WITH (
                      |  'connector' = 'values',
                      |  'async' = 'true'
                      |)
                      |""".stripMargin)

      util.addTable("""
                      |CREATE TABLE LookupTableWithComputedColumn (
                      |  `id` INT,
                      |  `name` STRING,
                      |  `age` INT,
                      |  `nominal_age` as age + 1
                      |) WITH (
                      |  'connector' = 'values',
                      |  'bounded' = 'true'
                      |)
                      |""".stripMargin)
    }
    util.addTable("""
                    |CREATE TABLE Sink1 (
                    |  a int,
                    |  name varchar,
                    |  age int
                    |) with (
                    |  'connector' = 'values',
                    |  'sink-insert-only' = 'false'
                    |)""".stripMargin)
    // for json plan test
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, Int.box(4))
  }

  @Test
  def testJoinInvalidJoinTemporalTable(): Unit = {
    // must follow a period specification
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN LookupTable T.proctime AS D ON T.a = D.id",
      "SQL parse failed",
      classOf[SqlParserException])

    // only support left or inner join
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T RIGHT JOIN LookupTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id",
      "Correlate has invalid join type RIGHT",
      classOf[AssertionError]
    )

    // only support join on raw key of right table
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN LookupTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a + 1 = D.id + 2",
      "Temporal table join requires an equality condition on fields of table " +
        "[default_catalog.default_database.LookupTable].",
      classOf[TableException]
    )
  }

  @Test
  def testNotDistinctFromInJoinCondition(): Unit = {

    // does not support join condition contains `IS NOT DISTINCT`
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN LookupTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a IS NOT  DISTINCT FROM D.id",
      "LookupJoin doesn't support join condition contains 'a IS NOT DISTINCT FROM b' (or " +
        "alternative '(a = b) or (a IS NULL AND b IS NULL)')",
      classOf[TableException]
    )

    // does not support join condition contains `IS NOT  DISTINCT` and similar syntax
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN LookupTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id OR (T.a IS NULL AND D.id IS NULL)",
      "LookupJoin doesn't support join condition contains 'a IS NOT DISTINCT FROM b' (or " +
        "alternative '(a = b) or (a IS NULL AND b IS NULL)')",
      classOf[TableException]
    )
  }

  @Test
  def testInvalidLookupTableFunction(): Unit = {
    if (legacyTableSource) {
      return
    }
    util.addDataStream[(Int, String, Long, Timestamp)]("T", 'a, 'b, 'c, 'ts, 'proctime.proctime)
    createLookupTable("LookupTable1", new InvalidTableFunctionResultType)

    expectExceptionThrown(
      "SELECT * FROM T JOIN LookupTable1 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      s"output class can simply be a Row or RowData class",
      classOf[ValidationException]
    )

    createLookupTable("LookupTable2", new InvalidTableFunctionEvalSignature)
    expectExceptionThrown(
      "SELECT * FROM T JOIN LookupTable2 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "Could not find an implementation method 'eval' in class " +
        "'org.apache.flink.table.planner.plan.utils.InvalidTableFunctionEvalSignature' " +
        "for function 'default_catalog.default_database.LookupTable2' that matches the " +
        "following signature:\n" +
        "void eval(java.lang.Integer, org.apache.flink.table.data.StringData, " +
        "org.apache.flink.table.data.TimestampData)",
      classOf[ValidationException]
    )

    createLookupTable("LookupTable3", new TableFunctionWithRowDataVarArg)
    verifyTranslationSuccess(
      "SELECT * FROM T JOIN LookupTable3 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D " +
        "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    createLookupTable("LookupTable4", new TableFunctionWithRow)
    verifyTranslationSuccess(
      "SELECT * FROM T JOIN LookupTable4 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D " +
        "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    createLookupTable("LookupTable5", new AsyncTableFunctionWithRowDataVarArg)
    verifyTranslationSuccess(
      "SELECT * FROM T JOIN LookupTable5 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D " +
        "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    createLookupTable("LookupTable6", new AsyncTableFunctionWithRow)
    verifyTranslationSuccess(
      "SELECT * FROM T JOIN LookupTable6 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    createLookupTable("LookupTable7", new InvalidAsyncTableFunctionEvalSignature1)
    expectExceptionThrown(
      "SELECT * FROM T JOIN LookupTable7 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "Could not find an implementation method 'eval' in class " +
        "'org.apache.flink.table.planner.plan.utils.InvalidAsyncTableFunctionEvalSignature1' " +
        "for function 'default_catalog.default_database.LookupTable7' that matches the " +
        "following signature:\n" +
        "void eval(java.util.concurrent.CompletableFuture, java.lang.Integer, " +
        "org.apache.flink.table.data.StringData, org.apache.flink.table.data.TimestampData)",
      classOf[ValidationException]
    )

    createLookupTable("LookupTable8", new InvalidAsyncTableFunctionEvalSignature2)
    expectExceptionThrown(
      "SELECT * FROM T JOIN LookupTable8 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "Could not find an implementation method 'eval' in class " +
        "'org.apache.flink.table.planner.plan.utils.InvalidAsyncTableFunctionEvalSignature2' " +
        "for function 'default_catalog.default_database.LookupTable8' that matches the " +
        "following signature:\nvoid eval(java.util.concurrent.CompletableFuture, " +
        "java.lang.Integer, java.lang.String, " +
        "java.time.LocalDateTime)",
      classOf[ValidationException]
    )

    createLookupTable("LookupTable9", new AsyncTableFunctionWithRowDataVarArg)
    verifyTranslationSuccess(
      "SELECT * FROM T JOIN LookupTable9 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D " +
        "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    createLookupTable("LookupTable10", new InvalidAsyncTableFunctionEvalSignature3)
    expectExceptionThrown(
      "SELECT * FROM T JOIN LookupTable10 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "Could not find an implementation method 'eval' in class " +
        "'org.apache.flink.table.planner.plan.utils.InvalidAsyncTableFunctionEvalSignature3' " +
        "for function 'default_catalog.default_database.LookupTable10' that matches the " +
        "following signature:\n" +
        "void eval(java.util.concurrent.CompletableFuture, java.lang.Integer, " +
        "org.apache.flink.table.data.StringData, org.apache.flink.table.data.TimestampData)",
      classOf[ValidationException]
    )
  }

  @Test
  def testJoinOnDifferentKeyTypes(): Unit = {
    // Will do implicit type coercion.
    thrown.expect(classOf[TableException])
    thrown.expectMessage(
      "implicit type conversion between VARCHAR(2147483647) and INTEGER " +
        "is not supported on join's condition now")
    util.verifyExecPlan(
      "SELECT * FROM MyTable AS T JOIN LookupTable "
        + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.b = D.id")
  }

  @Test
  def testJoinTemporalTable(): Unit = {
    val sql = "SELECT * FROM MyTable AS T JOIN LookupTable " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"

    util.verifyExecPlan(sql)
  }

  @Test
  def testLeftJoinTemporalTable(): Unit = {
    val sql = "SELECT * FROM MyTable AS T LEFT JOIN LookupTable " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithNestedQuery(): Unit = {
    val sql = "SELECT * FROM " +
      "(SELECT a, b, proctime FROM MyTable WHERE c > 1000) AS T " +
      "JOIN LookupTable " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithProjectionPushDown(): Unit = {
    val sql =
      """
        |SELECT T.*, D.id
        |FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithFilterPushDown(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id AND D.age = 10
        |WHERE T.c > 1000
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithCalcPushDown(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id AND D.age = 10
        |WHERE cast(D.name as bigint) > 1000
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithMultiIndexColumn(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id AND D.age = 10 AND D.name = 'AAA'
        |WHERE T.c > 1000
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testAvoidAggregatePushDown(): Unit = {
    val sql1 =
      """
        |SELECT b, a, sum(c) c, sum(d) d, PROCTIME() as proc
        |FROM T1
        |GROUP BY a, b
      """.stripMargin

    val sql2 =
      s"""
         |SELECT T.* FROM ($sql1) AS T
         |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proc AS D
         |ON T.a = D.id
         |WHERE D.age > 10
      """.stripMargin

    val sql =
      s"""
         |SELECT b, count(a), sum(c), sum(d)
         |FROM ($sql2) AS T
         |GROUP BY b
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithTrueCondition(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage(
      "Temporal table join requires an equality condition on fields of " +
        "table [default_catalog.default_database.LookupTable]")
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON true
        |WHERE T.c > 1000
      """.stripMargin

    util.verifyExplain(sql)
  }

  @Test
  def testJoinTemporalTableWithFunctionAndConstantCondition(): Unit = {

    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.b = concat(D.name, '!') AND D.age = 11
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithMultiFunctionAndConstantCondition(): Unit = {

    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id + 1 AND T.b = concat(D.name, '!') AND D.age = 11
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithFunctionAndReferenceCondition(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id AND T.b = concat(D.name, '!')
        |WHERE D.name LIKE 'Jack%'
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithUdfEqualFilter(): Unit = {
    val sql =
      """
        |SELECT
        |  T.a, T.b, T.c, D.name
        |FROM
        |  MyTable AS T JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id
        |WHERE CONCAT('Hello-', D.name) = 'Hello-Jark'
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithComputedColumn(): Unit = {
    // Computed column do not support in legacyTableSource.
    Assume.assumeFalse(legacyTableSource)
    val sql =
      """
        |SELECT
        |  T.a, T.b, T.c, D.name, D.age, D.nominal_age
        |FROM
        |  MyTable AS T JOIN LookupTableWithComputedColumn FOR SYSTEM_TIME AS OF T.proctime AS D
        |  ON T.a = D.id
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithComputedColumnAndPushDown(): Unit = {
    // Computed column do not support in legacyTableSource.
    Assume.assumeFalse(legacyTableSource)
    val sql =
      """
        |SELECT
        |  T.a, T.b, T.c, D.name, D.age, D.nominal_age
        |FROM
        |  MyTable AS T JOIN LookupTableWithComputedColumn FOR SYSTEM_TIME AS OF T.proctime AS D
        |  ON T.a = D.id and D.nominal_age > 12
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithMultiConditionOnSameDimField(): Unit = {
    val sql = "SELECT * FROM MyTable AS T JOIN LookupTable " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id and CAST(T.c as INT) = D.id"

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithCastOnLookupTable(): Unit = {
    util.addTable("""
                    |CREATE TABLE LookupTable2 (
                    |  `id` decimal(38, 18),
                    |  `name` STRING,
                    |  `age` INT
                    |) WITH (
                    |  'connector' = 'values'
                    |)
                    |""".stripMargin)
    val sql =
      """
        |SELECT MyTable.b, LookupTable2.id
        |FROM MyTable
        |LEFT JOIN LookupTable2 FOR SYSTEM_TIME AS OF MyTable.`proctime`
        |ON MyTable.a = CAST(LookupTable2.`id` as INT)
        |""".stripMargin
    thrown.expect(classOf[TableException])
    thrown.expectMessage(
      "Temporal table join requires an equality condition on fields of " +
        "table [default_catalog.default_database.LookupTable2]")
    verifyTranslationSuccess(sql)
  }

  @Test
  def testJoinTemporalTableWithInteroperableCastOnLookupTable(): Unit = {
    util.addTable("""
                    |CREATE TABLE LookupTable2 (
                    |  `id` INT,
                    |  `name` char(10),
                    |  `age` INT
                    |) WITH (
                    |  'connector' = 'values'
                    |)
                    |""".stripMargin)

    val sql =
      """
        |SELECT MyTable.b, LookupTable2.id
        |FROM MyTable
        |LEFT JOIN LookupTable2 FOR SYSTEM_TIME AS OF MyTable.`proctime`
        |ON MyTable.b = CAST(LookupTable2.`name` as String)
        |""".stripMargin
    verifyTranslationSuccess(sql)
  }

  @Test
  def testJoinTemporalTableWithCTE(): Unit = {
    val sql =
      """
        |WITH MyLookupTable AS (SELECT * FROM MyTable),
        |OtherLookupTable AS (SELECT * FROM LookupTable)
        |SELECT MyLookupTable.b FROM MyLookupTable
        |JOIN OtherLookupTable FOR SYSTEM_TIME AS OF MyLookupTable.proctime AS D
        |ON MyLookupTable.a = D.id AND D.age = 10
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testAggAndAllConstantLookupKeyWithTryResolveMode(): Unit = {
    // expect lookup join using single parallelism due to all constant lookup key
    util.tableEnv.getConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY,
      OptimizerConfigOptions.NonDeterministicUpdateStrategy.TRY_RESOLVE)

    val sql =
      """
        |INSERT INTO Sink1
        |SELECT T.a, D.name, D.age
        |FROM (SELECT max(a) a, count(c) c, PROCTIME() proctime FROM MyTable GROUP BY b) T
        | LEFT JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |  ON D.id = 100
      """.stripMargin
    val actual = util.tableEnv.explainSql(sql, ExplainDetail.JSON_EXECUTION_PLAN)
    val expected = if (legacyTableSource) {
      readFromResource(
        "explain/stream/join/lookup/testAggAndAllConstantLookupKeyWithTryResolveMode.out")
    } else {
      readFromResource(
        "explain/stream/join/lookup/testAggAndAllConstantLookupKeyWithTryResolveMode_newSource.out")
    }
    assertEquals(
      replaceNodeIdInOperator(replaceStreamNodeId(replaceStageId(expected))),
      replaceNodeIdInOperator(replaceStreamNodeId(replaceStageId(actual))))
  }

  @Test
  def testInvalidJoinHint(): Unit = {
    // lost required hint option 'table'
    expectExceptionThrown(
      """
        |SELECT /*+ LOOKUP('tableName'='LookupTable') */ *
        |FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        | ON T.a = D.id
        |""".stripMargin,
      "Invalid LOOKUP hint: incomplete required option(s): [Key: 'table' , default: null (fallback keys: [])]",
      classOf[AssertionError]
    )

    // invalid async option value
    expectExceptionThrown(
      """
        |SELECT /*+ LOOKUP('table'='LookupTable', 'async'='yes') */ *
        |FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        | ON T.a = D.id
        |""".stripMargin,
      "Invalid LOOKUP hint options: Could not parse value 'yes' for key 'async'",
      classOf[AssertionError]
    )

    // invalid async output-mode option value
    expectExceptionThrown(
      """
        |SELECT /*+ LOOKUP('table'='LookupTable', 'async'='true', 'output-mode'='allow-unordered') */ *
        |FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        | ON T.a = D.id
        |""".stripMargin,
      "Invalid LOOKUP hint options: Could not parse value 'allow-unordered' for key 'output-mode'",
      classOf[AssertionError]
    )

    // invalid async timeout option value
    expectExceptionThrown(
      """
        |SELECT /*+ LOOKUP('table'='LookupTable', 'async'='true', 'timeout'='300 si') */ *
        |FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        | ON T.a = D.id
        |""".stripMargin,
      "Invalid LOOKUP hint options: Could not parse value '300 si' for key 'timeout'",
      classOf[AssertionError]
    )

    // invalid retry-strategy option value
    expectExceptionThrown(
      """
        |SELECT /*+ LOOKUP('table'='LookupTable', 'retry-strategy'='fixed-delay') */ *
        |FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        | ON T.a = D.id
        |""".stripMargin,
      "Invalid LOOKUP hint options: Could not parse value 'fixed-delay' for key 'retry-strategy'",
      classOf[AssertionError]
    )

    // invalid retry fixed-delay option value
    expectExceptionThrown(
      """
        |SELECT /*+ LOOKUP('table'='LookupTable', 'fixed-delay'='100 nano sec') */ *
        |FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        | ON T.a = D.id
        |""".stripMargin,
      "Invalid LOOKUP hint options: Could not parse value '100 nano sec' for key 'fixed-delay'",
      classOf[AssertionError]
    )

    // invalid retry max-attempts option value
    expectExceptionThrown(
      """
        |SELECT /*+ LOOKUP('table'='LookupTable', 'max-attempts'='100.0') */ *
        |FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        | ON T.a = D.id
        |""".stripMargin,
      "Invalid LOOKUP hint options: Could not parse value '100.0' for key 'max-attempts'",
      classOf[AssertionError]
    )

    // incomplete retry hint options
    expectExceptionThrown(
      """
        |SELECT /*+ LOOKUP('table'='LookupTable', 'max-attempts'='100') */ *
        |FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        | ON T.a = D.id
        |""".stripMargin,
      "Invalid LOOKUP hint: retry options can be both null or all not null",
      classOf[AssertionError]
    )

    // invalid retry option value
    expectExceptionThrown(
      """
        |SELECT /*+ LOOKUP('table'='LookupTable', 'retry-predicate'='exception', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s', 'max-attempts'='-3') */ *
        |FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        | ON T.a = D.id
        |""".stripMargin,
      "Invalid LOOKUP hint option: unsupported retry-predicate 'exception', only 'lookup_miss' is supported currently",
      classOf[AssertionError]
    )
    expectExceptionThrown(
      """
        |SELECT /*+ LOOKUP('table'='LookupTable', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s', 'max-attempts'='-3') */ *
        |FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        | ON T.a = D.id
        |""".stripMargin,
      "Invalid LOOKUP hint option: max-attempts value should be positive integer but was -3",
      classOf[AssertionError]
    )
    expectExceptionThrown(
      """
        |SELECT /*+ LOOKUP('table'='LookupTable', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='-10s', 'max-attempts'='3') */ *
        |FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        | ON T.a = D.id
        |""".stripMargin,
      "Invalid LOOKUP hint options: Could not parse value '-10s' for key 'fixed-delay'",
      classOf[AssertionError]
    )
    expectExceptionThrown(
      """
        |SELECT /*+ LOOKUP('table'='LookupTable', 'retry-predicate'='lookup-miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s', 'max-attempts'='3') */ *
        |FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        | ON T.a = D.id
        |""".stripMargin,
      "Invalid LOOKUP hint option: unsupported retry-predicate 'lookup-miss', only 'lookup_miss' is supported currently",
      classOf[AssertionError]
    )
    expectExceptionThrown(
      """
        |SELECT /*+ LOOKUP('table'='LookupTable', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed-delay', 'fixed-delay'='10s', 'max-attempts'='3') */ *
        |FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        | ON T.a = D.id
        |""".stripMargin,
      "Invalid LOOKUP hint options: Could not parse value 'fixed-delay' for key 'retry-strategy'",
      classOf[AssertionError]
    )
  }

  @Test
  def testJoinHintWithTableAlias(): Unit = {
    val sql =
      "SELECT /*+ LOOKUP('table'='D', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s', 'max-attempts'='3') */ * FROM MyTable AS T JOIN LookupTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"
    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinHintWithTableNameOnly(): Unit = {
    val sql = "SELECT /*+ LOOKUP('table'='LookupTable') */ * FROM MyTable AS T JOIN LookupTable " +
      "FOR SYSTEM_TIME AS OF T.proctime ON T.a = LookupTable.id"
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleJoinHintsWithSameTableName(): Unit = {
    // only the first hint will take effect
    val sql =
      """
        |SELECT /*+ LOOKUP('table'='AsyncLookupTable', 'output-mode'='allow_unordered'),
        |           LOOKUP('table'='AsyncLookupTable', 'output-mode'='ordered') */ *
        |FROM MyTable AS T
        |JOIN AsyncLookupTable FOR SYSTEM_TIME AS OF T.proctime
        | ON T.a = AsyncLookupTable.id
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleJoinHintsWithSameTableAlias(): Unit = {
    // only the first hint will take effect
    val sql =
      """
        |SELECT /*+ LOOKUP('table'='D', 'output-mode'='allow_unordered'),
        |           LOOKUP('table'='D', 'output-mode'='ordered') */ *
        |FROM MyTable AS T
        |JOIN AsyncLookupTable FOR SYSTEM_TIME AS OF T.proctime AS D 
        | ON T.a = D.id
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleJoinHintsWithDifferentTableName(): Unit = {
    // both hints on corresponding tables will take effect
    val sql =
      """
        |SELECT /*+ LOOKUP('table'='AsyncLookupTable', 'output-mode'='allow_unordered'),
        |           LOOKUP('table'='LookupTable', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s', 'max-attempts'='3') */ *
        |FROM MyTable AS T
        |JOIN AsyncLookupTable FOR SYSTEM_TIME AS OF T.proctime
        |  ON T.a = AsyncLookupTable.id
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime
        |  ON T.a = LookupTable.id
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleJoinHintsWithDifferentTableAlias(): Unit = {
    // both hints on corresponding tables will take effect
    val sql =
      """
        |SELECT /*+ LOOKUP('table'='D', 'output-mode'='allow_unordered'),
        |           LOOKUP('table'='D1', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s', 'max-attempts'='3') */ *
        |FROM MyTable AS T
        |JOIN AsyncLookupTable FOR SYSTEM_TIME AS OF T.proctime AS D 
        |  ON T.a = D.id
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D1 
        |  ON T.a = D1.id
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinSyncTableWithAsyncHint(): Unit = {
    val sql =
      "SELECT /*+ LOOKUP('table'='D', 'async'='true') */ * FROM MyTable AS T JOIN LookupTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"
    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinAsyncTableWithAsyncHint(): Unit = {
    val sql =
      "SELECT /*+ LOOKUP('table'='D', 'async'='true') */ * " +
        "FROM MyTable AS T JOIN AsyncLookupTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"
    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinAsyncTableWithSyncHint(): Unit = {
    val sql =
      "SELECT /*+ LOOKUP('table'='D', 'async'='false') */ * " +
        "FROM MyTable AS T JOIN AsyncLookupTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"
    util.verifyExecPlan(sql)
  }

  @Test
  def testAggAndLeftJoinAllowUnordered(): Unit = {
    util.tableEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_OUTPUT_MODE,
      ExecutionConfigOptions.AsyncOutputMode.ALLOW_UNORDERED)

    val stmt = util.tableEnv.asInstanceOf[TestingTableEnvironment].createStatementSet()
    stmt.addInsertSql(
      """
        |INSERT INTO Sink1
        |SELECT T.a, D.name, D.age
        |FROM (SELECT max(a) a, count(c) c, PROCTIME() proctime FROM MyTable GROUP BY b) T
        |LEFT JOIN AsyncLookupTable
        |FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id
        |""".stripMargin)

    util.verifyExplain(stmt, ExplainDetail.JSON_EXECUTION_PLAN)
  }

  def testAggAndLeftJoinWithTryResolveMode(): Unit = {
    thrown.expectMessage("Required sync lookup function by planner, but table")
    thrown.expect(classOf[TableException])

    util.tableEnv.getConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY,
      OptimizerConfigOptions.NonDeterministicUpdateStrategy.TRY_RESOLVE)

    val stmt = util.tableEnv.asInstanceOf[TestingTableEnvironment].createStatementSet()
    stmt.addInsertSql(
      """
        |INSERT INTO Sink1
        |SELECT T.a, D.name, D.age
        |FROM (SELECT max(a) a, count(c) c, PROCTIME() proctime FROM MyTable GROUP BY b) T
        |LEFT JOIN AsyncLookupTable
        |FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id
        |""".stripMargin)

    util.verifyExplain(stmt, ExplainDetail.JSON_EXECUTION_PLAN)
  }

  @Test
  def testAsyncJoinWithDefaultParams(): Unit = {
    val stmt = util.tableEnv.asInstanceOf[TestingTableEnvironment].createStatementSet()
    stmt.addInsertSql("""
                        |INSERT INTO Sink1
                        |SELECT T.a, D.name, D.age
                        |FROM MyTable T
                        |JOIN AsyncLookupTable
                        |FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id
                        |""".stripMargin)

    util.verifyExplain(stmt, ExplainDetail.JSON_EXECUTION_PLAN)
  }

  @Test
  def testJoinWithAsyncHint(): Unit = {
    val stmt = util.tableEnv.asInstanceOf[TestingTableEnvironment].createStatementSet()
    stmt.addInsertSql(
      """
        |INSERT INTO Sink1
        |SELECT /*+ LOOKUP('table'='D', 'output-mode'='allow_unordered', 'time-out'='600s', 'capacity'='300') */
        | T.a, D.name, D.age
        |FROM MyTable T
        |JOIN AsyncLookupTable
        |FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id
        |""".stripMargin)

    util.verifyExplain(stmt, ExplainDetail.JSON_EXECUTION_PLAN)
  }

  @Test
  def testJoinWithRetryHint(): Unit = {
    val stmt = util.tableEnv.asInstanceOf[TestingTableEnvironment].createStatementSet()
    stmt.addInsertSql(
      """
        |INSERT INTO Sink1
        |SELECT /*+ LOOKUP('table'='D', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s', 'max-attempts'='3') */
        | T.a, D.name, D.age
        |FROM MyTable T
        |JOIN LookupTable
        |FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id
        |""".stripMargin)

    util.verifyExplain(stmt, ExplainDetail.JSON_EXECUTION_PLAN)
  }

  @Test
  def testJoinWithAsyncAndRetryHint(): Unit = {
    val stmt = util.tableEnv.asInstanceOf[TestingTableEnvironment].createStatementSet()
    stmt.addInsertSql(
      """
        |INSERT INTO Sink1
        |SELECT /*+ LOOKUP('table'='D', 'output-mode'='allow_unordered', 'time-out'='600s', 'capacity'='300', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s', 'max-attempts'='3') */
        | T.a, D.name, D.age
        |FROM MyTable T
        |JOIN AsyncLookupTable
        |FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id
        |""".stripMargin)

    util.verifyExplain(stmt, ExplainDetail.JSON_EXECUTION_PLAN)
  }

  @Test
  def testJoinWithMixedCaseJoinHint(): Unit = {
    util.verifyExecPlan(
      """
        |SELECT /*+ LookuP('table'='D', 'retry-predicate'='lookup_miss',
        |'retry-strategy'='fixed_delay', 'fixed-delay'='155 ms', 'max-attempts'='10',
        |'async'='true', 'output-mode'='allow_unordered','capacity'='1000', 'time-out'='300 s')
        |*/
        |T.a
        |FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id
        |""".stripMargin
    )
  }

  @Test
  def testJoinHintWithNoPropagatingToSubQuery(): Unit = {
    util.verifyExecPlan(
      """
        |SELECT /*+ LOOKUP('table'='D', 'output-mode'='ordered','capacity'='200') */ T1.a
        |FROM (
        |   SELECT /*+ LOOKUP('table'='D', 'output-mode'='allow_unordered', 'capacity'='1000') */
        |     T.a a, T.proctime
        |   FROM MyTable AS T JOIN AsyncLookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |     ON T.a = D.id
        |) T1
        |JOIN AsyncLookupTable FOR SYSTEM_TIME AS OF T1.proctime AS D
        |ON T1.a=D.id
        |""".stripMargin
    )
  }

  // ==========================================================================================

  private def createLookupTable(tableName: String, lookupFunction: UserDefinedFunction): Unit = {
    if (legacyTableSource) {
      lookupFunction match {
        case tf: TableFunction[_] =>
          TestInvalidTemporalTable.createTemporaryTable(util.tableEnv, tableName, tf)
        case atf: AsyncTableFunction[_] =>
          TestInvalidTemporalTable.createTemporaryTable(util.tableEnv, tableName, atf)
      }
    } else {
      util.addTable(s"""
                       |CREATE TABLE $tableName (
                       |  `id` INT,
                       |  `name` STRING,
                       |  `age` INT,
                       |  `ts` TIMESTAMP(3)
                       |) WITH (
                       |  'connector' = 'values',
                       |  'lookup-function-class' = '${lookupFunction.getClass.getName}'
                       |)
                       |""".stripMargin)
    }
  }

  private def expectExceptionThrown(
      sql: String,
      message: String,
      clazz: Class[_ <: Throwable] = classOf[ValidationException]): Unit = {
    try {
      verifyTranslationSuccess(sql)
      fail(s"Expected a $clazz, but no exception is thrown.")
    } catch {
      case e: Throwable =>
        assertTrue(clazz.isAssignableFrom(e.getClass))
        assertThat(e, containsMessage(message))
    }
  }

  private def verifyTranslationSuccess(sql: String): Unit = {
    util.tableEnv.sqlQuery(sql).explain()
  }
}

object LookupJoinTest {
  @Parameterized.Parameters(name = "LegacyTableSource={0}")
  def parameters(): JCollection[Array[Object]] = {
    Seq[Array[AnyRef]](Array(JBoolean.TRUE), Array(JBoolean.FALSE))
  }
}

class TestTemporalTable(
    bounded: Boolean = false,
    val keys: Array[String] = Array.empty[String],
    async: Boolean = false)
  extends LookupableTableSource[RowData]
  with StreamTableSource[RowData] {

  val fieldNames = Array("id", "name", "age")
  val fieldTypes = Array(DataTypes.INT(), DataTypes.STRING(), DataTypes.INT())

  override def getLookupFunction(lookupKeys: Array[String]): TableFunction[RowData] = {
    new TableFunctionWithRowDataVarArg()
  }

  override def getAsyncLookupFunction(lookupKeys: Array[String]): AsyncTableFunction[RowData] = {
    new AsyncTableFunctionWithRowDataVarArg()
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[RowData] = {
    throw new UnsupportedOperationException(
      "This TableSource is only used for unit test, " +
        "this method should never be called.")
  }

  override def isAsyncEnabled: Boolean = async

  override def isBounded: Boolean = this.bounded

  override def getProducedDataType: DataType = buildTableSchema.toRowDataType

  override def getTableSchema: TableSchema = buildTableSchema

  private def buildTableSchema(): TableSchema = {
    val pkSet = keys.toSet
    val builder = TableSchema.builder
    assert(fieldNames.length == fieldTypes.length)
    fieldNames.zipWithIndex.foreach {
      case (name, index) =>
        if (pkSet.contains(name)) {
          // pk field requires not null
          builder.field(name, fieldTypes(index).notNull())
        } else {
          builder.field(name, fieldTypes(index))
        }
    }
    if (keys.nonEmpty) {
      builder.primaryKey(keys: _*)
    }
    builder.build()
  }
}

object TestTemporalTable {

  def createTemporaryTable(
      tEnv: TableEnvironment,
      tableName: String,
      isBounded: Boolean = false,
      async: Boolean = false): Unit = {
    val source = new TestTemporalTable(isBounded, async = async)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(tableName, source)
  }
}

class TestTemporalTableFactory extends TableSourceFactory[RowData] {

  override def createTableSource(properties: JMap[String, String]): TableSource[RowData] = {
    val dp = new DescriptorProperties
    dp.putProperties(properties)
    val isBounded = dp.getOptionalBoolean("is-bounded").orElse(false)
    new TestTemporalTable(isBounded)
  }

  override def requiredContext(): JMap[String, String] = {
    val context = new JHashMap[String, String]()
    context.put(CONNECTOR_TYPE, "TestTemporalTable")
    context
  }

  override def supportedProperties(): JList[String] = {
    val properties = new JArrayList[String]()
    properties.add("*")
    properties
  }
}

class TestInvalidTemporalTable private (
    async: Boolean,
    fetcher: TableFunction[_],
    asyncFetcher: AsyncTableFunction[_])
  extends LookupableTableSource[RowData]
  with StreamTableSource[RowData] {

  def this(fetcher: TableFunction[_]) {
    this(false, fetcher, null)
  }

  def this(asyncFetcher: AsyncTableFunction[_]) {
    this(true, null, asyncFetcher)
  }

  override def getProducedDataType: DataType = {
    TestInvalidTemporalTable.tableScheam.toRowDataType
  }

  override def getTableSchema: TableSchema = TestInvalidTemporalTable.tableScheam

  override def getLookupFunction(lookupKeys: Array[String]): TableFunction[RowData] = {
    fetcher.asInstanceOf[TableFunction[RowData]]
  }

  override def getAsyncLookupFunction(lookupKeys: Array[String]): AsyncTableFunction[RowData] = {
    asyncFetcher.asInstanceOf[AsyncTableFunction[RowData]]
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[RowData] = {
    throw new UnsupportedOperationException(
      "This TableSource is only used for unit test, " +
        "this method should never be called.")
  }

  override def isAsyncEnabled: Boolean = async
}

object TestInvalidTemporalTable {
  lazy val tableScheam = TableSchema
    .builder()
    .field("id", DataTypes.INT())
    .field("name", DataTypes.STRING())
    .field("age", DataTypes.INT())
    .field("ts", DataTypes.TIMESTAMP(3))
    .build()

  def createTemporaryTable(
      tEnv: TableEnvironment,
      tableName: String,
      fetcher: TableFunction[_]): Unit = {

    tEnv.createTemporaryTable(
      tableName,
      TableDescriptor
        .forConnector("TestInvalidTemporalTable")
        .schema(TestInvalidTemporalTable.tableScheam.toSchema)
        .option("is-async", "false")
        .option("fetcher", EncodingUtils.encodeObjectToString(fetcher))
        .build()
    )
  }

  def createTemporaryTable(
      tEnv: TableEnvironment,
      tableName: String,
      asyncFetcher: AsyncTableFunction[_]): Unit = {

    tEnv.createTemporaryTable(
      tableName,
      TableDescriptor
        .forConnector("TestInvalidTemporalTable")
        .schema(TestInvalidTemporalTable.tableScheam.toSchema)
        .option("is-async", "true")
        .option("async-fetcher", EncodingUtils.encodeObjectToString(asyncFetcher))
        .build()
    )
  }
}

class TestInvalidTemporalTableFactory extends TableSourceFactory[RowData] {

  override def createTableSource(properties: JMap[String, String]): TableSource[RowData] = {
    val dp = new DescriptorProperties
    dp.putProperties(properties)
    val async = dp.getOptionalBoolean("is-async").orElse(false)
    if (!async) {
      val fetcherBase64 = dp
        .getOptionalString("fetcher")
        .orElseThrow(new util.function.Supplier[Throwable] {
          override def get() =
            new TableException("Synchronous LookupableTableSource should provide a TableFunction.")
        })
      val fetcher = EncodingUtils.decodeStringToObject(fetcherBase64, classOf[TableFunction[_]])
      new TestInvalidTemporalTable(fetcher)
    } else {
      val asyncFetcherBase64 = dp
        .getOptionalString("async-fetcher")
        .orElseThrow(new util.function.Supplier[Throwable] {
          override def get() = new TableException(
            "Asynchronous LookupableTableSource should provide a AsyncTableFunction.")
        })
      val asyncFetcher =
        EncodingUtils.decodeStringToObject(asyncFetcherBase64, classOf[AsyncTableFunction[_]])
      new TestInvalidTemporalTable(asyncFetcher)
    }
  }

  override def requiredContext(): JMap[String, String] = {
    val context = new JHashMap[String, String]()
    context.put(CONNECTOR_TYPE, "TestInvalidTemporalTable")
    context
  }

  override def supportedProperties(): JList[String] = {
    val properties = new JArrayList[String]()
    properties.add("*")
    properties
  }
}
