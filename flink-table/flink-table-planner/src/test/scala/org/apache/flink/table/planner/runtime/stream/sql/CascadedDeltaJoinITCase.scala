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

package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.api.config.OptimizerConfigOptions.DeltaJoinStrategy
import org.apache.flink.table.planner.factories.TestValuesRuntimeFunctions.AsyncTestValueLookupFunction
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{BeforeEach, TestTemplate}

import scala.collection.JavaConversions._

/** Tests for multi cascaded delta join with multi tables. */
class CascadedDeltaJoinITCase(enableCache: Boolean) extends DeltaJoinITCaseBase(enableCache) {

  @BeforeEach
  override def before(): Unit = {
    super.before()

    tEnv.getConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
      DeltaJoinStrategy.FORCE)

    tEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_CACHE_ENABLED,
      Boolean.box(enableCache))

    AsyncTestValueLookupFunction.invokeCount.set(0)

    tEnv.executeSql(s"""
                       |create table sink1(
                       |  a1 int,
                       |  c0 double,
                       |  c2 string,
                       |  a2 string,
                       |  b2 string,
                       |  primary key (a1, c0) not enforced
                       |) with (
                       |  'connector' = 'values',
                       |  'bounded' = 'false',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    tEnv.executeSql(s"""
                       |create table sink2(
                       |  a1 int,
                       |  c0 double,
                       |  d1 int,
                       |  c2 string,
                       |  d2 string,
                       |  a2 string,
                       |  b2 string,
                       |  primary key (a1, c0, d1) not enforced
                       |) with (
                       |  'connector' = 'values',
                       |  'bounded' = 'false',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)
  }

  @TestTemplate
  def testLHS1(): Unit = {
    //        DT2
    //      /    \
    //    DT1     C
    //  /    \
    // A      B
    // when records from C come, lookup chain is:
    // C -> B -> A
    prepareThreeTables()

    val sql =
      """
        |insert into sink1
        |select a1, c0, c2, a2, b2
        | from A
        |join B
        | on a1 = b1 and a0 = b0
        |join C
        | on c1 = b1 and c1 <> 1
        |
        |""".stripMargin
    tEnv.executeSql(sql).await()

    val expected = List("+I[2, 2.0, c-2-2, a-2, b-2-2]", "+I[2, 3.0, c-3, a-2, b-2-2]")

    val result = TestValuesTableFactory.getResultsAsStrings("sink1")
    assertThat(result.sorted).isEqualTo(expected.sorted)

    // | DT           | Lookup Count Without Cache  | Lookup Count With Cache |
    // | ------------ | --------------------------- | ----------------------- |
    // | DT1          | 2 + 4 = 6                   | 2 + 3 = 5               |
    // | DT2          | 3 + 9 = 12                  | 1 + 5 = 6               |
    // | TOTAL        | 18                          | 11                      |
    // | ------------ | --------------------------- | ----------------------- |
    assertThat(AsyncTestValueLookupFunction.invokeCount.get())
      .isEqualTo(if (enableCache) 11 else 18)
  }

  @TestTemplate
  def testLHS2(): Unit = {
    //        DT2
    //      /    \
    //    DT1     C
    //  /    \
    // A      B
    // when records from C come, lookup chain is:
    // C -> A -> B
    prepareThreeTables()

    val sql =
      """
        |insert into sink1
        |select a1, c0, c2, a2, b2
        | from A
        |join B
        | on a1 = b1 and a0 = b0
        |join C
        | on c1 = a1 and c1 <> 1
        |
        |""".stripMargin
    tEnv.executeSql(sql).await()

    val expected = List("+I[2, 2.0, c-2-2, a-2, b-2-2]", "+I[2, 3.0, c-3, a-2, b-2-2]")

    val result = TestValuesTableFactory.getResultsAsStrings("sink1")
    assertThat(result.sorted).isEqualTo(expected.sorted)

    // | DT           | Lookup Count Without Cache  | Lookup Count With Cache |
    // | ------------ | --------------------------- | ----------------------- |
    // | DT1          | 2 + 4 = 6                   | 2 + 3 = 5               |
    // | DT2          | 3 + 9 = 12                  | 1 + 5 = 6               |
    // | TOTAL        | 18                          | 11                      |
    // | ------------ | --------------------------- | ----------------------- |
    assertThat(AsyncTestValueLookupFunction.invokeCount.get())
      .isEqualTo(if (enableCache) 11 else 18)
  }

  @TestTemplate
  def testMultiLHS1(): Unit = {
    //           DT3
    //         /    \
    //        DT2     D
    //      /    \
    //    DT1     C
    //  /    \
    // A      B
    // when records from C come, lookup chain is:
    // C -> B -> A
    // when records from D come, lookup chain is:
    // D -> C -> B -> A
    prepareFourTables()
    val sql =
      """
        |insert into sink2
        |select a1, c0, d1, c2, d2, a2, b2
        | from A
        |join B
        | on a1 = b1 and a0 = b0
        |join C
        | on c1 = b1 and c1 <> 2
        |join D
        | on d1 = c1
        |
        |""".stripMargin
    tEnv.executeSql(sql).await()

    val expected =
      List("+I[1, 1.0, 1, c-1, d-2, a-1-2, b-1-2]", "+I[1, 3.0, 1, c-3, d-2, a-1-2, b-1-2]")

    val result = TestValuesTableFactory.getResultsAsStrings("sink2")
    assertThat(result.sorted).isEqualTo(expected.sorted)

    // | DT           | Lookup Count Without Cache  | Lookup Count With Cache |
    // | ------------ | --------------------------- | ----------------------- |
    // | DT1          | 3 + 4 = 7                   | 2 + 3 = 5               |
    // | DT2          | 4 + 9 = 13                  | 1 + 5 = 6               |
    // | DT3          | 10 + 10 = 20                | 1 + 7 = 8               |
    // | TOTAL        | 40                          | 19                      |
    // | ------------ | --------------------------- | ----------------------- |
    assertThat(AsyncTestValueLookupFunction.invokeCount.get())
      .isEqualTo(if (enableCache) 19 else 40)
  }

  @TestTemplate
  def testMultiLHS2(): Unit = {
    //           DT3
    //         /    \
    //        DT2     D
    //      /    \
    //    DT1     C
    //  /    \
    // A      B
    // when records from C come, lookup chain is:
    // C -> B -> A
    // when records from D come, lookup chain is:
    // D -> A -> B -> C
    prepareFourTables()
    val sql =
      """
        |insert into sink2
        |select a1, c0, d1, c2, d2, a2, b2
        | from A
        |join B
        | on a1 = b1 and a0 = b0
        |join C
        | on c1 = b1 and c1 <> 2
        |join D
        | on d1 = a1
        |
        |""".stripMargin
    tEnv.executeSql(sql).await()

    val expected =
      List("+I[1, 1.0, 1, c-1, d-2, a-1-2, b-1-2]", "+I[1, 3.0, 1, c-3, d-2, a-1-2, b-1-2]")

    val result = TestValuesTableFactory.getResultsAsStrings("sink2")
    assertThat(result.sorted).isEqualTo(expected.sorted)

    // | DT           | Lookup Count Without Cache  | Lookup Count With Cache |
    // | ------------ | --------------------------- | ----------------------- |
    // | DT1          | 3 + 4 = 7                   | 2 + 3 = 5               |
    // | DT2          | 4 + 9 = 13                  | 1 + 5 = 6               |
    // | DT3          | 10 + 9 = 19                 | 1 + 6 = 7               |
    // | TOTAL        | 39                          | 18                      |
    // | ------------ | --------------------------- | ----------------------- |
    assertThat(AsyncTestValueLookupFunction.invokeCount.get())
      .isEqualTo(if (enableCache) 18 else 39)
  }

  @TestTemplate
  def testMultiLHS3(): Unit = {
    //           DT3
    //         /    \
    //        DT2     D
    //      /    \
    //    DT1     C
    //  /    \
    // A      B
    // when records from C come, lookup chain is:
    // C -> B -> A
    // when records from D come, lookup chain is:
    // D -> B -> A -> C
    prepareFourTables()
    val sql =
      """
        |insert into sink2
        |select a1, c0, d1, c2, d2, a2, b2
        | from A
        |join B
        | on a1 = b1 and a0 = b0
        |join C
        | on c1 = b1 and c1 <> 2
        |join D
        | on d1 = b1
        |
        |""".stripMargin
    tEnv.executeSql(sql).await()

    val expected =
      List("+I[1, 1.0, 1, c-1, d-2, a-1-2, b-1-2]", "+I[1, 3.0, 1, c-3, d-2, a-1-2, b-1-2]")

    val result = TestValuesTableFactory.getResultsAsStrings("sink2")
    assertThat(result.sorted).isEqualTo(expected.sorted)

    // | DT           | Lookup Count Without Cache  | Lookup Count With Cache |
    // | ------------ | --------------------------- | ----------------------- |
    // | DT1          | 3 + 4 = 7                   | 2 + 3 = 5               |
    // | DT2          | 4 + 9 = 13                  | 1 + 5 = 6               |
    // | DT3          | 10 + 9 = 19                 | 1 + 6 = 7               |
    // | TOTAL        | 39                          | 18                      |
    // | ------------ | --------------------------- | ----------------------- |
    assertThat(AsyncTestValueLookupFunction.invokeCount.get())
      .isEqualTo(if (enableCache) 18 else 39)
  }

  @TestTemplate
  def testRHS1(): Unit = {
    //      DT2
    //     /    \
    //    C     DT1
    //         /    \
    //        A      B
    // when records from C come, lookup chain is:
    // C -> A -> B
    prepareThreeTables()

    tEnv.executeSql("""
                      |create temporary view myv as
                      |select *
                      | from A
                      |join B
                      | on a1 = b1 and a0 <> b0
                      |""".stripMargin)

    tEnv
      .executeSql("""
                    |insert into sink1
                    |select a1, c0, c2, a2, b2
                    | from C
                    |join myv
                    | on c1 = a1 and c1 <> 2
                    |""".stripMargin)
      .await()

    val expected = List("+I[3, 32.0, c-3-2, a-3, b-3]")

    val result = TestValuesTableFactory.getResultsAsStrings("sink1")
    assertThat(result.sorted).isEqualTo(expected.sorted)

    // | DT           | Lookup Count Without Cache  | Lookup Count With Cache |
    // | ------------ | --------------------------- | ----------------------- |
    // | DT1          | 2 + 4 = 6                   | 2 + 3 = 5               |
    // | DT2          | 5 + 2 = 7                   | 5 + 1 = 6               |
    // | TOTAL        | 13                          | 11                      |
    // | ------------ | --------------------------- | ----------------------- |
    assertThat(AsyncTestValueLookupFunction.invokeCount.get())
      .isEqualTo(if (enableCache) 11 else 13)
  }

  @TestTemplate
  def testRHS2(): Unit = {
    //      DT2
    //     /    \
    //    C     DT1
    //         /    \
    //        A      B
    // when records from C come, lookup chain is:
    // C -> B -> A
    prepareThreeTables()

    tEnv.executeSql("""
                      |create temporary view myv as
                      |select *
                      | from A
                      |join B
                      | on a1 = b1 and a0 <> b0
                      |""".stripMargin)

    tEnv
      .executeSql("""
                    |insert into sink1
                    |select a1, c0, c2, a2, b2
                    | from C
                    |join myv
                    | on c1 = b1 and c1 <> 2
                    |""".stripMargin)
      .await()

    val expected = List("+I[3, 32.0, c-3-2, a-3, b-3]")

    val result = TestValuesTableFactory.getResultsAsStrings("sink1")
    assertThat(result.sorted).isEqualTo(expected.sorted)

    // | DT           | Lookup Count Without Cache  | Lookup Count With Cache |
    // | ------------ | --------------------------- | ----------------------- |
    // | DT1          | 2 + 4 = 6                   | 2 + 3 = 5               |
    // | DT2          | 5 + 2 = 7                   | 5 + 1 = 6               |
    // | TOTAL        | 13                          | 11                      |
    // | ------------ | --------------------------- | ----------------------- |
    assertThat(AsyncTestValueLookupFunction.invokeCount.get())
      .isEqualTo(if (enableCache) 11 else 13)
  }

  @TestTemplate
  def testMultiRHS1(): Unit = {
    //     DT3
    //    /    \
    //   D    DT2
    //       /    \
    //      C    DT1
    //          /    \
    //         A      B
    // when records from C come, lookup chain is:
    // C -> B -> A
    // when records from C come, lookup chain is:
    // D -> C -> B -> A
    prepareFourTables()

    tEnv.executeSql("""
                      |create temporary view dt1 as
                      |select * from A
                      |join B
                      | on a1 = b1 and a0 <> b0
                      |""".stripMargin)

    tEnv.executeSql("""
                      |create temporary view dt2 as
                      |select * from C
                      |join dt1
                      | on c1 = b1 and c1 <> 2
                      |""".stripMargin)

    tEnv
      .executeSql("""
                    |insert into sink2
                    |select a1, c0, d1, c2, d2, a2, b2
                    | from D
                    |join dt2
                    | on d1 = c1
                    |""".stripMargin)
      .await()

    val expected =
      List("+I[3, 32.0, 3, c-3-2, d-3, a-3, b-3]", "+I[3, 33.0, 3, c-3-3, d-3, a-3, b-3]")

    val result = TestValuesTableFactory.getResultsAsStrings("sink2")
    assertThat(result.sorted).isEqualTo(expected.sorted)

    // | DT           | Lookup Count Without Cache  | Lookup Count With Cache |
    // | ------------ | --------------------------- | ----------------------- |
    // | DT1          | 3 + 4 = 7                   | 2 + 3 = 5               |
    // | DT2          | 9 + 2 = 11                  | 5 + 1 = 6               |
    // | DT3          | 10 + 6 = 16                 | 7 + 1 = 8               |
    // | TOTAL        | 34                          | 19                      |
    // | ------------ | --------------------------- | ----------------------- |
    assertThat(AsyncTestValueLookupFunction.invokeCount.get())
      .isEqualTo(if (enableCache) 19 else 34)
  }

  @TestTemplate
  def testMultiRHS2(): Unit = {
    //     DT3
    //    /    \
    //   D    DT2
    //       /    \
    //      C    DT1
    //          /    \
    //         A      B
    // when records from C come, lookup chain is:
    // C -> B -> A
    // when records from C come, lookup chain is:
    // D -> A -> B -> C
    prepareFourTables()

    tEnv.executeSql("""
                      |create temporary view dt1 as
                      |select * from A
                      |join B
                      | on a1 = b1 and a0 <> b0
                      |""".stripMargin)

    tEnv.executeSql("""
                      |create temporary view dt2 as
                      |select * from C
                      |join dt1
                      | on c1 = b1 and c1 <> 2
                      |""".stripMargin)

    tEnv
      .executeSql("""
                    |insert into sink2
                    |select a1, c0, d1, c2, d2, a2, b2
                    | from D
                    |join dt2
                    | on d1 = a1
                    |""".stripMargin)
      .await()

    val expected =
      List("+I[3, 32.0, 3, c-3-2, d-3, a-3, b-3]", "+I[3, 33.0, 3, c-3-3, d-3, a-3, b-3]")

    val result = TestValuesTableFactory.getResultsAsStrings("sink2")
    assertThat(result.sorted).isEqualTo(expected.sorted)

    // | DT           | Lookup Count Without Cache  | Lookup Count With Cache |
    // | ------------ | --------------------------- | ----------------------- |
    // | DT1          | 3 + 4 = 7                   | 2 + 3 = 5               |
    // | DT2          | 9 + 2 = 11                  | 5 + 1 = 6               |
    // | DT3          | 8 + 6 = 14                  | 6 + 1 = 7               |
    // | TOTAL        | 32                          | 18                      |
    // | ------------ | --------------------------- | ----------------------- |
    assertThat(AsyncTestValueLookupFunction.invokeCount.get())
      .isEqualTo(if (enableCache) 18 else 32)
  }

  @TestTemplate
  def testMultiRHS3(): Unit = {
    //     DT3
    //    /    \
    //   D    DT2
    //       /    \
    //      C    DT1
    //          /    \
    //         A      B
    // when records from C come, lookup chain is:
    // C -> B -> A
    // when records from C come, lookup chain is:
    // D -> B -> A -> C
    prepareFourTables()

    tEnv.executeSql("""
                      |create temporary view dt1 as
                      |select * from A
                      |join B
                      | on a1 = b1 and a0 <> b0
                      |""".stripMargin)

    tEnv.executeSql("""
                      |create temporary view dt2 as
                      |select * from C
                      |join dt1
                      | on c1 = b1 and c1 <> 2
                      |""".stripMargin)

    tEnv
      .executeSql("""
                    |insert into sink2
                    |select a1, c0, d1, c2, d2, a2, b2
                    | from D
                    |join dt2
                    | on d1 = b1
                    |""".stripMargin)
      .await()

    val expected =
      List("+I[3, 32.0, 3, c-3-2, d-3, a-3, b-3]", "+I[3, 33.0, 3, c-3-3, d-3, a-3, b-3]")

    val result = TestValuesTableFactory.getResultsAsStrings("sink2")
    assertThat(result.sorted).isEqualTo(expected.sorted)

    // | DT           | Lookup Count Without Cache  | Lookup Count With Cache |
    // | ------------ | --------------------------- | ----------------------- |
    // | DT1          | 3 + 4 = 7                   | 2 + 3 = 5               |
    // | DT2          | 9 + 2 = 11                  | 5 + 1 = 6               |
    // | DT3          | 8 + 6 = 14                  | 6 + 1 = 7               |
    // | TOTAL        | 32                          | 18                      |
    // | ------------ | --------------------------- | ----------------------- |
    assertThat(AsyncTestValueLookupFunction.invokeCount.get())
      .isEqualTo(if (enableCache) 18 else 32)
  }

  @TestTemplate
  def testBushy1(): Unit = {
    //        DT3
    //      /      \
    //    DT1      DT2
    //  /    \    /    \
    // A      B  C      D
    // when records from DT1 come, lookup chain is:
    // DT-1 -> C -> D
    // when records from DT2 come, lookup chain is:
    // DT-2 -> B -> A
    prepareFourTables()

    tEnv.executeSql("""
                      |create temporary view dt1 as
                      |select * from A
                      |join B
                      | on a1 = b1 and a0 <> b0
                      |""".stripMargin)

    tEnv.executeSql("""
                      |create temporary view dt2 as
                      |select * from C
                      |join D
                      | on c1 = d1 and c0 <> 32.0
                      |""".stripMargin)

    tEnv
      .executeSql(
        """
          |insert into sink2
          |select a1, c0, d1, c2, d2, a2, b2
          | from dt1
          |join dt2
          | on c1 = b1
          |""".stripMargin
      )
      .await()

    val expected = List("+I[3, 33.0, 3, c-3-3, d-3, a-3, b-3]")

    val result = TestValuesTableFactory.getResultsAsStrings("sink2")
    assertThat(result.sorted).isEqualTo(expected.sorted)

    // | DT           | Lookup Count Without Cache  | Lookup Count With Cache |
    // | ------------ | --------------------------- | ----------------------- |
    // | DT1          | 4 + 5 = 9                   | 3 + 4 = 7               |
    // | DT2          | 5 + 4 = 9                   | 4 + 3 = 7               |
    // | DT3          | 4 + 16 = 20                 | 2 + 4 = 6               |
    // | TOTAL        | 38                          | 20                      |
    // | ------------ | --------------------------- | ----------------------- |
    assertThat(AsyncTestValueLookupFunction.invokeCount.get())
      .isEqualTo(if (enableCache) 20 else 38)
  }

  @TestTemplate
  def testBushy2(): Unit = {
    //        DT3
    //      /      \
    //    DT1      DT2
    //  /    \    /    \
    // A      B  C      D
    // when records from DT1 come, lookup chain is:
    // DT-1 -> D -> C
    // when records from DT2 come, lookup chain is:
    // DT-2 -> A -> B
    prepareFourTables()

    tEnv.executeSql("""
                      |create temporary view dt1 as
                      |select * from A
                      |join B
                      | on a1 = b1 and a0 <> b0
                      |""".stripMargin)

    tEnv.executeSql("""
                      |create temporary view dt2 as
                      |select * from C
                      |join D
                      | on c1 = d1 and c0 <> 32.0
                      |""".stripMargin)

    tEnv
      .executeSql(
        """
          |insert into sink2
          |select a1, c0, d1, c2, d2, a2, b2
          | from dt1
          |join dt2
          | on d1 = a1
          |""".stripMargin
      )
      .await()

    val expected = List("+I[3, 33.0, 3, c-3-3, d-3, a-3, b-3]")

    val result = TestValuesTableFactory.getResultsAsStrings("sink2")
    assertThat(result.sorted).isEqualTo(expected.sorted)

    // | DT           | Lookup Count Without Cache  | Lookup Count With Cache |
    // | ------------ | --------------------------- | ----------------------- |
    // | DT1          | 4 + 5 = 9                   | 3 + 4 = 7               |
    // | DT2          | 5 + 4 = 9                   | 4 + 3 = 7               |
    // | DT3          | 4 + 16 = 20                 | 2 + 4 = 6               |
    // | TOTAL        | 38                          | 20                      |
    // | ------------ | --------------------------- | ----------------------- |
    assertThat(AsyncTestValueLookupFunction.invokeCount.get())
      .isEqualTo(if (enableCache) 20 else 38)
  }

  @TestTemplate
  def testCalcExistsBothBetweenSourceAndJoinAndCascadedJoins(): Unit = {
    //        DT2
    //      /    \
    //    DT1     C
    //  /    \
    // A      B
    // when records from C come, lookup chain is:
    // C -> B -> A
    prepareThreeTables()

    tEnv.executeSql("""
                      |create temporary view dt1 as
                      |select
                      |   a0, a1, b1,
                      |   trim(a2) as new_a2,
                      |   concat_ws('~', a2, b2) as ab2
                      | from A
                      |join B
                      | on a1 = b1 and b0 <> 1.0
                      |""".stripMargin)

    val sql =
      """
        |insert into sink1
        |select a1, c0, c2, new_a2, ab2
        | from dt1
        |join C
        | on c1 = b1 and c0 <> 3.0
        |""".stripMargin
    tEnv.executeSql(sql).await()

    val expected = List("+I[2, 2.0, c-2-2, a-2, a-2~b-2-2]", "+I[3, 32.0, c-3-2, a-3, a-3~b-3]")

    val result = TestValuesTableFactory.getResultsAsStrings("sink1")
    assertThat(result.sorted).isEqualTo(expected.sorted)

    // | DT           | Lookup Count Without Cache  | Lookup Count With Cache |
    // | ------------ | --------------------------- | ----------------------- |
    // | DT1          | 3 + 4 = 7                   | 3 + 3 = 6               |
    // | DT2          | 4 + 9 = 13                  | 2 + 6 = 8               |
    // | TOTAL        | 20                          | 14                      |
    // | ------------ | --------------------------- | ----------------------- |
    assertThat(AsyncTestValueLookupFunction.invokeCount.get())
      .isEqualTo(if (enableCache) 14 else 20)
  }

  @TestTemplate
  def testConsecutiveOneToManyJoins(): Unit = {
    //        DT2
    //      /    \
    //    DT1     C
    //  /    \
    // A      B
    // when records from C come, lookup chain is:
    // C -> B -> A
    prepareSrcTableWithData(
      "A",
      List("a0 double primary key not enforced", "a1 int", "a2 string"),
      List(
        changelogRow("+I", Double.box(1.0), Int.box(1), String.valueOf("a-1")),
        changelogRow("+I", Double.box(2.0), Int.box(2), String.valueOf("a-2"))
      ),
      List(List("a1")),
      List("a1")
    )
    prepareSrcTableWithData(
      "B",
      List("b0 double primary key not enforced", "b1 int", "b2 string"),
      List(
        changelogRow("+I", Double.box(1.0), Int.box(1), String.valueOf("b-1")),
        changelogRow("+I", Double.box(11.0), Int.box(1), String.valueOf("b-11")),
        changelogRow("+I", Double.box(2.0), Int.box(2), String.valueOf("b-2")),
        changelogRow("+I", Double.box(22.0), Int.box(2), String.valueOf("b-22"))
      ),
      List(List("b1")),
      List("b1")
    )
    prepareSrcTableWithData(
      "C",
      List("c0 double primary key not enforced", "c1 int", "c2 string"),
      List(
        changelogRow("+I", Double.box(1.0), Int.box(1), String.valueOf("c-1")),
        changelogRow("+I", Double.box(11.0), Int.box(1), String.valueOf("c-11")),
        changelogRow("+I", Double.box(2.0), Int.box(2), String.valueOf("c-2")),
        changelogRow("+I", Double.box(22.0), Int.box(2), String.valueOf("c-22"))
      ),
      List(List("c1")),
      List("c1")
    )

    tEnv.executeSql(s"""
                       |create table tmp_sink(
                       |  a0 double,
                       |  b0 double,
                       |  c0 double,
                       |  abc2 string,
                       |  primary key (a0, b0, c0) not enforced
                       |) with (
                       |  'connector' = 'values',
                       |  'bounded' = 'false',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val sql =
      """
        |insert into tmp_sink
        |select a0, b0, c0, concat_ws('~', a2, b2, c2) as abc2
        | from A
        |join B
        | on a1 = b1
        |join C
        | on c1 = b1
        |
        |""".stripMargin
    tEnv.executeSql(sql).await()

    val expected = List(
      "+I[1.0, 1.0, 1.0, a-1~b-1~c-1]",
      "+I[1.0, 1.0, 11.0, a-1~b-1~c-11]",
      "+I[1.0, 11.0, 1.0, a-1~b-11~c-1]",
      "+I[1.0, 11.0, 11.0, a-1~b-11~c-11]",
      "+I[2.0, 2.0, 2.0, a-2~b-2~c-2]",
      "+I[2.0, 2.0, 22.0, a-2~b-2~c-22]",
      "+I[2.0, 22.0, 2.0, a-2~b-22~c-2]",
      "+I[2.0, 22.0, 22.0, a-2~b-22~c-22]"
    )

    val result = TestValuesTableFactory.getResultsAsStrings("tmp_sink")
    assertThat(result.sorted).isEqualTo(expected.sorted)

    // | DT           | Lookup Count Without Cache  | Lookup Count With Cache |
    // | ------------ | --------------------------- | ----------------------- |
    // | DT1          | 2 + 4 = 6                   | 2 + 2 = 4               |
    // | DT2          | 8 + 8 = 16                  | 2 + 4 = 6               |
    // | TOTAL        | 22                          | 10                      |
    // | ------------ | --------------------------- | ----------------------- |
    assertThat(AsyncTestValueLookupFunction.invokeCount.get())
      .isEqualTo(if (enableCache) 10 else 22)
  }

  @TestTemplate
  def testJoinTwoTablesWhileJoinKeyChanged1(): Unit = {
    //        DT2
    //      /    \
    //    DT1     C
    //  /    \
    // A      B
    // when records from C come, lookup chain is:
    // C -> B -> A
    prepareSrcTableWithData(
      "A",
      List("a0 double primary key not enforced", "a_key string", "a_b_key string"),
      List(
        changelogRow("+I", Double.box(1.0), String.valueOf("a-1"), String.valueOf("b-1")),
        changelogRow("+I", Double.box(2.0), String.valueOf("a-2"), String.valueOf("b-2"))
      ),
      List(List("a_b_key")),
      List("a_b_key")
    )
    prepareSrcTableWithData(
      "B",
      List("b0 double primary key not enforced", "b_key string", "b_c_key string"),
      List(
        changelogRow("+I", Double.box(1.0), String.valueOf("b-1"), String.valueOf("c-1")),
        changelogRow("+I", Double.box(11.0), String.valueOf("b-1"), String.valueOf("c-2")),
        changelogRow("+I", Double.box(2.0), String.valueOf("b-2"), String.valueOf("c-1")),
        changelogRow("+I", Double.box(22.0), String.valueOf("b-2"), String.valueOf("c-2"))
      ),
      List(List("b_key"), List("b_c_key")),
      List("b_key", "b_c_key")
    )
    prepareSrcTableWithData(
      "C",
      List("c0 double primary key not enforced", "c_key string"),
      List(
        changelogRow("+I", Double.box(1.0), String.valueOf("c-1")),
        changelogRow("+I", Double.box(2.0), String.valueOf("c-2"))
      ),
      List(List("c_key")),
      List("c_key")
    )

    tEnv.executeSql(s"""
                       |create table tmp_sink(
                       |  a0 double,
                       |  b0 double,
                       |  c0 double,
                       |  abc_key string,
                       |  primary key (a0, b0, c0) not enforced
                       |) with (
                       |  'connector' = 'values',
                       |  'bounded' = 'false',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val sql =
      """
        |insert into tmp_sink
        |select a0, b0, c0, concat_ws('~', a_key, b_key, c_key) as abc_key
        | from A
        |join B
        | on a_b_key = b_key
        |join C
        | on c_key = b_c_key
        |
        |""".stripMargin
    tEnv.executeSql(sql).await()

    val expected = List(
      "+I[1.0, 1.0, 1.0, a-1~b-1~c-1]",
      "+I[1.0, 11.0, 2.0, a-1~b-1~c-2]",
      "+I[2.0, 2.0, 1.0, a-2~b-2~c-1]",
      "+I[2.0, 22.0, 2.0, a-2~b-2~c-2]"
    )

    val result = TestValuesTableFactory.getResultsAsStrings("tmp_sink")
    assertThat(result.sorted).isEqualTo(expected.sorted)

    // | DT           | Lookup Count Without Cache  | Lookup Count With Cache |
    // | ------------ | --------------------------- | ----------------------- |
    // | DT1          | 2 + 4 = 6                   | 2 + 2 = 4               |
    // | DT2          | 8 + 6 = 14                  | 2 + 6 = 8               |
    // | TOTAL        | 20                          | 12                      |
    // | ------------ | --------------------------- | ----------------------- |
    assertThat(AsyncTestValueLookupFunction.invokeCount.get())
      .isEqualTo(if (enableCache) 12 else 20)
  }

  @TestTemplate
  def testJoinTwoTablesWhileJoinKeyChanged2(): Unit = {
    //        DT2
    //      /    \
    //    DT1     C
    //  /    \
    // A      B
    // when records from C come, lookup chain is:
    // C -> A -> B
    prepareSrcTableWithData(
      "A",
      List(
        "a0 double primary key not enforced",
        "a_key string",
        "a_b_key string",
        "a_c_key string"),
      List(
        changelogRow(
          "+I",
          Double.box(1.0),
          String.valueOf("a-1"),
          String.valueOf("b-1"),
          String.valueOf("c-1")),
        changelogRow(
          "+I",
          Double.box(2.0),
          String.valueOf("a-2"),
          String.valueOf("b-2"),
          String.valueOf("c-2"))
      ),
      List(List("a_b_key"), List("a_c_key")),
      List("a_b_key", "a_c_key")
    )
    prepareSrcTableWithData(
      "B",
      List("b0 double primary key not enforced", "b_key string"),
      List(
        changelogRow("+I", Double.box(1.0), String.valueOf("b-1")),
        changelogRow("+I", Double.box(11.0), String.valueOf("b-1")),
        changelogRow("+I", Double.box(2.0), String.valueOf("b-2")),
        changelogRow("+I", Double.box(22.0), String.valueOf("b-2"))
      ),
      List(List("b_key")),
      List("b_key")
    )
    prepareSrcTableWithData(
      "C",
      List("c0 double primary key not enforced", "c_key string"),
      List(
        changelogRow("+I", Double.box(1.0), String.valueOf("c-1")),
        changelogRow("+I", Double.box(2.0), String.valueOf("c-2"))
      ),
      List(List("c_key")),
      List("c_key")
    )

    tEnv.executeSql(s"""
                       |create table tmp_sink(
                       |  a0 double,
                       |  b0 double,
                       |  c0 double,
                       |  abc_key string,
                       |  primary key (a0, b0, c0) not enforced
                       |) with (
                       |  'connector' = 'values',
                       |  'bounded' = 'false',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val sql =
      """
        |insert into tmp_sink
        |select a0, b0, c0, concat_ws('~', a_key, a_b_key, a_c_key) as abc_key
        | from A
        |join B
        | on a_b_key = b_key
        |join C
        | on a_c_key = c_key
        |
        |""".stripMargin
    tEnv.executeSql(sql).await()

    val expected = List(
      "+I[1.0, 1.0, 1.0, a-1~b-1~c-1]",
      "+I[1.0, 11.0, 1.0, a-1~b-1~c-1]",
      "+I[2.0, 2.0, 2.0, a-2~b-2~c-2]",
      "+I[2.0, 22.0, 2.0, a-2~b-2~c-2]"
    )

    val result = TestValuesTableFactory.getResultsAsStrings("tmp_sink")
    assertThat(result.sorted).isEqualTo(expected.sorted)

    // | DT           | Lookup Count Without Cache  | Lookup Count With Cache |
    // | ------------ | --------------------------- | ----------------------- |
    // | DT1          | 2 + 4 = 6                   | 2 + 2 = 4               |
    // | DT2          | 8 + 4 = 12                  | 2 + 4 = 6               |
    // | TOTAL        | 18                          | 10                      |
    // | ------------ | --------------------------- | ----------------------- |
    assertThat(AsyncTestValueLookupFunction.invokeCount.get())
      .isEqualTo(if (enableCache) 10 else 18)
  }

  private def prepareThreeTables(): Unit = {
    prepareSrcTableWithData(
      "A",
      List("a0 double", "a1 int primary key not enforced", "a2 string"),
      List(
        changelogRow("+I", Double.box(1.0), Int.box(1), String.valueOf("a-1")),
        changelogRow("+I", Double.box(2.0), Int.box(2), String.valueOf("a-2")),
        changelogRow("+I", Double.box(3.0), Int.box(3), String.valueOf("a-3"))
      ),
      List(List("a1")),
      List("a0")
    )
    prepareSrcTableWithData(
      "B",
      List("b1 int primary key not enforced", "b0 double", "b2 string"),
      List(
        changelogRow("+I", Int.box(1), Double.box(1.0), String.valueOf("b-1")),
        changelogRow("-U", Int.box(1), Double.box(1.0), String.valueOf("b-1")),
        changelogRow("+U", Int.box(1), Double.box(1.0), String.valueOf("b-1-2")),
        changelogRow("+I", Int.box(2), Double.box(2.0), String.valueOf("b-2")),
        changelogRow("-U", Int.box(2), Double.box(2.0), String.valueOf("b-2")),
        changelogRow("+U", Int.box(2), Double.box(2.0), String.valueOf("b-2-2")),
        changelogRow("+I", Int.box(3), Double.box(4.0), String.valueOf("b-3")),
        changelogRow("+I", Int.box(13), Double.box(13.0), String.valueOf("b-13"))
      ),
      List(List("b1")),
      List("b0")
    )
    prepareSrcTableWithData(
      "C",
      List("c1 int", "c2 string", "c0 double primary key not enforced"),
      List(
        changelogRow("+I", Int.box(1), String.valueOf("c-1"), Double.box(1.0)),
        changelogRow("+I", Int.box(2), String.valueOf("c-2"), Double.box(2.0)),
        changelogRow("-U", Int.box(2), String.valueOf("c-2"), Double.box(2.0)),
        changelogRow("+U", Int.box(2), String.valueOf("c-2-2"), Double.box(2.0)),
        changelogRow("+I", Int.box(2), String.valueOf("c-3"), Double.box(3.0)),
        changelogRow("+I", Int.box(3), String.valueOf("c-3-2"), Double.box(32.0)),
        changelogRow("+I", Int.box(23), String.valueOf("c-23"), Double.box(23.0))
      ),
      List(List("c1")),
      List("c1")
    )
  }

  private def prepareFourTables(): Unit = {
    prepareSrcTableWithData(
      "A",
      List("a0 double", "a1 int primary key not enforced", "a2 string"),
      List(
        changelogRow("+I", Double.box(1.0), Int.box(1), String.valueOf("a-1")),
        changelogRow("-U", Double.box(1.0), Int.box(1), String.valueOf("a-1")),
        changelogRow("+U", Double.box(1.0), Int.box(1), String.valueOf("a-1-2")),
        changelogRow("+I", Double.box(2.0), Int.box(2), String.valueOf("a-2")),
        changelogRow("+I", Double.box(3.0), Int.box(3), String.valueOf("a-3"))
      ),
      List(List("a1")),
      List("a0")
    )

    prepareSrcTableWithData(
      "B",
      List("b1 int primary key not enforced", "b0 double", "b2 string"),
      List(
        changelogRow("+I", Int.box(1), Double.box(1.0), String.valueOf("b-1")),
        changelogRow("-U", Int.box(1), Double.box(1.0), String.valueOf("b-1")),
        changelogRow("+U", Int.box(1), Double.box(1.0), String.valueOf("b-1-2")),
        changelogRow("+I", Int.box(2), Double.box(2.0), String.valueOf("b-2")),
        changelogRow("+I", Int.box(3), Double.box(4.0), String.valueOf("b-3")),
        changelogRow("+I", Int.box(13), Double.box(13.0), String.valueOf("b-13"))
      ),
      List(List("b1")),
      List("b0")
    )

    prepareSrcTableWithData(
      "C",
      List("c1 int", "c2 string", "c0 double primary key not enforced"),
      List(
        changelogRow("+I", Int.box(1), String.valueOf("c-1"), Double.box(1.0)),
        changelogRow("+I", Int.box(2), String.valueOf("c-2"), Double.box(2.0)),
        changelogRow("+I", Int.box(1), String.valueOf("c-3"), Double.box(3.0)),
        changelogRow("+I", Int.box(3), String.valueOf("c-3-2"), Double.box(32.0)),
        changelogRow("+I", Int.box(3), String.valueOf("c-3-3"), Double.box(33.0)),
        changelogRow("+I", Int.box(99), String.valueOf("c-99"), Double.box(99.0))
      ),
      List(List("c1")),
      List("c1")
    )

    prepareSrcTableWithData(
      "D",
      List("d2 string", "d1 int primary key not enforced", "d0 double"),
      List(
        changelogRow("+I", String.valueOf("d-1"), Int.box(1), Double.box(1.0)),
        changelogRow("-U", String.valueOf("d-1"), Int.box(1), Double.box(1.0)),
        changelogRow("+U", String.valueOf("d-2"), Int.box(1), Double.box(1.0)),
        changelogRow("+I", String.valueOf("d-3"), Int.box(3), Double.box(3.0)),
        changelogRow("+I", String.valueOf("d-100"), Int.box(100), Double.box(100.0))
      ),
      List(List("d1")),
      List()
    )
  }

  private def prepareSrcTableWithData(
      tableName: String,
      fields: List[String],
      data: List[Row],
      indexes: List[List[String]],
      immutableCols: List[String]): Unit = {
    Preconditions.checkArgument(fields.nonEmpty)
    tEnv.executeSql(s"drop table if exists $tableName")
    tEnv.executeSql(s"""
                       |create table $tableName(
                       |  ${fields.mkString(",")}
                       |) with (
                       |  'connector' = 'values',
                       |  'bounded' = 'false',
                       |  'changelog-mode' = 'I,UA,UB',
                       |  'data-id' = '${TestValuesTableFactory.registerData(data)}',
                       |  'async' = 'true'
                       |)
                       |""".stripMargin)

    addIndexesAndImmutableCols(tableName, indexes, immutableCols)
  }

}
