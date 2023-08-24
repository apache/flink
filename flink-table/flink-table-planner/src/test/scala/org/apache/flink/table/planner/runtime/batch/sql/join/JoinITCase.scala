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
package org.apache.flink.table.planner.runtime.batch.sql.join

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.common.typeutils.TypeComparator
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, RowTypeInfo}
import org.apache.flink.streaming.api.transformations.{LegacySinkTransformation, OneInputTransformation, TwoInputTransformation}
import org.apache.flink.table.api.internal.{StatementSetImpl, TableEnvironmentInternal}
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.expressions.utils.FuncWithOpen
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.batch.sql.join.JoinType.{BroadcastHashJoin, HashJoin, JoinType, NestedLoopJoin, SortMergeJoin}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.planner.sinks.CollectRowTableSink
import org.apache.flink.table.planner.utils.TestingTableEnvironment
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.testutils.junit.extensions.parameterized.{Parameter, ParameterizedTestExtension, Parameters}
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{BeforeEach, TestTemplate}
import org.junit.jupiter.api.extension.ExtendWith

import java.util

import scala.collection.JavaConversions._

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class JoinITCase extends BatchTestBase {

  @Parameter var expectedJoinType: JoinType = _
  @BeforeEach
  override def before(): Unit = {
    super.before()
    val smallData3Id = TestValuesTableFactory.registerData(smallData3)
    val data3Id = TestValuesTableFactory.registerData(data3)
    val nullData3Id = TestValuesTableFactory.registerData(nullData3)
    val data5Id = TestValuesTableFactory.registerData(data5)
    val nullData5Id = TestValuesTableFactory.registerData(nullData5)
    val data21Id = TestValuesTableFactory.registerData(data2_1)
    val data22Id = TestValuesTableFactory.registerData(data2_2)
    val data23Id = TestValuesTableFactory.registerData(data2_3)

    tEnv.executeSql(s"""CREATE TEMPORARY TABLE SmallTable3 (
                       | a int,
                       | b bigint,
                       | c string
                       |)WITH(
                       |  'connector' = 'values',
                       |  'data-id' = '$smallData3Id',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)

    tEnv.executeSql(s"""CREATE TEMPORARY TABLE Table3 (
                       | a int,
                       | b bigint,
                       | c string
                       |)WITH(
                       |  'connector' = 'values',
                       |  'data-id' = '$data3Id',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)

    tEnv.executeSql(s"""CREATE TEMPORARY TABLE NullTable3 (
                       | a int,
                       | b bigint,
                       | c string
                       |)WITH(
                       |  'connector' = 'values',
                       |  'data-id' = '$nullData3Id',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)

    tEnv.executeSql(s"""CREATE TEMPORARY TABLE Table5(
                       | d int,
                       | e bigint,
                       | f int,
                       | g string,
                       | h bigint
                       |)WITH(
                       |  'connector' = 'values',
                       |  'data-id' = '$data5Id',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)
    tEnv.executeSql(s"""CREATE TEMPORARY TABLE NullTable5(
                       | d int,
                       | e bigint,
                       | f int,
                       | g string,
                       | h bigint
                       |)WITH(
                       |  'connector' = 'values',
                       |  'data-id' = '$nullData5Id',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)

    tEnv.executeSql(s"""CREATE TEMPORARY TABLE l(
                       | a int,
                       | b double
                       |)WITH(
                       |  'connector' = 'values',
                       |  'data-id' = '$data21Id',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)

    tEnv.executeSql(s"""CREATE TEMPORARY TABLE r(
                       | c int,
                       | d double
                       |)WITH(
                       |  'connector' = 'values',
                       |  'data-id' = '$data22Id',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)

    tEnv.executeSql(s"""CREATE TEMPORARY TABLE t(
                       | c int,
                       | d double
                       |)WITH(
                       |  'connector' = 'values',
                       |  'data-id' = '$data23Id',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)
    JoinITCaseHelper.disableOtherJoinOpForJoin(tEnv, expectedJoinType)
  }

  @TestTemplate
  def testJoin(): Unit = {
    checkResult(
      "SELECT c, g FROM SmallTable3, Table5 WHERE b = e",
      Seq(
        row("Hi", "Hallo"),
        row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt")
      ))
  }

  @TestTemplate
  def testLongJoinWithBigRange(): Unit = {
    val data1 =
      Seq(row(Long.box(Long.MaxValue), Double.box(1)), row(Long.box(Long.MinValue), Double.box(1)))
    val data2 =
      Seq(row(Long.box(Long.MaxValue), Double.box(1)), row(Long.box(Long.MinValue), Double.box(1)))
    val inputDataId1 = TestValuesTableFactory.registerData(data1)
    val inputDataId2 = TestValuesTableFactory.registerData(data2)
    tEnv.executeSql(s"""CREATE TEMPORARY TABLE inputT1(
                       | a bigint,
                       | b double
                       |)WITH(
                       |  'connector' = 'values',
                       |  'data-id' = '$inputDataId1',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)

    tEnv.executeSql(s"""CREATE TEMPORARY TABLE inputT2(
                       | c bigint,
                       | d double
                       |)WITH(
                       |  'connector' = 'values',
                       |  'data-id' = '$inputDataId2',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)

    checkResult(
      "SELECT a, b, c, d FROM inputT1, inputT2 WHERE a = c",
      Seq(
        row(Long.box(Long.MaxValue), Double.box(1), Long.box(Long.MaxValue), Double.box(1)),
        row(Long.box(Long.MinValue), Double.box(1), Long.box(Long.MinValue), Double.box(1))
      )
    )
  }

  @TestTemplate
  def testLongHashJoinGenerator(): Unit = {
    if (expectedJoinType == HashJoin) {
      val sink = (new CollectRowTableSink).configure(Array("c"), Array(Types.STRING))
      tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("outputTable", sink)
      val stmtSet = tEnv.createStatementSet()
      val table = tEnv.sqlQuery("SELECT c FROM SmallTable3, Table5 WHERE b = e")
      stmtSet.addInsert("outputTable", table)
      val testingTEnv = tEnv.asInstanceOf[TestingTableEnvironment]
      val testingStmtSet = stmtSet.asInstanceOf[StatementSetImpl[_]]
      val transforms = testingTEnv.getPlanner
        .asInstanceOf[PlannerBase]
        .translate(testingStmtSet.getOperations)
      var haveTwoOp = false

      @scala.annotation.tailrec
      def findTwoInputTransform(t: Transformation[_]): TwoInputTransformation[_, _, _] = {
        t match {
          case sink: LegacySinkTransformation[_] => findTwoInputTransform(sink.getInputs.get(0))
          case one: OneInputTransformation[_, _] => findTwoInputTransform(one.getInputs.get(0))
          case two: TwoInputTransformation[_, _, _] => two
        }
      }

      transforms.map(findTwoInputTransform).foreach {
        transform =>
          transform.getOperatorFactory match {
            case factory: CodeGenOperatorFactory[_] =>
              if (factory.getGeneratedClass.getCode.contains("LongHashJoinOperator")) {
                haveTwoOp = true
              }
            case _ =>
          }
      }
      assertThat(haveTwoOp).isTrue
    }
  }

  @TestTemplate
  def testOneSideSmjFieldError(): Unit = {
    if (expectedJoinType == SortMergeJoin) {
      val smallData3Id = TestValuesTableFactory.registerData(smallData3)
      tEnv.executeSql(s"""CREATE TEMPORARY TABLE PojoSmallTable3(
                         | a int,
                         | b bigint,
                         | c string
                         |)WITH(
                         |  'connector' = 'values',
                         |  'data-id' = '$smallData3Id',
                         |  'bounded' = 'true'
                         |)
                         |""".stripMargin)

      val data5Id = TestValuesTableFactory.registerData(data5)
      tEnv.executeSql(s"""CREATE TEMPORARY TABLE PojoTable5(
                         | d int,
                         | e bigint,
                         | f int,
                         | g string,
                         | h bigint
                         |)WITH(
                         |  'connector' = 'values',
                         |  'data-id' = '$data5Id',
                         |  'bounded' = 'true'
                         |)
                         |""".stripMargin)

      checkResult(
        "SELECT c, g FROM (SELECT h, g, f, e, d FROM PojoSmallTable3, PojoTable5 WHERE b = e)," +
          " PojoSmallTable3 WHERE b = e",
        Seq(
          row("Hi", "Hallo"),
          row("Hello", "Hallo Welt"),
          row("Hello", "Hallo Welt"),
          row("Hello world", "Hallo Welt"),
          row("Hello world", "Hallo Welt")
        )
      )
    }
  }

  @TestTemplate
  def testJoinSameFieldEqual(): Unit = {
    checkResult(
      "SELECT c, g FROM SmallTable3, Table5 WHERE b = e and b = h",
      Seq(
        row("Hi", "Hallo"),
        row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt")
      ))
  }

  @TestTemplate
  def testJoinOn(): Unit = {
    checkResult(
      "SELECT c, g FROM SmallTable3 JOIN Table5 ON b = e",
      Seq(
        row("Hi", "Hallo"),
        row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt")
      ))
  }

  @TestTemplate
  def testJoinNoMatches(): Unit = {
    checkResult("SELECT c, g FROM SmallTable3, Table5 where c = g", Seq())
  }

  @TestTemplate
  def testJoinNoMatchesWithSubquery(): Unit = {
    checkResult(
      "SELECT c, g FROM " +
        "(SELECT * FROM SmallTable3 WHERE b>2), (SELECT * FROM Table5 WHERE e>2) WHERE b = e",
      Seq())
  }

  @TestTemplate
  def testJoinWithFilter(): Unit = {
    checkResult(
      "SELECT c, g FROM SmallTable3, Table5 WHERE b = e AND b < 2",
      Seq(
        row("Hi", "Hallo")
      ))
  }

  @TestTemplate
  def testJoinWithJoinFilter(): Unit = {
    checkResult(
      "SELECT c, g FROM Table3, Table5 WHERE b = e AND a < 6",
      Seq(
        row("Hi", "Hallo"),
        row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt"),
        row("Hello world, how are you?", "Hallo Welt wie"),
        row("I am fine.", "Hallo Welt wie")
      )
    )
  }

  @TestTemplate
  def testInnerJoinWithBooleanFilterCondition(): Unit = {
    val data1: Seq[Row] =
      Seq(row(1, 1L, "Hi", true), row(2, 2L, "Hello", false), row(3, 2L, "Hello world", true))

    val data1Id = TestValuesTableFactory.registerData(data1)
    tEnv.executeSql(s"""CREATE TEMPORARY TABLE table5(
                       | a1 int,
                       | b1 bigint,
                       | c1 string,
                       | d1 boolean
                       |)WITH(
                       |  'connector' = 'values',
                       |  'data-id' = '$data1Id',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)

    tEnv.executeSql(s"""CREATE TEMPORARY TABLE table6(
                       | a2 int,
                       | b2 bigint,
                       | c2 string,
                       | d2 boolean
                       |)WITH(
                       |  'connector' = 'values',
                       |  'data-id' = '$data1Id',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)

    checkResult(
      "SELECT a1, a1, c2 FROM table5 INNER JOIN table6 ON d1 = d2 where d1 is true",
      Seq(
        row("1, 1, Hello world"),
        row("1, 1, Hi"),
        row("3, 3, Hello world"),
        row("3, 3, Hi")
      )
    )
  }

  @TestTemplate
  def testInnerJoinWithNonEquiJoinPredicate(): Unit = {
    checkResult(
      "SELECT c, g FROM Table3, Table5 WHERE b = e AND a < 6 AND h < b",
      Seq(
        row("Hello world, how are you?", "Hallo Welt wie"),
        row("I am fine.", "Hallo Welt wie")
      )
    )
  }

  @TestTemplate
  def testJoinWithMultipleKeys(): Unit = {
    checkResult(
      "SELECT c, g FROM NullTable3, NullTable5 WHERE a = d AND b = h",
      Seq(
        row("Hi", "Hallo"),
        row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt wie gehts?"),
        row("Hello world", "ABC"),
        row("I am fine.", "HIJ"),
        row("I am fine.", "IJK")
      )
    )
  }

  @TestTemplate
  def testJoinWithAlias(): Unit = {
    val data1Id = TestValuesTableFactory.registerData(data5)
    tEnv.executeSql(s"""CREATE TEMPORARY TABLE AliasTable5(
                       | d int,
                       | e bigint,
                       | f int,
                       | g string,
                       | c bigint
                       |)WITH(
                       |  'connector' = 'values',
                       |  'data-id' = '$data1Id',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)

    checkResult(
      "SELECT AliasTable5.c, T.`1-_./Ü` FROM " +
        "(SELECT a, b, c AS `1-_./Ü` FROM Table3) AS T, AliasTable5 WHERE a = d AND a < 4",
      Seq(
        row("1", "Hi"),
        row("2", "Hello"),
        row("1", "Hello"),
        row("2", "Hello world"),
        row("2", "Hello world"),
        row("3", "Hello world")
      )
    )
  }

  @TestTemplate
  def testLeftJoinWithMultipleKeys(): Unit = {
    checkResult(
      "SELECT c, g FROM NullTable3 LEFT JOIN NullTable5 ON a = d and b = h",
      Seq(
        row("Hi", "Hallo"),
        row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt wie gehts?"),
        row("Hello world", "ABC"),
        row("I am fine.", "HIJ"),
        row("I am fine.", "IJK"),
        row("Hello world, how are you?", null),
        row("Luke Skywalker", null),
        row("Comment#1", null),
        row("Comment#2", null),
        row("Comment#3", null),
        row("Comment#4", null),
        row("Comment#5", null),
        row("Comment#6", null),
        row("Comment#7", null),
        row("Comment#8", null),
        row("Comment#9", null),
        row("Comment#10", null),
        row("Comment#11", null),
        row("Comment#12", null),
        row("Comment#13", null),
        row("Comment#14", null),
        row("Comment#15", null),
        row("NullTuple", null),
        row("NullTuple", null)
      )
    )
  }

  @TestTemplate
  def testLeftJoinWithNonEquiJoinPred(): Unit = {
    checkResult(
      "SELECT c, g FROM NullTable3 LEFT JOIN NullTable5 ON a = d and b <= h",
      Seq(
        row("Hi", "Hallo"),
        row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt wie gehts?"),
        row("Hello world", "ABC"),
        row("Hello world", "BCD"),
        row("I am fine.", "HIJ"),
        row("I am fine.", "IJK"),
        row("Hello world, how are you?", null),
        row("Luke Skywalker", null),
        row("Comment#1", null),
        row("Comment#2", null),
        row("Comment#3", null),
        row("Comment#4", null),
        row("Comment#5", null),
        row("Comment#6", null),
        row("Comment#7", null),
        row("Comment#8", null),
        row("Comment#9", null),
        row("Comment#10", null),
        row("Comment#11", null),
        row("Comment#12", null),
        row("Comment#13", null),
        row("Comment#14", null),
        row("Comment#15", null),
        row("NullTuple", null),
        row("NullTuple", null)
      )
    )
  }

  @TestTemplate
  def testLeftJoinWithLeftLocalPred(): Unit = {
    checkResult(
      "SELECT c, g FROM NullTable3 LEFT JOIN NullTable5 ON a = d and b = 2",
      Seq(
        row("Hi", null),
        row("Hello", "Hallo Welt"),
        row("Hello", "Hallo Welt wie"),
        row("Hello world", "Hallo Welt wie gehts?"),
        row("Hello world", "ABC"),
        row("Hello world", "BCD"),
        row("I am fine.", null),
        row("Hello world, how are you?", null),
        row("Luke Skywalker", null),
        row("Comment#1", null),
        row("Comment#2", null),
        row("Comment#3", null),
        row("Comment#4", null),
        row("Comment#5", null),
        row("Comment#6", null),
        row("Comment#7", null),
        row("Comment#8", null),
        row("Comment#9", null),
        row("Comment#10", null),
        row("Comment#11", null),
        row("Comment#12", null),
        row("Comment#13", null),
        row("Comment#14", null),
        row("Comment#15", null),
        row("NullTuple", null),
        row("NullTuple", null)
      )
    )
  }

  @TestTemplate
  def testRightJoinWithMultipleKeys(): Unit = {
    checkResult(
      "SELECT c, g FROM NullTable3 RIGHT JOIN NullTable5 ON a = d and b = h",
      Seq(
        row("Hi", "Hallo"),
        row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt wie gehts?"),
        row("Hello world", "ABC"),
        row("I am fine.", "HIJ"),
        row("I am fine.", "IJK"),
        row(null, "Hallo Welt wie"),
        row(null, "BCD"),
        row(null, "CDE"),
        row(null, "DEF"),
        row(null, "EFG"),
        row(null, "FGH"),
        row(null, "GHI"),
        row(null, "JKL"),
        row(null, "KLM"),
        row(null, "NullTuple"),
        row(null, "NullTuple")
      )
    )
  }

  @TestTemplate
  def testRightJoinWithNonEquiJoinPred(): Unit = {
    checkResult(
      "SELECT c, g FROM NullTable5 RIGHT JOIN NullTable3 ON a = d and b <= h",
      Seq(
        row("Hi", "Hallo"),
        row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt wie gehts?"),
        row("Hello world", "ABC"),
        row("Hello world", "BCD"),
        row("I am fine.", "HIJ"),
        row("I am fine.", "IJK"),
        row("Hello world, how are you?", null),
        row("Luke Skywalker", null),
        row("Comment#1", null),
        row("Comment#2", null),
        row("Comment#3", null),
        row("Comment#4", null),
        row("Comment#5", null),
        row("Comment#6", null),
        row("Comment#7", null),
        row("Comment#8", null),
        row("Comment#9", null),
        row("Comment#10", null),
        row("Comment#11", null),
        row("Comment#12", null),
        row("Comment#13", null),
        row("Comment#14", null),
        row("Comment#15", null),
        row("NullTuple", null),
        row("NullTuple", null)
      )
    )
  }

  @TestTemplate
  def testRightJoinWithLeftLocalPred(): Unit = {
    checkResult(
      "SELECT c, g FROM NullTable5 RIGHT JOIN NullTable3 ON a = d and b = 2",
      Seq(
        row("Hi", null),
        row("Hello", "Hallo Welt"),
        row("Hello", "Hallo Welt wie"),
        row("Hello world", "Hallo Welt wie gehts?"),
        row("Hello world", "ABC"),
        row("Hello world", "BCD"),
        row("I am fine.", null),
        row("Hello world, how are you?", null),
        row("Luke Skywalker", null),
        row("Comment#1", null),
        row("Comment#2", null),
        row("Comment#3", null),
        row("Comment#4", null),
        row("Comment#5", null),
        row("Comment#6", null),
        row("Comment#7", null),
        row("Comment#8", null),
        row("Comment#9", null),
        row("Comment#10", null),
        row("Comment#11", null),
        row("Comment#12", null),
        row("Comment#13", null),
        row("Comment#14", null),
        row("Comment#15", null),
        row("NullTuple", null),
        row("NullTuple", null)
      )
    )
  }

  @TestTemplate
  def testFullOuterJoinWithMultipleKeys(): Unit = {
    if (expectedJoinType != BroadcastHashJoin && expectedJoinType != NestedLoopJoin) {
      checkResult(
        "SELECT c, g FROM NullTable3 FULL JOIN NullTable5 ON a = d and b = h",
        Seq(
          row("Hi", "Hallo"),
          row("Hello", "Hallo Welt"),
          row("Hello world", "Hallo Welt wie gehts?"),
          row("Hello world", "ABC"),
          row("I am fine.", "HIJ"),
          row("I am fine.", "IJK"),
          row(null, "Hallo Welt wie"),
          row(null, "BCD"),
          row(null, "CDE"),
          row(null, "DEF"),
          row(null, "EFG"),
          row(null, "FGH"),
          row(null, "GHI"),
          row(null, "JKL"),
          row(null, "KLM"),
          row("Hello world, how are you?", null),
          row("Luke Skywalker", null),
          row("Comment#1", null),
          row("Comment#2", null),
          row("Comment#3", null),
          row("Comment#4", null),
          row("Comment#5", null),
          row("Comment#6", null),
          row("Comment#7", null),
          row("Comment#8", null),
          row("Comment#9", null),
          row("Comment#10", null),
          row("Comment#11", null),
          row("Comment#12", null),
          row("Comment#13", null),
          row("Comment#14", null),
          row("Comment#15", null),
          row("NullTuple", null),
          row("NullTuple", null),
          row(null, "NullTuple"),
          row(null, "NullTuple")
        )
      )
    }
  }

  @TestTemplate
  def testFullJoinWithNonEquiJoinPred(): Unit = {
    if (expectedJoinType != BroadcastHashJoin && expectedJoinType != NestedLoopJoin) {
      checkResult(
        "SELECT c, g FROM NullTable3 FULL JOIN NullTable5 ON a = d and b <= h",
        Seq(
          // join matcher
          row("Hi", "Hallo"),
          row("Hello", "Hallo Welt"),
          row("Hello world", "Hallo Welt wie gehts?"),
          row("Hello world", "ABC"),
          row("Hello world", "BCD"),
          row("I am fine.", "HIJ"),
          row("I am fine.", "IJK"),

          // preserved left
          row("Hello world, how are you?", null),
          row("Luke Skywalker", null),
          row("Comment#1", null),
          row("Comment#2", null),
          row("Comment#3", null),
          row("Comment#4", null),
          row("Comment#5", null),
          row("Comment#6", null),
          row("Comment#7", null),
          row("Comment#8", null),
          row("Comment#9", null),
          row("Comment#10", null),
          row("Comment#11", null),
          row("Comment#12", null),
          row("Comment#13", null),
          row("Comment#14", null),
          row("Comment#15", null),
          row("NullTuple", null),
          row("NullTuple", null),

          // preserved right
          row(null, "Hallo Welt wie"),
          row(null, "CDE"),
          row(null, "DEF"),
          row(null, "EFG"),
          row(null, "FGH"),
          row(null, "GHI"),
          row(null, "JKL"),
          row(null, "KLM"),
          row(null, "NullTuple"),
          row(null, "NullTuple")
        )
      )
    }
  }

  @TestTemplate
  def testFullJoinWithLeftLocalPred(): Unit = {
    if (expectedJoinType != BroadcastHashJoin && expectedJoinType != NestedLoopJoin) {
      checkResult(
        "SELECT c, g FROM NullTable3 FULL JOIN NullTable5 ON a = d and b >= 2 and h = 1",
        Seq(
          // join matcher
          row("Hello", "Hallo Welt wie"),
          row("Hello world, how are you?", "DEF"),
          row("Hello world, how are you?", "EFG"),
          row("I am fine.", "GHI"),

          // preserved left
          row("Hi", null),
          row("Hello world", null),
          row("Luke Skywalker", null),
          row("Comment#1", null),
          row("Comment#2", null),
          row("Comment#3", null),
          row("Comment#4", null),
          row("Comment#5", null),
          row("Comment#6", null),
          row("Comment#7", null),
          row("Comment#8", null),
          row("Comment#9", null),
          row("Comment#10", null),
          row("Comment#11", null),
          row("Comment#12", null),
          row("Comment#13", null),
          row("Comment#14", null),
          row("Comment#15", null),
          row("NullTuple", null),
          row("NullTuple", null),

          // preserved right
          row(null, "Hallo"),
          row(null, "Hallo Welt"),
          row(null, "Hallo Welt wie gehts?"),
          row(null, "ABC"),
          row(null, "BCD"),
          row(null, "CDE"),
          row(null, "FGH"),
          row(null, "HIJ"),
          row(null, "IJK"),
          row(null, "JKL"),
          row(null, "KLM"),
          row(null, "NullTuple"),
          row(null, "NullTuple")
        )
      )
    }
  }

  @TestTemplate
  def testFullOuterJoin(): Unit = {
    if (expectedJoinType != BroadcastHashJoin && expectedJoinType != NestedLoopJoin) {
      checkResult(
        "SELECT c, g FROM SmallTable3 FULL OUTER JOIN Table5 ON b = e",
        Seq(
          row("Hi", "Hallo"),
          row("Hello", "Hallo Welt"),
          row("Hello world", "Hallo Welt"),
          row(null, "Hallo Welt wie gehts?"),
          row(null, "Hallo Welt wie"),
          row(null, "ABC"),
          row(null, "BCD"),
          row(null, "CDE"),
          row(null, "DEF"),
          row(null, "EFG"),
          row(null, "FGH"),
          row(null, "GHI"),
          row(null, "HIJ"),
          row(null, "IJK"),
          row(null, "JKL"),
          row(null, "KLM")
        )
      )
    }
  }

  @TestTemplate
  def testFullOuterJoinWithoutEqualCond(): Unit = {
    if (expectedJoinType == NestedLoopJoin) {
      checkResult(
        "SELECT t1.c, t2.c FROM SmallTable3 t1 FULL OUTER JOIN SmallTable3 t2 ON t1.b > t2.b",
        Seq(
          row("Hello world", "Hi"),
          row("Hello", "Hi"),
          row("Hi", null),
          row(null, "Hello"),
          row(null, "Hello world")
        )
      )
    }
  }

  @TestTemplate
  def testSingleRowFullOuterJoinWithoutEqualCond(): Unit = {
    if (expectedJoinType == NestedLoopJoin) {
      checkResult(
        "SELECT c, mc FROM SmallTable3 t1 FULL OUTER JOIN " +
          "(SELECT min(b) AS mb, max(c) AS mc FROM SmallTable3) t2 ON b > mb",
        Seq(
          row("Hello world", "Hi"),
          row("Hello", "Hi"),
          row("Hi", null)
        )
      )
    }
  }

  @TestTemplate
  def testSingleRowFullOuterJoinWithoutEqualCondNoMatch(): Unit = {
    if (expectedJoinType == NestedLoopJoin) {
      checkResult(
        "SELECT c, mc FROM SmallTable3 t1 FULL OUTER JOIN " +
          "(SELECT max(b) AS mb, max(c) AS mc FROM SmallTable3) t2 ON b > mb",
        Seq(
          row("Hello world", null),
          row("Hello", null),
          row("Hi", null),
          row(null, "Hi")
        )
      )
    }
  }

  @TestTemplate
  def testLeftOuterJoin(): Unit = {
    checkResult(
      "SELECT c, g FROM Table5 LEFT OUTER JOIN SmallTable3 ON b = e",
      Seq(
        row("Hi", "Hallo"),
        row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt"),
        row(null, "Hallo Welt wie gehts?"),
        row(null, "Hallo Welt wie"),
        row(null, "ABC"),
        row(null, "BCD"),
        row(null, "CDE"),
        row(null, "DEF"),
        row(null, "EFG"),
        row(null, "FGH"),
        row(null, "GHI"),
        row(null, "HIJ"),
        row(null, "IJK"),
        row(null, "JKL"),
        row(null, "KLM")
      )
    )
  }

  @TestTemplate
  def testRightOuterJoin(): Unit = {
    checkResult(
      "SELECT c, g FROM SmallTable3 RIGHT OUTER JOIN Table5 ON b = e",
      Seq(
        row("Hi", "Hallo"),
        row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt"),
        row(null, "Hallo Welt wie gehts?"),
        row(null, "Hallo Welt wie"),
        row(null, "ABC"),
        row(null, "BCD"),
        row(null, "CDE"),
        row(null, "DEF"),
        row(null, "EFG"),
        row(null, "FGH"),
        row(null, "GHI"),
        row(null, "HIJ"),
        row(null, "IJK"),
        row(null, "JKL"),
        row(null, "KLM")
      )
    )
  }

  @TestTemplate
  def testJoinWithAggregation(): Unit = {
    checkResult("SELECT COUNT(g), COUNT(b) FROM SmallTable3, Table5 WHERE a = d", Seq(row(6L, 6L)))
  }

  @TestTemplate
  def testJoinConditionNeedSimplify(): Unit = {
    checkResult(
      "SELECT A.d FROM Table5 A JOIN SmallTable3 B ON (A.d=B.a and B.a>2) or (A.d=B.a and B.b=1)",
      Seq(row(1), row(3), row(3), row(3)))
  }

  @TestTemplate
  def testJoinConditionDerivedFromCorrelatedSubQueryNeedSimplify(): Unit = {
    checkResult(
      "SELECT B.a FROM SmallTable3 B WHERE b = (" +
        "select count(*) from Table5 A where (A.d=B.a and A.d<3) or (A.d=B.a and B.b=5))",
      Seq(row(1), row(2)))
  }

  @TestTemplate
  def testSimple(): Unit = {
    checkResult(
      "select a, b from l where a in (select c from r where c > 2)",
      Seq(row(3, 3.0), row(6, null)))
  }

  @TestTemplate
  def testSelect(): Unit = {
    checkResult("select t.a from (select 1 as a)t", Seq(row(1)))
  }

  @TestTemplate
  def testCorrelated(): Unit = {
    expectedJoinType match {
      case NestedLoopJoin =>
        checkResult(
          "select t.a from (select l.a from l, r where l.a = r.c and l.a = 6)t",
          Seq(row(6)))
      case _ =>
      // l.a=r.c and l.a = 6 => l.a=6 and r.c=6, so after ftd and join condition simplified, join
      // condition is TRUE. Only NestedLoopJoin can handle join without any equi-condition.
    }
  }

  @TestTemplate
  def testCorrelatedExist(): Unit = {
    checkResult(
      "select * from l where exists (select * from r where l.a = r.c)",
      Seq(row(2, 1.0), row(2, 1.0), row(3, 3.0), row(6, null)))

    checkResult(
      "select * from l where exists (select * from r where l.a = r.c) and l.a <= 2",
      Seq(row(2, 1.0), row(2, 1.0)))
  }

  @TestTemplate
  def testCorrelatedExist2(): Unit = {
    val data: Seq[Row] =
      Seq(row(0L), row(123456L), row(-123456L), row(2147483647L), row(-2147483647L))
    val dataId = TestValuesTableFactory.registerData(data)
    tEnv.executeSql(s"""CREATE TEMPORARY TABLE t1(
                       | f1 bigint
                       |)WITH(
                       |  'connector' = 'values',
                       |  'data-id' = '$dataId',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)

    checkResult(
      "select * from t1 o where exists (select 1 from t1 i where i.f1=o.f1 limit 0)",
      Seq())
  }

  @TestTemplate
  def testCorrelatedNotExist(): Unit = {
    checkResult(
      "select * from l where not exists (select * from r where l.a = r.c and l.b <> r.d)",
      Seq(row(1, 2.0), row(1, 2.0), row(6, null), row(null, 5.0), row(null, null))
    )
  }

  @TestTemplate
  def testUncorrelatedScalar(): Unit = {
    checkResult("select (select 1) as b", Seq(row(1)))

    checkResult("select (select 1 as b)", Seq(row(1)))

    checkResult("select (select 1 as a) as b", Seq(row(1)))
  }

  @TestTemplate
  def testEqualWithAggScalar(): Unit = {
    checkResult(
      "select a, b from l where a = (select distinct (c) from r where c = 2)",
      Seq(row(2, 1.0), row(2, 1.0)))
  }

  @TestTemplate
  def testComparisonsScalar(): Unit = {
    if (expectedJoinType == NestedLoopJoin) {
      checkEmptyResult("select a, b from l where a = (select c from r where 1 = 2)")

      checkResult(
        "select a, b from l where a >= 1.0 * (select avg(d) from r where c > 2)",
        row(2, 1.0) :: row(2, 1.0) :: row(3, 3.0) :: row(6, null) :: Nil)
    }

    checkResult(
      "select a, b from l where a * b < 2.0 * (select avg(d) from r where l.a = r.c and c < 6 )",
      row(2, 1.0) :: row(2, 1.0) :: Nil)
  }

  @TestTemplate
  def testJoinWithNull(): Unit = {
    // TODO enable all
    // TODO not support same source until set lazy_from_source
    if (expectedJoinType == SortMergeJoin) {
      checkResult(
        "SELECT c, g FROM NullTable3, NullTable5 " +
          "WHERE (a = d OR (a IS NULL AND d IS NULL)) AND b = h",
        Seq(
          row("Hi", "Hallo"),
          row("Hello", "Hallo Welt"),
          row("Hello world", "Hallo Welt wie gehts?"),
          row("Hello world", "ABC"),
          row("I am fine.", "HIJ"),
          row("I am fine.", "IJK"),
          row("NullTuple", "NullTuple"),
          row("NullTuple", "NullTuple"),
          row("NullTuple", "NullTuple"),
          row("NullTuple", "NullTuple")
        )
      )

      checkResult(
        "SELECT c, g FROM NullTable3, NullTable5 " +
          "WHERE (a = d OR (a IS NULL AND d IS NULL)) and c = 'NullTuple'",
        Seq(
          row("NullTuple", "NullTuple"),
          row("NullTuple", "NullTuple"),
          row("NullTuple", "NullTuple"),
          row("NullTuple", "NullTuple")
        )
      )

      val dataId = TestValuesTableFactory.registerData(Seq(row(null, null, "c")))
      tEnv.executeSql(s"""CREATE TEMPORARY TABLE NullT(
                         | a int,
                         | b bigint,
                         | c string
                         |)WITH(
                         |  'connector' = 'values',
                         |  'data-id' = '$dataId',
                         |  'bounded' = 'true'
                         |)
                         |""".stripMargin)

      checkResult(
        "SELECT T1.a, T1.b, T1.c FROM NullT T1, NullT T2 WHERE " +
          "(T1.a = T2.a OR (T1.a IS NULL AND T2.a IS NULL)) " +
          "AND (T1.b = T2.b OR (T1.b IS NULL AND T2.b IS NULL)) AND T1.c = T2.c",
        Seq(row("null", "null", "c"))
      )
    }
  }

  @TestTemplate
  def testSingleRowJoin(): Unit = {
    if (expectedJoinType == NestedLoopJoin) {
      checkResult(
        "SELECT s, a, b, c FROM SmallTable3 JOIN (SELECT SUM(b) AS s FROM SmallTable3) ON true",
        Seq(
          row(5L, 1, 1L, "Hi"),
          row(5L, 2, 2L, "Hello"),
          row(5L, 3, 2L, "Hello world")
        )
      )

      checkResult(
        "SELECT s, a, b, c FROM (SELECT SUM(b) AS s FROM SmallTable3) JOIN SmallTable3 ON true",
        Seq(
          row(5L, 1, 1L, "Hi"),
          row(5L, 2, 2L, "Hello"),
          row(5L, 3, 2L, "Hello world")
        )
      )

      checkResult(
        "SELECT s, a, b, c FROM SmallTable3 JOIN (SELECT SUM(b) AS s FROM SmallTable3) ON s <> b",
        Seq(
          row(5L, 1, 1L, "Hi"),
          row(5L, 2, 2L, "Hello"),
          row(5L, 3, 2L, "Hello world")
        )
      )

      checkResult(
        "SELECT s, a, b, c FROM (SELECT SUM(b) AS s FROM SmallTable3) JOIN SmallTable3 ON s <> b",
        Seq(
          row(5L, 1, 1L, "Hi"),
          row(5L, 2, 2L, "Hello"),
          row(5L, 3, 2L, "Hello world")
        )
      )
    }
  }

  @TestTemplate
  def testNonEmptyTableJoinEmptyTable(): Unit = {
    if (expectedJoinType == NestedLoopJoin) {
      checkResult(
        "SELECT s, a, b, c FROM " +
          "SmallTable3 JOIN (SELECT SUM(b) AS s FROM SmallTable3 HAVING COUNT(*) < 0) ON true",
        Seq()
      )

      checkResult(
        "SELECT s, a, b, c FROM " +
          "(SELECT SUM(b) AS s FROM SmallTable3 HAVING COUNT(*) < 0) JOIN SmallTable3 ON true",
        Seq()
      )

      checkResult(
        "SELECT s, a, b, c FROM SmallTable3 FULL JOIN " +
          "(SELECT SUM(b) AS s FROM SmallTable3 HAVING COUNT(*) < 0) ON true",
        Seq(row(null, 1, 1, "Hi"), row(null, 2, 2, "Hello"), row(null, 3, 2, "Hello world"))
      )

      checkResult(
        "SELECT s, a, b, c FROM (SELECT SUM(b) AS s FROM SmallTable3 HAVING COUNT(*) < 0) " +
          "FULL JOIN SmallTable3 ON true",
        Seq(row(null, 1, 1, "Hi"), row(null, 2, 2, "Hello"), row(null, 3, 2, "Hello world"))
      )
    }
  }

  @TestTemplate
  def testEmptyTableJoinEmptyTable(): Unit = {
    if (expectedJoinType == NestedLoopJoin) {
      checkResult(
        "SELECT sa, sb FROM " +
          "(SELECT SUM(a) AS sa FROM SmallTable3 HAVING COUNT(*) < 0) JOIN " +
          "(SELECT SUM(b) AS sb FROM SmallTable3 HAVING COUNT(*) < 0) ON true",
        Seq()
      )

      checkResult(
        "SELECT sa, sb FROM " +
          "(SELECT SUM(b) AS sb FROM SmallTable3 HAVING COUNT(*) < 0) JOIN " +
          "(SELECT SUM(a) AS sa FROM SmallTable3 HAVING COUNT(*) < 0) ON true",
        Seq()
      )

      checkResult(
        "SELECT sa, sb FROM " +
          "(SELECT SUM(a) AS sa FROM SmallTable3 HAVING COUNT(*) < 0) FULL JOIN " +
          "(SELECT SUM(b) AS sb FROM SmallTable3 HAVING COUNT(*) < 0) ON true",
        Seq()
      )

      checkResult(
        "SELECT sa, sb FROM " +
          "(SELECT SUM(b) AS sb FROM SmallTable3 HAVING COUNT(*) < 0) FULL JOIN " +
          "(SELECT SUM(a) AS sa FROM SmallTable3 HAVING COUNT(*) < 0) ON true",
        Seq()
      )
    }
  }

  @TestTemplate
  def testJoinCollation(): Unit = {
    checkResult(
      """
        |WITH v1 AS (
        |  SELECT t1.a AS a, (t1.b + t2.b) AS b
        |    FROM SmallTable3 AS t1, SmallTable3 AS t2 WHERE t1.a = t2.a
        |),
        |
        |v2 AS (
        |  SELECT t1.a AS a, (t1.b * t2.b) AS b
        |    FROM SmallTable3 AS t1, SmallTable3 AS t2 WHERE t1.a = t2.a
        |)
        |
        |SELECT v1.a, v2.a, v1.b, v2.b FROM v1, v2 WHERE v1.a = v2.a
      """.stripMargin,
      Seq(
        row(1, 1, 2L, 1L),
        row(2, 2, 4L, 4L),
        row(3, 3, 4L, 4L)
      )
    )

    checkResult(
      """
        |WITH v1 AS (
        |  SELECT t1.a AS a, (t1.b + t2.b) AS b
        |    FROM SmallTable3 AS t1, SmallTable3 AS t2 WHERE t1.a = t2.a
        |),
        |
        |v2 AS (
        |  SELECT t1.b AS a, (t1.b * t2.b) AS b
        |    FROM SmallTable3 AS t1, SmallTable3 AS t2 WHERE t1.b = t2.b
        |)
        |
        |SELECT v1.a, v2.a, v1.b, v2.b FROM v1, v2 WHERE v1.a = v2.a
      """.stripMargin,
      Seq(
        row(1, 1L, 2L, 1L),
        row(2, 2L, 4L, 4L),
        row(2, 2L, 4L, 4L),
        row(2, 2L, 4L, 4L),
        row(2, 2L, 4L, 4L)
      )
    )
  }

  @TestTemplate
  def testJoinWithUDFFilter(): Unit = {
    registerFunction("funcWithOpen", new FuncWithOpen)
    checkResult(
      "SELECT c, g FROM SmallTable3 join Table5 on funcWithOpen(a + d) where b = e",
      Seq(row("Hi", "Hallo"), row("Hello", "Hallo Welt"), row("Hello world", "Hallo Welt"))
    )
  }

  @TestTemplate
  def testJoinWithFilterPushDown(): Unit = {
    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 where a >= 2
        |""".stripMargin,
      Seq(row(2, 1.0, 2, 2, 3.0, 2), row(3, 3.0, 1, 3, 2.0, 1), row(6, null, 1, 6, null, 1))
    )

    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  left join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 where a >= 2
        |""".stripMargin,
      Seq(row(2, 1.0, 2, 2, 3.0, 2), row(3, 3.0, 1, 3, 2.0, 1), row(6, null, 1, 6, null, 1))
    )

    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  left join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 where c >= 2
        |""".stripMargin,
      Seq(row(2, 1.0, 2, 2, 3.0, 2), row(3, 3.0, 1, 3, 2.0, 1), row(6, null, 1, 6, null, 1))
    )

    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  right join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 where a >= 2
        |""".stripMargin,
      Seq(row(2, 1.0, 2, 2, 3.0, 2), row(3, 3.0, 1, 3, 2.0, 1), row(6, null, 1, 6, null, 1))
    )

    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  right join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 where c >= 2
        |""".stripMargin,
      Seq(
        row(2, 1.0, 2, 2, 3.0, 2),
        row(3, 3.0, 1, 3, 2.0, 1),
        row(6, null, 1, 6, null, 1),
        row(null, null, null, 4, 1.0, 1))
    )

    checkResult(
      """
        |select * from
        | l inner join r on a = c where c IS NULL
        |""".stripMargin,
      Seq()
    )

    checkResult(
      """
        |select * from
        | l inner join r on a = c where c = NULL
        |""".stripMargin,
      Seq()
    )

    if (expectedJoinType == NestedLoopJoin) {
      // For inner join, we will push c = 3 into left side l by
      // derived from a = c and c = 3.
      checkResult(
        """
          |select * from
          | l inner join r on a = c where c = 3
          |""".stripMargin,
        Seq(
          row(3, 3.0, 3, 2.0)
        )
      )

      // For left join, we will push c = 3 into left side l by
      // derived from a = c and c = 3.
      checkResult(
        """
          |select * from
          | l left join r on a = c where c = 3
          |""".stripMargin,
        Seq(
          row(3, 3.0, 3, 2.0)
        )
      )
    }

    // For left/right join, we will only push equal filter condition into
    // other side by derived from join condition and filter condition. So,
    // c IS NULL cannot be push into left side.
    checkResult(
      """
        |select * from
        | l left join r on a = c where c IS NULL
        |""".stripMargin,
      Seq(
        row(1, 2.0, null, null),
        row(1, 2.0, null, null),
        row(null, 5.0, null, null),
        row(null, null, null, null)
      )
    )

    checkResult(
      """
        |select * from
        | l left join r on a = c where c IS NULL AND a <= 1
        |""".stripMargin,
      Seq(
        row(1, 2.0, null, null),
        row(1, 2.0, null, null)
      )
    )

    // For 'c = NULL', all data cannot match this condition.
    checkResult(
      """
        |select * from
        | l left join r on a = c where c = NULL
        |""".stripMargin,
      Seq()
    )

    // For left/right join, we will only push equal filter condition into
    // other side by derived from join condition and filter condition. So,
    // c < 3 cannot be push into left side.
    checkResult(
      """
        |select * from
        | l left join r on a = c where c < 3 AND a <= 3
        |""".stripMargin,
      Seq(
        row(2, 1.0, 2, 3.0),
        row(2, 1.0, 2, 3.0),
        row(2, 1.0, 2, 3.0),
        row(2, 1.0, 2, 3.0)
      )
    )

    // C <> 3 cannot be push into left side.
    checkResult(
      """
        |select * from
        | l left join r on a = c where c <> 3 AND a <= 3
        |""".stripMargin,
      Seq(
        row(2, 1.0, 2, 3.0),
        row(2, 1.0, 2, 3.0),
        row(2, 1.0, 2, 3.0),
        row(2, 1.0, 2, 3.0)
      )
    )
  }

  @TestTemplate
  def testJoinWithJoinConditionPushDown(): Unit = {
    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 and a >= 2
        |""".stripMargin,
      Seq(row(2, 1.0, 2, 2, 3.0, 2), row(3, 3.0, 1, 3, 2.0, 1), row(6, null, 1, 6, null, 1))
    )

    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  left join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 and a >= 2
        |""".stripMargin,
      Seq(
        row(1, 2.0, 2, null, null, null),
        row(2, 1.0, 2, 2, 3.0, 2),
        row(3, 3.0, 1, 3, 2.0, 1),
        row(6, null, 1, 6, null, 1),
        row(null, 5.0, 2, null, null, null))
    )

    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  left join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 and c >= 2
        |""".stripMargin,
      Seq(
        row(1, 2.0, 2, null, null, null),
        row(2, 1.0, 2, 2, 3.0, 2),
        row(3, 3.0, 1, 3, 2.0, 1),
        row(6, null, 1, 6, null, 1),
        row(null, 5.0, 2, null, null, null))
    )

    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  right join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 and a >= 2
        |""".stripMargin,
      Seq(
        row(2, 1.0, 2, 2, 3.0, 2),
        row(3, 3.0, 1, 3, 2.0, 1),
        row(6, null, 1, 6, null, 1),
        row(null, null, null, 4, 1.0, 1),
        row(null, null, null, null, 5.0, 2))
    )

    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  right join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 and c >= 2
        |""".stripMargin,
      Seq(
        row(2, 1.0, 2, 2, 3.0, 2),
        row(3, 3.0, 1, 3, 2.0, 1),
        row(6, null, 1, 6, null, 1),
        row(null, null, null, 4, 1.0, 1),
        row(null, null, null, null, 5.0, 2))
    )
  }
}

object JoinITCase {
  @Parameters(name = "expectedJoinType={0}")
  def parameters(): util.Collection[Any] = {
    util.Arrays.asList(
      Array(BroadcastHashJoin),
      Array(HashJoin),
      Array(SortMergeJoin),
      Array(NestedLoopJoin))
  }
}

class GenericTypeInfoWithoutComparator[T](clazz: Class[T]) extends GenericTypeInfo[T](clazz) {

  override def createComparator(
      sortOrderAscending: Boolean,
      executionConfig: ExecutionConfig): TypeComparator[T] = {
    throw new RuntimeException("Not expected!")
  }
}
