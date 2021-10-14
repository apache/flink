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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{DOUBLE_TYPE_INFO, INT_TYPE_INFO, LONG_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.common.typeutils.TypeComparator
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, RowTypeInfo}
import org.apache.flink.streaming.api.transformations.{LegacySinkTransformation, OneInputTransformation, TwoInputTransformation}
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.expressions.utils.FuncWithOpen
import org.apache.flink.table.planner.runtime.batch.sql.join.JoinType.{BroadcastHashJoin, HashJoin, JoinType, NestedLoopJoin, SortMergeJoin}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.planner.sinks.CollectRowTableSink
import org.apache.flink.table.planner.utils.{TestingStatementSet, TestingTableEnvironment}
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.types.Row

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Assert, Before, Test}

import java.util

import scala.collection.JavaConversions._
import scala.collection.Seq

@RunWith(classOf[Parameterized])
class JoinITCase(expectedJoinType: JoinType) extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    registerCollection("SmallTable3", smallData3, type3, "a, b, c", nullablesOfSmallData3)
    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)
    registerCollection("Table5", data5, type5, "d, e, f, g, h", nullablesOfData5)
    registerCollection("NullTable3", nullData3, type3, "a, b, c", nullablesOfNullData3)
    registerCollection("NullTable5", nullData5, type5, "d, e, f, g, h", nullablesOfNullData5)
    registerCollection("l", data2_1, INT_DOUBLE, "a, b")
    registerCollection("r", data2_2, INT_DOUBLE, "c, d")
    registerCollection("t", data2_3, INT_DOUBLE, "c, d", nullablesOfData2_3)
    JoinITCaseHelper.disableOtherJoinOpForJoin(tEnv, expectedJoinType)
  }

  @Test
  def testJoin(): Unit = {
    checkResult(
      "SELECT c, g FROM SmallTable3, Table5 WHERE b = e",
      Seq(
        row("Hi", "Hallo"),
        row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt")
      ))
  }

  @Test
  def testLongJoinWithBigRange(): Unit = {
    registerCollection(
      "inputT1",
      Seq(
        row(Long.box(Long.MaxValue), Double.box(1)),
        row(Long.box(Long.MinValue), Double.box(1))),
      new RowTypeInfo(LONG_TYPE_INFO, DOUBLE_TYPE_INFO),
      "a, b")
    registerCollection(
      "inputT2",
      Seq(
        row(Long.box(Long.MaxValue), Double.box(1)),
        row(Long.box(Long.MinValue), Double.box(1))),
      new RowTypeInfo(LONG_TYPE_INFO, DOUBLE_TYPE_INFO),
      "c, d")

    checkResult(
      "SELECT a, b, c, d FROM inputT1, inputT2 WHERE a = c",
      Seq(
        row(Long.box(Long.MaxValue), Double.box(1), Long.box(Long.MaxValue), Double.box(1)),
        row(Long.box(Long.MinValue), Double.box(1), Long.box(Long.MinValue), Double.box(1))
      ))
  }

  @Test
  def testLongHashJoinGenerator(): Unit = {
    if (expectedJoinType == HashJoin) {
      val sink = (new CollectRowTableSink).configure(Array("c"), Array(Types.STRING))
      tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("outputTable", sink)
      val stmtSet = tEnv.createStatementSet()
      val table = tEnv.sqlQuery("SELECT c FROM SmallTable3, Table5 WHERE b = e")
      stmtSet.addInsert("outputTable", table)
      val testingTEnv = tEnv.asInstanceOf[TestingTableEnvironment]
      val testingStmtSet = stmtSet.asInstanceOf[TestingStatementSet]
      val transforms = testingTEnv.getPlanner.asInstanceOf[PlannerBase]
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

      transforms.map(findTwoInputTransform).foreach { transform =>
        transform.getOperatorFactory match {
          case factory: CodeGenOperatorFactory[_] =>
            if (factory.getGeneratedClass.getCode.contains("LongHashJoinOperator")) {
              haveTwoOp = true
            }
          case _ =>
        }
      }
      Assert.assertTrue(haveTwoOp)
    }
  }

  @Test
  def testOneSideSmjFieldError(): Unit = {
    if (expectedJoinType == SortMergeJoin) {
      registerCollection("PojoSmallTable3", smallData3,
        new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO,
          new GenericTypeInfoWithoutComparator[String](classOf[String])),
        "a, b, c", nullablesOfSmallData3)
      registerCollection("PojoTable5", data5,
        new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, INT_TYPE_INFO,
          new GenericTypeInfoWithoutComparator[String](classOf[String]), LONG_TYPE_INFO),
        "d, e, f, g, h", nullablesOfData5)

      checkResult(
        "SELECT c, g FROM (SELECT h, g, f, e, d FROM PojoSmallTable3, PojoTable5 WHERE b = e)," +
          " PojoSmallTable3 WHERE b = e",
        Seq(
          row("Hi", "Hallo"),
          row("Hello", "Hallo Welt"),
          row("Hello", "Hallo Welt"),
          row("Hello world", "Hallo Welt"),
          row("Hello world", "Hallo Welt")
        ))
    }
  }

  @Test
  def testJoinSameFieldEqual(): Unit = {
    checkResult(
      "SELECT c, g FROM SmallTable3, Table5 WHERE b = e and b = h",
      Seq(
        row("Hi", "Hallo"),
        row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt")
      ))
  }

  @Test
  def testJoinOn(): Unit = {
    checkResult(
      "SELECT c, g FROM SmallTable3 JOIN Table5 ON b = e",
      Seq(
        row("Hi", "Hallo"),
        row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt")
      ))
  }

  @Test
  def testJoinNoMatches(): Unit = {
    checkResult(
      "SELECT c, g FROM SmallTable3, Table5 where c = g",
      Seq())
  }

  @Test
  def testJoinNoMatchesWithSubquery(): Unit = {
    checkResult(
      "SELECT c, g FROM " +
        "(SELECT * FROM SmallTable3 WHERE b>2), (SELECT * FROM Table5 WHERE e>2) WHERE b = e",
      Seq())
  }

  @Test
  def testJoinWithFilter(): Unit = {
    checkResult(
      "SELECT c, g FROM SmallTable3, Table5 WHERE b = e AND b < 2",
      Seq(
        row("Hi", "Hallo")
      ))
  }

  @Test
  def testJoinWithJoinFilter(): Unit = {
    checkResult(
      "SELECT c, g FROM Table3, Table5 WHERE b = e AND a < 6",
      Seq(
        row("Hi", "Hallo"),
        row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt"),
        row("Hello world, how are you?", "Hallo Welt wie"),
        row("I am fine.", "Hallo Welt wie")
      ))
  }

  @Test
  def testInnerJoinWithNonEquiJoinPredicate(): Unit = {
    checkResult(
      "SELECT c, g FROM Table3, Table5 WHERE b = e AND a < 6 AND h < b",
      Seq(
        row("Hello world, how are you?", "Hallo Welt wie"),
        row("I am fine.", "Hallo Welt wie")
      ))
  }

  @Test
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
      ))
  }

  @Test
  def testJoinWithAlias(): Unit = {
    registerCollection("AliasTable5", data5, type5, "d, e, f, g, c")
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
      ))
  }

  @Test
  def testLeftJoinWithMultipleKeys(): Unit = {
    checkResult(
      "SELECT c, g FROM NullTable3 LEFT JOIN NullTable5 ON a = d and b = h",
      Seq(
        row("Hi", "Hallo"), row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt wie gehts?"), row("Hello world", "ABC"),
        row("I am fine.", "HIJ"), row("I am fine.", "IJK"),
        row("Hello world, how are you?", null), row("Luke Skywalker", null),
        row("Comment#1", null), row("Comment#2", null), row("Comment#3", null),
        row("Comment#4", null), row("Comment#5", null), row("Comment#6", null),
        row("Comment#7", null), row("Comment#8", null), row("Comment#9", null),
        row("Comment#10", null), row("Comment#11", null), row("Comment#12", null),
        row("Comment#13", null), row("Comment#14", null), row("Comment#15", null),
        row("NullTuple", null), row("NullTuple", null)
      ))
  }

  @Test
  def testLeftJoinWithNonEquiJoinPred(): Unit = {
    checkResult(
      "SELECT c, g FROM NullTable3 LEFT JOIN NullTable5 ON a = d and b <= h",
      Seq(
        row("Hi", "Hallo"), row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt wie gehts?"), row("Hello world", "ABC"),
        row("Hello world", "BCD"), row("I am fine.", "HIJ"), row("I am fine.", "IJK"),
        row("Hello world, how are you?", null), row("Luke Skywalker", null),
        row("Comment#1", null), row("Comment#2", null), row("Comment#3", null),
        row("Comment#4", null), row("Comment#5", null), row("Comment#6", null),
        row("Comment#7", null), row("Comment#8", null), row("Comment#9", null),
        row("Comment#10", null), row("Comment#11", null), row("Comment#12", null),
        row("Comment#13", null), row("Comment#14", null), row("Comment#15", null),
        row("NullTuple", null), row("NullTuple", null)
      ))
  }

  @Test
  def testLeftJoinWithLeftLocalPred(): Unit = {
    checkResult(
      "SELECT c, g FROM NullTable3 LEFT JOIN NullTable5 ON a = d and b = 2",
      Seq(
        row("Hi", null), row("Hello", "Hallo Welt"), row("Hello", "Hallo Welt wie"),
        row("Hello world", "Hallo Welt wie gehts?"), row("Hello world", "ABC"),
        row("Hello world", "BCD"), row("I am fine.", null),
        row("Hello world, how are you?", null), row("Luke Skywalker", null),
        row("Comment#1", null), row("Comment#2", null), row("Comment#3", null),
        row("Comment#4", null), row("Comment#5", null), row("Comment#6", null),
        row("Comment#7", null), row("Comment#8", null), row("Comment#9", null),
        row("Comment#10", null), row("Comment#11", null), row("Comment#12", null),
        row("Comment#13", null), row("Comment#14", null), row("Comment#15", null),
        row("NullTuple", null), row("NullTuple", null)
      ))
  }

  @Test
  def testRightJoinWithMultipleKeys(): Unit = {
    checkResult(
      "SELECT c, g FROM NullTable3 RIGHT JOIN NullTable5 ON a = d and b = h",
      Seq(
        row("Hi", "Hallo"), row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt wie gehts?"), row("Hello world", "ABC"),
        row("I am fine.", "HIJ"), row("I am fine.", "IJK"),
        row(null, "Hallo Welt wie"), row(null, "BCD"), row(null, "CDE"),
        row(null, "DEF"), row(null, "EFG"), row(null, "FGH"),
        row(null, "GHI"), row(null, "JKL"), row(null, "KLM"),
        row(null, "NullTuple"), row(null, "NullTuple")
      ))
  }

  @Test
  def testRightJoinWithNonEquiJoinPred(): Unit = {
    checkResult(
      "SELECT c, g FROM NullTable5 RIGHT JOIN NullTable3 ON a = d and b <= h",
      Seq(
        row("Hi", "Hallo"), row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt wie gehts?"), row("Hello world", "ABC"),
        row("Hello world", "BCD"), row("I am fine.", "HIJ"), row("I am fine.", "IJK"),
        row("Hello world, how are you?", null), row("Luke Skywalker", null),
        row("Comment#1", null), row("Comment#2", null), row("Comment#3", null),
        row("Comment#4", null), row("Comment#5", null), row("Comment#6", null),
        row("Comment#7", null), row("Comment#8", null), row("Comment#9", null),
        row("Comment#10", null), row("Comment#11", null), row("Comment#12", null),
        row("Comment#13", null), row("Comment#14", null), row("Comment#15", null),
        row("NullTuple", null), row("NullTuple", null)
      ))
  }

  @Test
  def testRightJoinWithLeftLocalPred(): Unit = {
    checkResult(
      "SELECT c, g FROM NullTable5 RIGHT JOIN NullTable3 ON a = d and b = 2",
      Seq(
        row("Hi", null), row("Hello", "Hallo Welt"), row("Hello", "Hallo Welt wie"),
        row("Hello world", "Hallo Welt wie gehts?"), row("Hello world", "ABC"),
        row("Hello world", "BCD"), row("I am fine.", null),
        row("Hello world, how are you?", null), row("Luke Skywalker", null),
        row("Comment#1", null), row("Comment#2", null), row("Comment#3", null),
        row("Comment#4", null), row("Comment#5", null), row("Comment#6", null),
        row("Comment#7", null), row("Comment#8", null), row("Comment#9", null),
        row("Comment#10", null), row("Comment#11", null), row("Comment#12", null),
        row("Comment#13", null), row("Comment#14", null), row("Comment#15", null),
        row("NullTuple", null), row("NullTuple", null)
      ))
  }

  @Test
  def testFullOuterJoinWithMultipleKeys(): Unit = {
    if (expectedJoinType != BroadcastHashJoin && expectedJoinType != NestedLoopJoin) {
      checkResult(
        "SELECT c, g FROM NullTable3 FULL JOIN NullTable5 ON a = d and b = h",
        Seq(
          row("Hi", "Hallo"), row("Hello", "Hallo Welt"),
          row("Hello world", "Hallo Welt wie gehts?"), row("Hello world", "ABC"),
          row("I am fine.", "HIJ"), row("I am fine.", "IJK"),
          row(null, "Hallo Welt wie"), row(null, "BCD"), row(null, "CDE"),
          row(null, "DEF"), row(null, "EFG"), row(null, "FGH"),
          row(null, "GHI"), row(null, "JKL"), row(null, "KLM"),
          row("Hello world, how are you?", null), row("Luke Skywalker", null),
          row("Comment#1", null), row("Comment#2", null), row("Comment#3", null),
          row("Comment#4", null), row("Comment#5", null), row("Comment#6", null),
          row("Comment#7", null), row("Comment#8", null), row("Comment#9", null),
          row("Comment#10", null), row("Comment#11", null), row("Comment#12", null),
          row("Comment#13", null), row("Comment#14", null), row("Comment#15", null),
          row("NullTuple", null), row("NullTuple", null),
          row(null, "NullTuple"), row(null, "NullTuple")
        ))
    }
  }

  @Test
  def testFullJoinWithNonEquiJoinPred(): Unit = {
    if (expectedJoinType != BroadcastHashJoin && expectedJoinType != NestedLoopJoin) {
      checkResult(
        "SELECT c, g FROM NullTable3 FULL JOIN NullTable5 ON a = d and b <= h",
        Seq(
          // join matcher
          row("Hi", "Hallo"), row("Hello", "Hallo Welt"),
          row("Hello world", "Hallo Welt wie gehts?"), row("Hello world", "ABC"),
          row("Hello world", "BCD"), row("I am fine.", "HIJ"), row("I am fine.", "IJK"),

          // preserved left
          row("Hello world, how are you?", null), row("Luke Skywalker", null),
          row("Comment#1", null), row("Comment#2", null), row("Comment#3", null),
          row("Comment#4", null), row("Comment#5", null), row("Comment#6", null),
          row("Comment#7", null), row("Comment#8", null), row("Comment#9", null),
          row("Comment#10", null), row("Comment#11", null), row("Comment#12", null),
          row("Comment#13", null), row("Comment#14", null), row("Comment#15", null),
          row("NullTuple", null), row("NullTuple", null),

          // preserved right
          row(null, "Hallo Welt wie"), row(null, "CDE"),
          row(null, "DEF"), row(null, "EFG"), row(null, "FGH"),
          row(null, "GHI"), row(null, "JKL"), row(null, "KLM"),
          row(null, "NullTuple"), row(null, "NullTuple")
        ))
    }
  }

  @Test
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
          row("Hi", null), row("Hello world", null), row("Luke Skywalker", null),
          row("Comment#1", null), row("Comment#2", null), row("Comment#3", null),
          row("Comment#4", null), row("Comment#5", null), row("Comment#6", null),
          row("Comment#7", null), row("Comment#8", null), row("Comment#9", null),
          row("Comment#10", null), row("Comment#11", null), row("Comment#12", null),
          row("Comment#13", null), row("Comment#14", null), row("Comment#15", null),
          row("NullTuple", null), row("NullTuple", null),

          // preserved right
          row(null, "Hallo"), row(null, "Hallo Welt"), row(null, "Hallo Welt wie gehts?"),
          row(null, "ABC"), row(null, "BCD"), row(null, "CDE"), row(null, "FGH"),
          row(null, "HIJ"), row(null, "IJK"), row(null, "JKL"), row(null, "KLM"),
          row(null, "NullTuple"), row(null, "NullTuple")
        ))
    }
  }

  @Test
  def testFullOuterJoin(): Unit = {
    if (expectedJoinType != BroadcastHashJoin && expectedJoinType != NestedLoopJoin) {
      checkResult(
        "SELECT c, g FROM SmallTable3 FULL OUTER JOIN Table5 ON b = e",
        Seq(
          row("Hi", "Hallo"), row("Hello", "Hallo Welt"),
          row("Hello world", "Hallo Welt"),
          row(null, "Hallo Welt wie gehts?"), row(null, "Hallo Welt wie"),
          row(null, "ABC"), row(null, "BCD"), row(null, "CDE"),
          row(null, "DEF"), row(null, "EFG"), row(null, "FGH"),
          row(null, "GHI"), row(null, "HIJ"), row(null, "IJK"),
          row(null, "JKL"), row(null, "KLM")
        ))
    }
  }

  @Test
  def testFullOuterJoinWithoutEqualCond(): Unit = {
    if (expectedJoinType == NestedLoopJoin) {
      checkResult(
        "SELECT t1.c, t2.c FROM SmallTable3 t1 FULL OUTER JOIN SmallTable3 t2 ON t1.b > t2.b",
        Seq(
          row("Hello world", "Hi"), row("Hello", "Hi"), row("Hi", null),
          row(null, "Hello"), row(null, "Hello world")
        ))
    }
  }

  @Test
  def testSingleRowFullOuterJoinWithoutEqualCond(): Unit = {
    if (expectedJoinType == NestedLoopJoin) {
      checkResult(
        "SELECT c, mc FROM SmallTable3 t1 FULL OUTER JOIN " +
          "(SELECT min(b) AS mb, max(c) AS mc FROM SmallTable3) t2 ON b > mb",
        Seq(
          row("Hello world", "Hi"), row("Hello", "Hi"), row("Hi", null)
        ))
    }
  }

  @Test
  def testSingleRowFullOuterJoinWithoutEqualCondNoMatch(): Unit = {
    if (expectedJoinType == NestedLoopJoin) {
      checkResult(
        "SELECT c, mc FROM SmallTable3 t1 FULL OUTER JOIN " +
          "(SELECT max(b) AS mb, max(c) AS mc FROM SmallTable3) t2 ON b > mb",
        Seq(
          row("Hello world", null), row("Hello", null), row("Hi", null), row(null, "Hi")
        ))
    }
  }

  @Test
  def testLeftOuterJoin(): Unit = {
    checkResult(
      "SELECT c, g FROM Table5 LEFT OUTER JOIN SmallTable3 ON b = e",
      Seq(
        row("Hi", "Hallo"), row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt"),
        row(null, "Hallo Welt wie gehts?"), row(null, "Hallo Welt wie"),
        row(null, "ABC"), row(null, "BCD"), row(null, "CDE"),
        row(null, "DEF"), row(null, "EFG"), row(null, "FGH"),
        row(null, "GHI"), row(null, "HIJ"), row(null, "IJK"),
        row(null, "JKL"), row(null, "KLM")
      ))
  }

  @Test
  def testRightOuterJoin(): Unit = {
    checkResult(
      "SELECT c, g FROM SmallTable3 RIGHT OUTER JOIN Table5 ON b = e",
      Seq(
        row("Hi", "Hallo"), row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt"),
        row(null, "Hallo Welt wie gehts?"), row(null, "Hallo Welt wie"),
        row(null, "ABC"), row(null, "BCD"), row(null, "CDE"),
        row(null, "DEF"), row(null, "EFG"), row(null, "FGH"),
        row(null, "GHI"), row(null, "HIJ"), row(null, "IJK"),
        row(null, "JKL"), row(null, "KLM")
      ))
  }

  @Test
  def testJoinWithAggregation(): Unit = {
    checkResult(
      "SELECT COUNT(g), COUNT(b) FROM SmallTable3, Table5 WHERE a = d",
      Seq(row(6L, 6L)))
  }

  @Test
  def testJoinConditionNeedSimplify(): Unit = {
    checkResult(
      "SELECT A.d FROM Table5 A JOIN SmallTable3 B ON (A.d=B.a and B.a>2) or (A.d=B.a and B.b=1)",
      Seq(row(1), row(3), row(3), row(3)))
  }

  @Test
  def testJoinConditionDerivedFromCorrelatedSubQueryNeedSimplify(): Unit = {
    checkResult(
      "SELECT B.a FROM SmallTable3 B WHERE b = (" +
        "select count(*) from Table5 A where (A.d=B.a and A.d<3) or (A.d=B.a and B.b=5))",
      Seq(row(1), row(2)))
  }

  @Test
  def testSimple(): Unit = {
    checkResult(
      "select a, b from l where a in (select c from r where c > 2)",
      Seq(row(3, 3.0), row(6, null)))
  }

  @Test
  def testSelect(): Unit = {
    checkResult(
      "select t.a from (select 1 as a)t",
      Seq(row(1)))
  }

  @Test
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

  @Test
  def testCorrelatedExist(): Unit = {
    checkResult(
      "select * from l where exists (select * from r where l.a = r.c)",
      Seq(row(2, 1.0), row(2, 1.0), row(3, 3.0), row(6, null)))

    checkResult(
      "select * from l where exists (select * from r where l.a = r.c) and l.a <= 2",
      Seq(row(2, 1.0), row(2, 1.0)))
  }

  @Test
  def testCorrelatedExist2(): Unit = {
    val data: Seq[Row] = Seq(
      row(0L),
      row(123456L),
      row(-123456L),
      row(2147483647L),
      row(-2147483647L))
    registerCollection("t1", data, new RowTypeInfo(LONG_TYPE_INFO), "f1")

    checkResult(
      "select * from t1 o where exists (select 1 from t1 i where i.f1=o.f1 limit 0)",
      Seq())
  }

  @Test
  def testCorrelatedNotExist(): Unit = {
    checkResult(
      "select * from l where not exists (select * from r where l.a = r.c and l.b <> r.d)",
      Seq(row(1, 2.0), row(1, 2.0), row(6, null), row(null, 5.0), row(null, null)))
  }

  @Test
  def testUncorrelatedScalar(): Unit = {
    checkResult(
      "select (select 1) as b",
      Seq(row(1)))

    checkResult(
      "select (select 1 as b)",
      Seq(row(1)))

    checkResult(
      "select (select 1 as a) as b",
      Seq(row(1)))
  }

  @Test
  def testEqualWithAggScalar(): Unit = {
    checkResult(
      "select a, b from l where a = (select distinct (c) from r where c = 2)",
      Seq(row(2, 1.0), row(2, 1.0)))
  }

  @Test
  def testComparisonsScalar(): Unit = {
    if (expectedJoinType == NestedLoopJoin) {
      checkEmptyResult(
        "select a, b from l where a = (select c from r where 1 = 2)")

      checkResult(
        "select a, b from l where a >= 1.0 * (select avg(d) from r where c > 2)",
        row(2, 1.0) :: row(2, 1.0) :: row(3, 3.0) :: row(6, null) :: Nil)
    }

    checkResult(
      "select a, b from l where a * b < 2.0 * (select avg(d) from r where l.a = r.c and c < 6 )",
      row(2, 1.0) :: row(2, 1.0) :: Nil)
  }

  @Test
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
        ))

      checkResult(
        "SELECT c, g FROM NullTable3, NullTable5 " +
            "WHERE (a = d OR (a IS NULL AND d IS NULL)) and c = 'NullTuple'",
        Seq(
          row("NullTuple", "NullTuple"),
          row("NullTuple", "NullTuple"),
          row("NullTuple", "NullTuple"),
          row("NullTuple", "NullTuple")
        ))

      registerCollection(
        "NullT", Seq(row(null, null, "c")), type3, "a, b, c", allNullablesOfNullData3)
      checkResult(
        "SELECT T1.a, T1.b, T1.c FROM NullT T1, NullT T2 WHERE " +
            "(T1.a = T2.a OR (T1.a IS NULL AND T2.a IS NULL)) " +
            "AND (T1.b = T2.b OR (T1.b IS NULL AND T2.b IS NULL)) AND T1.c = T2.c",
        Seq(row("null", "null", "c")))
    }
  }

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
  def testJoinWithUDFFilter(): Unit = {
    registerFunction("funcWithOpen", new FuncWithOpen)
    checkResult(
      "SELECT c, g FROM SmallTable3 join Table5 on funcWithOpen(a + d) where b = e",
      Seq(
        row("Hi", "Hallo"),
        row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt"))
    )
  }
}

object JoinITCase {
  @Parameterized.Parameters(name = "{0}")
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
