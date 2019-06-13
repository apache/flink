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

package org.apache.flink.table.runtime.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.batch.sql.join.JoinITCaseHelper.disableOtherJoinOpForJoin
import org.apache.flink.table.runtime.batch.sql.join.JoinType
import org.apache.flink.table.runtime.batch.sql.join.JoinType.JoinType
import org.apache.flink.table.runtime.utils.{BatchScalaTableEnvUtil, BatchTestBase, CollectionBatchExecTable}
import org.apache.flink.table.util.TableFunc2
import org.apache.flink.test.util.TestBaseUtils

import org.junit._

import scala.collection.JavaConverters._

class JoinITCase extends BatchTestBase {

  val expectedJoinType: JoinType = JoinType.SortMergeJoin

  @Before
  def before(): Unit = {
    disableOtherJoinOpForJoin(tEnv, expectedJoinType)
  }

  @Test
  def testJoin(): Unit = {
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.get5TupleDataSet(tEnv, "d, e, f, g, h")

    val joinT = ds1.join(ds2).where('b === 'e).select('c, 'g)

    val expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n"
    val results = executeQuery(joinT)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testJoinWithFilter(): Unit = {
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv).as('a, 'b, 'c)
    val ds2 = CollectionBatchExecTable.get5TupleDataSet(tEnv).as('d, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('b === 'e && 'b < 2).select('c, 'g)

    val expected = "Hi,Hallo\n"
    val results = executeQuery(joinT)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testJoinWithJoinFilter(): Unit = {
    val ds1 = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.get5TupleDataSet(tEnv, "d, e, f, g, h")

    val joinT = ds1.join(ds2).where('b === 'e && 'a < 6).select('c, 'g)

    val expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n" +
      "Hello world, how are you?,Hallo Welt wie\n" + "I am fine.,Hallo Welt wie\n"
    val results = executeQuery(joinT)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testInnerJoinWithNonEquiJoinPredicate(): Unit = {
    val ds1 = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.get5TupleDataSet(tEnv, "d, e, f, g, h")

    val joinT = ds1.join(ds2).where('b === 'e && 'a < 6 && 'h < 'b).select('c, 'g)

    val results = executeQuery(joinT)
    val expected = "Hello world, how are you?,Hallo Welt wie\n" + "I am fine.,Hallo Welt wie\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testJoinWithMultipleKeys(): Unit = {
    val ds1 = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.get5TupleDataSet(tEnv, "d, e, f, g, h")

    val joinT = ds1.join(ds2).filter('a === 'd && 'b === 'h).select('c, 'g)

    val expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt wie gehts?\n" +
    "Hello world,ABC\n" + "I am fine.,HIJ\n" + "I am fine.,IJK\n"
    val results = executeQuery(joinT)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testJoinWithAggregation(): Unit = {
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.get5TupleDataSet(tEnv, "d, e, f, g, h")

    val joinT = ds1.join(ds2).where('a === 'd).select('g.count)

    val expected = "6"
    val results = executeQuery(joinT)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testJoinWithGroupedAggregation(): Unit = {
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.get5TupleDataSet(tEnv, "d, e, f, g, h")

    val joinT = ds1.join(ds2)
      .where('a === 'd)
      .groupBy('a, 'd)
      .select('b.sum, 'g.count)

    val expected = "6,3\n" + "4,2\n" + "1,1"
    val results = executeQuery(joinT)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testJoinPushThroughJoin(): Unit = {
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.get5TupleDataSet(tEnv, "d, e, f, g, h")
    val ds3 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "j, k, l")

    val joinT = ds1.join(ds2)
      .where(true)
      .join(ds3)
      .where('a === 'd && 'e === 'k)
      .select('a, 'f, 'l)

    val expected = "2,1,Hello\n" + "2,1,Hello world\n" + "1,0,Hi"
    val results = executeQuery(joinT)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testJoinWithDisjunctivePred(): Unit = {
    val ds1 = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.get5TupleDataSet(tEnv, "d, e, f, g, h")

    val joinT = ds1.join(ds2).filter('a === 'd && ('b === 'e || 'b === 'e - 10)).select('c, 'g)

    val expected = "Hi,Hallo\n" +
      "Hello,Hallo Welt\n" +
      "I am fine.,IJK"
    val results = executeQuery(joinT)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testJoinWithExpressionPreds(): Unit = {
    val ds1 = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.get5TupleDataSet(tEnv, "d, e, f, g, h")

    val joinT = ds1.join(ds2).filter('b === 'h + 1 && 'a - 1 === 'd + 2).select('c, 'g)

    val expected = "I am fine.,Hallo Welt\n" +
      "Luke Skywalker,Hallo Welt wie gehts?\n" +
      "Luke Skywalker,ABC\n" +
      "Comment#2,HIJ\n" +
      "Comment#2,IJK"
    val results = executeQuery(joinT)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testLeftJoinWithMultipleKeys(): Unit = {
    val ds1 = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.get5TupleDataSet(tEnv, "d, e, f, g, h")

    val joinT = ds1.leftOuterJoin(ds2, 'a === 'd && 'b === 'h).select('c, 'g)

    val expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt wie gehts?\n" +
      "Hello world,ABC\n" + "Hello world, how are you?,null\n" + "I am fine.,HIJ\n" +
      "I am fine.,IJK\n" + "Luke Skywalker,null\n" + "Comment#1,null\n" + "Comment#2,null\n" +
      "Comment#3,null\n" + "Comment#4,null\n" + "Comment#5,null\n" + "Comment#6,null\n" +
      "Comment#7,null\n" + "Comment#8,null\n" + "Comment#9,null\n" + "Comment#10,null\n" +
      "Comment#11,null\n" + "Comment#12,null\n" + "Comment#13,null\n" + "Comment#14,null\n" +
      "Comment#15,null\n"
    val results = executeQuery(joinT)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testRightJoinWithMultipleKeys(): Unit = {
    val ds1 = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.get5TupleDataSet(tEnv, "d, e, f, g, h")

    val joinT = ds1.rightOuterJoin(ds2, 'a === 'd && 'b === 'h).select('c, 'g)

    val expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "null,Hallo Welt wie\n" +
      "Hello world,Hallo Welt wie gehts?\n" + "Hello world,ABC\n" + "null,BCD\n" + "null,CDE\n" +
      "null,DEF\n" + "null,EFG\n" + "null,FGH\n" + "null,GHI\n" + "I am fine.,HIJ\n" +
      "I am fine.,IJK\n" + "null,JKL\n" + "null,KLM\n"
    val results = executeQuery(joinT)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFullOuterJoinWithMultipleKeys(): Unit = {
    val ds1 = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.get5TupleDataSet(tEnv, "d, e, f, g, h")

    val joinT = ds1.fullOuterJoin(ds2, 'a === 'd && 'b === 'h).select('c, 'g)

    val expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "null,Hallo Welt wie\n" +
      "Hello world,Hallo Welt wie gehts?\n" + "Hello world,ABC\n" + "null,BCD\n" + "null,CDE\n" +
      "null,DEF\n" + "null,EFG\n" + "null,FGH\n" + "null,GHI\n" + "I am fine.,HIJ\n" +
      "I am fine.,IJK\n" + "null,JKL\n" + "null,KLM\n" + "Luke Skywalker,null\n" +
      "Comment#1,null\n" + "Comment#2,null\n" + "Comment#3,null\n" + "Comment#4,null\n" +
      "Comment#5,null\n" + "Comment#6,null\n" + "Comment#7,null\n" + "Comment#8,null\n" +
      "Comment#9,null\n" + "Comment#10,null\n" + "Comment#11,null\n" + "Comment#12,null\n" +
      "Comment#13,null\n" + "Comment#14,null\n" + "Comment#15,null\n" +
      "Hello world, how are you?,null\n"
    val results = executeQuery(joinT)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUDTFJoinOnTuples(): Unit = {
    val data = List("hi#world", "how#are#you")

    val ds1 = BatchScalaTableEnvUtil.fromCollection(tEnv, data, "a")
    val func2 = new TableFunc2

    val joinDs = ds1.joinLateral(func2('a) as ('name, 'len))

    val results = executeQuery(joinDs)
    val expected = Seq(
      "hi#world,hi,2",
      "hi#world,world,5",
      "how#are#you,how,3",
      "how#are#you,are,3",
      "how#are#you,you,3").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

}
