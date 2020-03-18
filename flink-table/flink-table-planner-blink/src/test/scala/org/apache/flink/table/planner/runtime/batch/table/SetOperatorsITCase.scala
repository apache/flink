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

package org.apache.flink.table.planner.runtime.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.runtime.batch.sql.join.JoinITCaseHelper.disableOtherJoinOpForJoin
import org.apache.flink.table.planner.runtime.batch.sql.join.JoinType
import org.apache.flink.table.planner.runtime.batch.sql.join.JoinType.JoinType
import org.apache.flink.table.planner.runtime.utils.{BatchTableEnvUtil, BatchTestBase, CollectionBatchExecTable}
import org.apache.flink.test.util.TestBaseUtils

import org.junit._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

class SetOperatorsITCase extends BatchTestBase {

  val expectedJoinType: JoinType = JoinType.SortMergeJoin

  @Before
  override def before(): Unit = {
    super.before()
    disableOtherJoinOpForJoin(tEnv, expectedJoinType)
  }

  @Test
  def testUnionAll(): Unit = {
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "d, e, f")

    val unionDs = ds1.unionAll(ds2).select('c)

    val results = executeQuery(unionDs)
    val expected = "Hi\n" + "Hello\n" + "Hello world\n" + "Hi\n" + "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUnion(): Unit = {
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "d, e, f")

    val unionDs = ds1.union(ds2).select('c)

    val results = executeQuery(unionDs)
    val expected = "Hi\n" + "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTernaryUnionAll(): Unit = {
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
    val ds3 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")

    val unionDs = ds1.unionAll(ds2).unionAll(ds3).select('c)

    val results = executeQuery(unionDs)
    val expected = "Hi\n" + "Hello\n" + "Hello world\n" +
      "Hi\n" + "Hello\n" + "Hello world\n" +
      "Hi\n" + "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTernaryUnion(): Unit = {
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
    val ds3 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")

    val unionDs = ds1.union(ds2).union(ds3).select('c)

    val results = executeQuery(unionDs)
    val expected = "Hi\n" + "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testMinusAll(): Unit = {
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv)
    val ds2 = BatchTableEnvUtil.fromElements(tEnv, (1, 1L, "Hi"))

    val minusDs = ds1.unionAll(ds1).unionAll(ds1)
      .minusAll(ds2.unionAll(ds2)).select('_3)

    val results = executeQuery(minusDs)
    val expected = "Hi\n" +
      "Hello\n" + "Hello world\n" +
      "Hello\n" + "Hello world\n" +
      "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testMinus(): Unit = {
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
    val ds2 = BatchTableEnvUtil.fromElements(tEnv, (1, 1L, "Hi"))

    val minusDs = ds1.unionAll(ds1).unionAll(ds1)
      .minus(ds2.unionAll(ds2)).select('c)

    val results = executeQuery(minusDs)
    val expected = "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testMinusDifferentFieldNames(): Unit = {
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
    val ds2 = BatchTableEnvUtil.fromElements(tEnv, (1, 1L, "Hi"))

    val minusDs = ds1.unionAll(ds1).unionAll(ds1)
      .minus(ds2.unionAll(ds2)).select('c)

    val results = executeQuery(minusDs)
    val expected = "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testIntersect(): Unit = {
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Hi"))
    data.+=((2, 2L, "Hello"))
    data.+=((2, 2L, "Hello"))
    data.+=((3, 2L, "Hello world!"))
    val ds2 = BatchTableEnvUtil.fromCollection(tEnv, Random.shuffle(data), "a, b, c")

    val intersectDS = ds1.intersect(ds2).select('c)

    val results = executeQuery(intersectDS)

    val expected = "Hi\n" + "Hello\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testIntersectAll(): Unit = {
    val data1 = new mutable.MutableList[Int]
    data1 += (1, 1, 1, 2, 2)
    val data2 = new mutable.MutableList[Int]
    data2 += (1, 2, 2, 2, 3)
    val ds1 = BatchTableEnvUtil.fromCollection(tEnv, data1, "c")
    val ds2 = BatchTableEnvUtil.fromCollection(tEnv, data2, "c")

    val intersectDS = ds1.intersectAll(ds2).select('c)

    val expected = "1\n2\n2"
    val results = executeQuery(intersectDS)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testIntersectWithDifferentFieldNames(): Unit = {
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.get3TupleDataSet(tEnv, "e, f, g")

    val intersectDs = ds1.intersect(ds2).select('c)

    val results = executeQuery(intersectDs)
    val expected = "Hi\n" + "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testIntersectWithScalarExpression(): Unit = {
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
      .select('a + 1, 'b, 'c)
    val ds2 = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
      .select('a + 1, 'b, 'c)

    val intersectDs = ds1.intersect(ds2)

    val results = executeQuery(intersectDs)
    val expected = "2,1,Hi\n" + "3,2,Hello\n" + "4,2,Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}
