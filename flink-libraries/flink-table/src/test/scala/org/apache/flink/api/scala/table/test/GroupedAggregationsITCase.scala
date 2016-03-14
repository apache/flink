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

package org.apache.flink.api.scala.table.test

import org.apache.flink.api.table.Row
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.table.expressions.Literal
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class GroupedAggregationsITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  @Test(expected = classOf[IllegalArgumentException])
  def testGroupingOnNonExistentField(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
      // must fail. '_foo not a valid field
      .groupBy('_foo)
      .select('a.avg)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testGroupingInvalidSelection(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
      .groupBy('a, 'b)
      // must fail. 'c is not a grouping key or aggregation
      .select('c)
  }

  @Test
  def testGroupedAggregate(): Unit = {

    // the grouping key needs to be forwarded to the intermediate DataSet, even
    // if we don't want the key in the output

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
      .groupBy('b)
      .select('b, 'a.sum)

    val expected = "1,1\n" + "2,5\n" + "3,15\n" + "4,34\n" + "5,65\n" + "6,111\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupingKeyForwardIfNotUsed(): Unit = {

    // the grouping key needs to be forwarded to the intermediate DataSet, even
    // if we don't want the key in the output

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
      .groupBy('b)
      .select('a.sum)

    val expected = "1\n" + "5\n" + "15\n" + "34\n" + "65\n" + "111\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupNoAggregation(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env)
      .as('a, 'b, 'c)
      .groupBy('b)
      .select('a.sum as 'd, 'b)
      .groupBy('b, 'd)
      .select('b)

    val expected = "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupedAggregateWithLongKeys(): Unit = {
    // This uses very long keys to force serialized comparison.
    // With short keys, the normalized key is sufficient.
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhab", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhab", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhab", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhab", 1, 2))
      .rebalance().setParallelism(2).as('a, 'b, 'c)
      .groupBy('a, 'b)
      .select('c.sum)

    val expected = "10\n" + "8\n"
    val results = ds.collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupedAggregateWithConstant1(): Unit = {

    // verify AggregateProjectPullUpConstantsRule

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
      .select('a, 4 as 'four, 'b)
      .groupBy('four, 'a)
      .select('four, 'b.sum)

    val expected = "4,2\n" + "4,3\n" + "4,5\n" + "4,5\n" + "4,5\n" + "4,6\n" +
      "4,6\n" + "4,6\n" + "4,3\n" + "4,4\n" + "4,6\n" + "4,1\n" + "4,4\n" +
      "4,4\n" + "4,5\n" + "4,6\n" + "4,2\n" + "4,3\n" + "4,4\n" + "4,5\n" + "4,6\n"
    val results = t.toDataSet[Row].collect()

    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupedAggregateWithConstant2(): Unit = {

    // verify AggregateProjectPullUpConstantsRule

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
        .select('b, 4 as 'four, 'a)
        .groupBy('b, 'four)
        .select('four, 'a.sum)

    val expected = "4,1\n" + "4,5\n" + "4,15\n" + "4,34\n" + "4,65\n" + "4,111\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupedAggregateWithExpression(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get5TupleDataSet(env).as('a, 'b, 'c, 'd, 'e)
        .groupBy('e, 'b % 3)
        .select('c.min, 'e, 'a.avg, 'd.count)

    val expected = "0,1,1,1\n" + "3,2,3,3\n" + "7,1,4,2\n" + "14,2,5,1\n" +
        "5,3,4,2\n" + "2,1,3,2\n" + "1,2,3,3\n" + "12,3,5,1"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupedAggregateWithFilter(): Unit = {

    // the grouping key needs to be forwarded to the intermediate DataSet, even
    // if we don't want the key in the output

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
      .groupBy('b)
      .select('b, 'a.sum)
      .where('b === 2)

    val expected = "2,5\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}
