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

import org.apache.flink.api.table.{TableException, Row}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.table.expressions.Literal
import org.apache.flink.test.util.{TestBaseUtils, MultipleProgramsTestBase}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class JoinITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  @Test
  def testJoin(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('b === 'e).select('c, 'g)

    val expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n"
    val results = joinT.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testJoinWithFilter(): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('b === 'e && 'b < 2).select('c, 'g)

    val expected = "Hi,Hallo\n"
    val results = joinT.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testJoinWithJoinFilter(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('b === 'e && 'a < 6 && 'h < 'b).select('c, 'g)

    val expected = "Hello world, how are you?,Hallo Welt wie\n" +
      "I am fine.,Hallo Welt wie\n"
    val results = joinT.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testJoinWithMultipleKeys(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).filter('a === 'd && 'b === 'h).select('c, 'g)

    val expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt wie gehts?\n" +
      "Hello world,ABC\n" + "I am fine.,HIJ\n" + "I am fine.,IJK\n"
    val results = joinT.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testJoinNonExistingKey(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. Field 'foo does not exist
      .where('foo === 'e)
      .select('c, 'g)
  }

  @Test(expected = classOf[TableException])
  def testJoinWithNonMatchingKeyTypes(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. Field 'a is Int, and 'g is String
      .where('a === 'g)
      .select('c, 'g).collect()
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testJoinWithAmbiguousFields(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'c)

    ds1.join(ds2)
      // must fail. Both inputs share the same field 'c
      .where('a === 'd)
      .select('c, 'g)
  }

  @Test(expected = classOf[TableException])
  def testNoEqualityJoinPredicate1(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. No equality join predicate
      .where('d === 'f)
      .select('c, 'g).collect()
  }

  @Test(expected = classOf[TableException])
  def testNoEqualityJoinPredicate2(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. No equality join predicate
      .where('a < 'd)
      .select('c, 'g).collect()
  }

  @Test
  def testJoinWithAggregation(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('a === 'd).select('g.count)

    val expected = "6"
    val results = joinT.toDataSet[Row]collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testJoinWithGroupedAggregation(): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2)
      .where('a === 'd)
      .groupBy('a, 'd)
      .select('b.sum, 'g.count)

    val expected = "6,3\n" + "4,2\n" + "1,1"
    val results = joinT.toDataSet[Row]collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testJoinPushThroughJoin(): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)
    val ds3 = CollectionDataSets.getSmall3TupleDataSet(env).as('j, 'k, 'l)

    val joinT = ds1.join(ds2)
      .where(Literal(true))
      .join(ds3)
      .where('a === 'd && 'e === 'k)
      .select('a, 'f, 'l)

    val expected = "2,1,Hello\n" + "2,1,Hello world\n" + "1,0,Hi"
    val results = joinT.toDataSet[Row]collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testJoinWithDisjunctivePred(): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).filter('a === 'd && ('b === 'e || 'b === 'e - 10 )).select('c, 'g)

    val expected =
      "Hi,Hallo\n" +
        "Hello,Hallo Welt\n" +
        "I am fine.,IJK"
    val results = joinT.toDataSet[Row]collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testJoinWithExpressionPreds(): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).filter('b === 'h + 1 && 'a - 1 === 'd + 2).select('c, 'g)

    val expected =
        "I am fine.,Hallo Welt\n" +
        "Luke Skywalker,Hallo Welt wie gehts?\n" +
        "Luke Skywalker,ABC\n" +
        "Comment#2,HIJ\n" +
        "Comment#2,IJK"
    val results = joinT.toDataSet[Row]collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

}
