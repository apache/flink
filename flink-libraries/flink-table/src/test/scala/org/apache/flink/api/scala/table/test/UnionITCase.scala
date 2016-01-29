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

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.table.{ExpressionException, Row}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class UnionITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  @Test(expected = classOf[NotImplementedError])
  def testUnion(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)

    val unionDs = ds1.unionAll(ds2).select('c)

    val results = unionDs.toDataSet[Row].collect()
    val expected = "Hi\n" + "Hello\n" + "Hello world\n" + "Hi\n" + "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[NotImplementedError])
  def testUnionWithFilter(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('a, 'b, 'd, 'c, 'e)

    val joinDs = ds1.unionAll(ds2.select('a, 'b, 'c)).filter('b < 2).select('c)

    val results = joinDs.toDataSet[Row].collect()
    val expected = "Hi\n" + "Hallo\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testUnionFieldsNameNotOverlap1(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('a, 'b, 'd, 'c, 'e)

    val unionDs = ds1.unionAll(ds2)

    val results = unionDs.toDataSet[Row].collect()
    val expected = ""
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testUnionFieldsNameNotOverlap2(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('a, 'b, 'c, 'd, 'e).select('a, 'b, 'c)

    val unionDs = ds1.unionAll(ds2)

    val results = unionDs.toDataSet[Row].collect()
    val expected = ""
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[NotImplementedError])
  def testUnionWithAggregation(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('a, 'b, 'd, 'c, 'e)

    val unionDs = ds1.unionAll(ds2.select('a, 'b, 'c)).select('c.count)

    val results = unionDs.toDataSet[Row].collect()
    val expected = "18"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}
