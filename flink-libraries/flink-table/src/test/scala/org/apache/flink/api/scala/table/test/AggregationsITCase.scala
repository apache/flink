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

import org.apache.flink.api.table.{ExpressionException, Row}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.test.util.{TestBaseUtils, MultipleProgramsTestBase}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class AggregationsITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  @Ignore //DataSetMap needs to be implemented
  @Test
  def testAggregationTypes(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).toTable
      .select('_1.sum, '_1.min, '_1.max, '_1.count, '_1.avg)

    val results = t.toDataSet[Row].collect()
    val expected = "231,1,21,21,11"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testAggregationOnNonExistingField(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).toTable
      .select('foo.avg)

    val expected = ""
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testWorkingAggregationDataTypes(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements(
      (1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d, "Hello"),
      (2: Byte, 2: Short, 2, 2L, 2.0f, 2.0d, "Ciao")).toTable
      .select('_1.avg, '_2.avg, '_3.avg, '_4.avg, '_5.avg, '_6.avg, '_7.count)

    val expected = "1,1,1,1,1.5,1.5,2"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Ignore // it seems like the arithmetic expression is added to the field position
  @Test(expected = classOf[NotImplementedError])
  def testAggregationWithArithmetic(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements((1f, "Hello"), (2f, "Ciao")).toTable
      .select(('_1 + 2).avg + 2, '_2.count + 5)

    val expected = "5.5,7"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAggregationWithTwoCount(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements((1f, "Hello"), (2f, "Ciao")).toTable
      .select('_1.count, '_2.count)

    val expected = "2,2"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Ignore // Calcite does not eagerly check types
  @Test(expected = classOf[ExpressionException])
  def testNonWorkingAggregationDataTypes(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements(("Hello", 1)).toTable
      .select('_1.sum)

    val expected = ""
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testNoNestedAggregations(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements(("Hello", 1)).toTable
      .select('_2.sum.sum)

    val expected = ""
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSQLStyleAggregations(): Unit = {

    // the grouping key needs to be forwarded to the intermediate DataSet, even
    // if we don't want the key in the output

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
      .select(
        """Sum( a) as a1, a.sum as a2,
          |Min (a) as b1, a.min as b2,
          |Max (a ) as c1, a.max as c2,
          |Avg ( a ) as d1, a.avg as d2,
          |Count(a) as e1, a.count as e2
        """.stripMargin)

    val expected = "231,231,1,1,21,21,11,11,21,21"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

}
