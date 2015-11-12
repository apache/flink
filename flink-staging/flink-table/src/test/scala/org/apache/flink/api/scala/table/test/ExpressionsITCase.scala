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

import org.apache.flink.api.table.{Row, ExpressionException}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.test.util.{TestBaseUtils, MultipleProgramsTestBase}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class ExpressionsITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  @Test
  def testArithmetic(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements((5, 10)).as('a, 'b)
      .select('a - 5, 'a + 5, 'a / 2, 'a * 2, 'a % 2, -'a).toDataSet[Row]
    val expected = "0,10,2,10,1,-5"
    val results = ds.collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testLogic(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements((5, true)).as('a, 'b)
      .select('b && true, 'b && false, 'b || false, !'b).toDataSet[Row]
    val expected = "true,false,true,false"
    val results = ds.collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testComparisons(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements((5, 5, 4)).as('a, 'b, 'c)
      .select('a > 'c, 'a >= 'b, 'a < 'c, 'a.isNull, 'a.isNotNull).toDataSet[Row]
    val expected = "true,true,false,false,true"
    val results = ds.collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testBitwiseOperations(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val ds = env.fromElements((3.toByte, 5.toByte)).as('a, 'b)
      .select('a & 'b, 'a | 'b, 'a ^ 'b, ~'a).toDataSet[Row]
    val expected = "1,7,6,-4"
    val results = ds.collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testBitwiseWithAutocast(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val ds = env.fromElements((3, 5.toByte)).as('a, 'b)
      .select('a & 'b, 'a | 'b, 'a ^ 'b, ~'a).toDataSet[Row]
    val expected = "1,7,6,-4"
    val results = ds.collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[ExpressionException])
  def testBitwiseWithNonWorkingAutocast(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val ds = env.fromElements((3.0, 5)).as('a, 'b)
      .select('a & 'b, 'a | 'b, 'a ^ 'b, ~'a).toDataSet[Row] 
    val expected = "1,7,6,-4"
    val results = ds.collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCaseInsensitiveForAs(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val ds = env.fromElements((3, 5.toByte)).as('a, 'b)
      .groupBy("a").select("a, a.count As cnt").toDataSet[Row]
    val expected = "3,1"
    val results = ds.collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

}
