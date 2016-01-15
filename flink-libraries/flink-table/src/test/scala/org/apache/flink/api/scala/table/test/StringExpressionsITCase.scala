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
class StringExpressionsITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  @Test
  def testSubstring(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements(("AAAA", 2), ("BBBB", 1)).as('a, 'b)
      .select('a.substring(0, 'b))

//    val expected = "AA\nB"
//    val results = t.toDataSet[Row].collect()
//    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSubstringWithMaxEnd(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements(("ABCD", 2), ("ABCD", 1)).as('a, 'b)
      .select('a.substring('b))

//    val expected = "CD\nBCD"
//    val results = t.toDataSet[Row].collect()
//    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  // Calcite does eagerly check expression types
  @Ignore
  @Test(expected = classOf[IllegalArgumentException])
  def testNonWorkingSubstring1(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements(("AAAA", 2.0), ("BBBB", 1.0)).as('a, 'b)
      .select('a.substring(0, 'b))

//    val expected = "AAA\nBB"
//    val results = t.toDataSet[Row].collect()
//    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  // Calcite does eagerly check expression types
  @Ignore
  @Test(expected = classOf[IllegalArgumentException])
  def testNonWorkingSubstring2(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements(("AAAA", "c"), ("BBBB", "d")).as('a, 'b)
      .select('a.substring('b, 15))

//    val expected = "AAA\nBB"
//    val results = t.toDataSet[Row].collect()
//    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }


}
