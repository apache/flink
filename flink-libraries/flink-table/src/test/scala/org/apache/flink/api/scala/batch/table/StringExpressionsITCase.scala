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

package org.apache.flink.api.scala.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.codegen.CodeGenException
import org.apache.flink.api.table.{Row, TableEnvironment}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class StringExpressionsITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  @Test
  def testSubstring(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = env.fromElements(("AAAA", 2), ("BBBB", 1)).toTable(tEnv, 'a, 'b)
      .select('a.substring(1, 'b))

    val expected = "AA\nB"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSubstringWithMaxEnd(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = env.fromElements(("ABCD", 3), ("ABCD", 2)).toTable(tEnv, 'a, 'b)
      .select('a.substring('b))

    val expected = "CD\nBCD"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[CodeGenException])
  def testNonWorkingSubstring1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = env.fromElements(("AAAA", 2.0), ("BBBB", 1.0)).toTable(tEnv, 'a, 'b)
      // must fail, second argument of substring must be Integer not Double.
      .select('a.substring(0, 'b))

    t.toDataSet[Row].collect()
  }

  @Test(expected = classOf[CodeGenException])
  def testNonWorkingSubstring2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = env.fromElements(("AAAA", "c"), ("BBBB", "d")).toTable(tEnv, 'a, 'b)
      // must fail, first argument of substring must be Integer not String.
      .select('a.substring('b, 15))

    t.toDataSet[Row].collect()
  }


}
