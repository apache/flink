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
import org.apache.flink.api.table.plan.TranslationContext
import org.apache.flink.test.util.MultipleProgramsTestBase

import org.junit._
import org.junit.Assert.assertEquals

class SqlExplainTest
  extends MultipleProgramsTestBase(MultipleProgramsTestBase.TestExecutionMode.CLUSTER) {

  val testFilePath = SqlExplainTest.this.getClass.getResource("/").getFile

  @Before
  def resetContext(): Unit = {
    TranslationContext.reset()
  }

  @Test
  def testFilterWithoutExtended() : Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val table = env.fromElements((1, "hello")).as('a, 'b)

    val result = table.filter("a % 2 = 0").explain()
    val source = scala.io.Source.fromFile(testFilePath +
      "../../src/test/scala/resources/testFilter0.out").mkString
    assertEquals(result, source)
  }

  @Test
  def testFilterWithExtended() : Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val table = env.fromElements((1, "hello")).as('a, 'b)

    val result = table.filter("a % 2 = 0").explain(true)
    val source = scala.io.Source.fromFile(testFilePath +
      "../../src/test/scala/resources/testFilter1.out").mkString
    assertEquals(result, source)
  }

  @Test
  def testJoinWithoutExtended() : Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val table1 = env.fromElements((1, "hello")).as('a, 'b)
    val table2 = env.fromElements((1, "hello")).as('c, 'd)

    val result = table1.join(table2).where("b = d").select("a, c").explain()
    val source = scala.io.Source.fromFile(testFilePath +
      "../../src/test/scala/resources/testJoin0.out").mkString
    assertEquals(result, source)
  }

  @Test
  def testJoinWithExtended() : Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val table1 = env.fromElements((1, "hello")).as('a, 'b)
    val table2 = env.fromElements((1, "hello")).as('c, 'd)

    val result = table1.join(table2).where("b = d").select("a, c").explain(true)
    val source = scala.io.Source.fromFile(testFilePath +
      "../../src/test/scala/resources/testJoin1.out").mkString
    assertEquals(result, source)
  }

  @Test
  def testUnionWithoutExtended() : Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val table1 = env.fromElements((1, "hello")).as('count, 'word)
    val table2 = env.fromElements((1, "hello")).as('count, 'word)

    val result = table1.unionAll(table2).explain()
    val source = scala.io.Source.fromFile(testFilePath +
      "../../src/test/scala/resources/testUnion0.out").mkString
    assertEquals(result, source)
  }

  @Test
  def testUnionWithExtended() : Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val table1 = env.fromElements((1, "hello")).as('count, 'word)
    val table2 = env.fromElements((1, "hello")).as('count, 'word)

    val result = table1.unionAll(table2).explain(true)
    val source = scala.io.Source.fromFile(testFilePath +
      "../../src/test/scala/resources/testUnion1.out").mkString
    assertEquals(result, source)
  }
}
