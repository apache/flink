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

import org.apache.flink.api.table.ExpressionException
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.{TestBaseUtils, MultipleProgramsTestBase}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.junit._
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class ExpressionsITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
  private var resultPath: String = null
  private var expected: String = ""
  private val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder = _tempFolder

  @Before
  def before(): Unit = {
    resultPath = tempFolder.newFile().toURI.toString
  }

  @After
  def after(): Unit = {
    TestBaseUtils.compareResultsByLinesInMemory(expected, resultPath)
  }

  @Test
  def testArithmetic(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements((5, 10)).as('a, 'b)
      .select('a - 5, 'a + 5, 'a / 2, 'a * 2, 'a % 2, -'a)

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "0,10,2,10,1,-5"
  }

  @Test
  def testLogic(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements((5, true)).as('a, 'b)
      .select('b && true, 'b && false, 'b || false, !'b)

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "true,false,true,false"
  }

  @Test
  def testComparisons(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements((5, 5, 4)).as('a, 'b, 'c)
      .select('a > 'c, 'a >= 'b, 'a < 'c, 'a.isNull, 'a.isNotNull)

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "true,true,false,false,true"
  }

  @Test
  def testBitwiseOperations(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val ds = env.fromElements((3.toByte, 5.toByte)).as('a, 'b)
      .select('a & 'b, 'a | 'b, 'a ^ 'b, ~'a)

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "1,7,6,-4"
  }

  @Test
  def testBitwiseWithAutocast(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val ds = env.fromElements((3, 5.toByte)).as('a, 'b)
      .select('a & 'b, 'a | 'b, 'a ^ 'b, ~'a)

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "1,7,6,-4"
  }

  @Test(expected = classOf[ExpressionException])
  def testBitwiseWithNonWorkingAutocast(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val ds = env.fromElements((3.0, 5)).as('a, 'b)
      .select('a & 'b, 'a | 'b, 'a ^ 'b, ~'a)

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "1,7,6,-4"
  }

  @Test
  def testCaseInsensitiveForAs(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val ds = env.fromElements((3, 5.toByte)).as('a, 'b)
      .groupBy("a").select("a, a.count As cnt")

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "3,1"
  }

}
