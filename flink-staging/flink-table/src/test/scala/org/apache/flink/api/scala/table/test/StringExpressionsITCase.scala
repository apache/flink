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
import org.apache.flink.test.util.MultipleProgramsTestBase
import org.apache.flink.test.util.AbstractMultipleProgramsTestBase.TestExecutionMode
import org.junit._
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class StringExpressionsITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
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
  def after: Unit = {
    compareResultsByLinesInMemory(expected, resultPath)
  }

  @Test
  def testSubstring: Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(("AAAA", 2), ("BBBB", 1)).as('a, 'b)
      .select('a.substring(0, 'b))

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "AA\nB"
  }

  @Test
  def testSubstringWithMaxEnd: Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(("ABCD", 2), ("ABCD", 1)).as('a, 'b)
      .select('a.substring('b))

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "CD\nBCD"
  }

  @Test(expected = classOf[ExpressionException])
  def testNonWorkingSubstring1: Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(("AAAA", 2.0), ("BBBB", 1.0)).as('a, 'b)
      .select('a.substring(0, 'b))

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "AAA\nBB"
  }

  @Test(expected = classOf[ExpressionException])
  def testNonWorkingSubstring2: Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(("AAAA", "c"), ("BBBB", "d")).as('a, 'b)
      .select('a.substring('b, 15))

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "AAA\nBB"
  }


}
