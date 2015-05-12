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
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.MultipleProgramsTestBase
import org.apache.flink.test.util.AbstractMultipleProgramsTestBase.TestExecutionMode
import org.junit._
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class CastingITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
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
  def testAutoCastToString: Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements((1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d)).toTable
      .select('_1 + "b", '_2 + "s", '_3 + "i", '_4 + "L", '_5 + "f", '_6 + "d")

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "1b,1s,1i,1L,1.0f,1.0d"
  }

  @Test
  def testNumericAutoCastInArithmetic: Unit = {

    // don't test everything, just some common cast directions

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements((1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d)).toTable
      .select('_1 + 1, '_2 + 1, '_3 + 1L, '_4 + 1.0f, '_5 + 1.0d, '_6 + 1)

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "2,2,2,2.0,2.0,2.0"
  }

  @Test
  def testNumericAutoCastInComparison: Unit = {

    // don't test everything, just some common cast directions

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(
      (1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d),
      (2: Byte, 2: Short, 2, 2L, 2.0f, 2.0d)).as('a, 'b, 'c, 'd, 'e, 'f)
      .filter('a > 1 && 'b > 1 && 'c > 1L && 'd > 1.0f && 'e > 1.0d  && 'f > 1)

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "2,2,2,2,2.0,2.0"
  }

}
