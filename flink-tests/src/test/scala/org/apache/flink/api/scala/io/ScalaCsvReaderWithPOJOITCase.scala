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

package org.apache.flink.api.scala.io

import java.util.Locale

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.util.FileUtils
import org.junit.Assert._
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Rule, Test}

@RunWith(classOf[Parameterized])
class ScalaCsvReaderWithPOJOITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
  private val _tempFolder = new TemporaryFolder()
  private var resultPath: String = null
  private var expected: String = null

  @Rule
  def tempFolder = _tempFolder

  @Before
  def before(): Unit = {
    resultPath = tempFolder.newFile("result").toURI.toString
  }

  @After
  def after(): Unit = {
    TestBaseUtils.compareResultsByLinesInMemory(expected, resultPath)
  }

  def createInputData(data: String): String = {
    val dataFile = tempFolder.newFile("data")
    FileUtils.writeFileUtf8(dataFile, data)
    dataFile.toURI.toString
  }

  @Test
  def testPOJOType(): Unit = {
    val dataPath = createInputData("ABC,2.20,3\nDEF,5.1,5\nDEF,3.30,1\nGHI,3.30,10")
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.readCsvFile[POJOItem](dataPath, pojoFields = Array("f1", "f2", "f3"))

    implicit val typeInfo = createTypeInformation[(String, Int)]
    data.writeAsText(resultPath, WriteMode.OVERWRITE)

    env.execute()

    expected = "ABC,2.20,3\nDEF,5.10,5\nDEF,3.30,1\nGHI,3.30,10"
  }

  @Test
  def testPOJOTypeWithFieldsOrder(): Unit = {
    val dataPath = createInputData("2.20,ABC,3\n5.1,DEF,5\n3.30,DEF,1\n3.30,GHI,10")
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.readCsvFile[POJOItem](dataPath, pojoFields = Array("f2", "f1", "f3"))

    implicit val typeInfo = createTypeInformation[(String, Int)]
    data.writeAsText(resultPath, WriteMode.OVERWRITE)

    env.execute()

    expected = "ABC,2.20,3\nDEF,5.10,5\nDEF,3.30,1\nGHI,3.30,10"
  }

  @Test
  def testPOJOTypeWithoutFieldsOrder(): Unit = {
    val dataPath = createInputData("")
    val env = ExecutionEnvironment.getExecutionEnvironment

    try {
      val data = env.readCsvFile[POJOItem](dataPath)
      fail("POJO type without fields order must raise IllegalArgumentException!")
    } catch {
      case _: IllegalArgumentException => // success
    }

    expected = ""
    resultPath = dataPath
  }

  @Test
  def testPOJOTypeWithFieldsOrderAndFieldsSelection(): Unit = {
    val dataPath = createInputData("2.20,3,ABC\n5.1,5,DEF\n3.30,1,DEF\n3.30,10,GHI")
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.readCsvFile[POJOItem](dataPath, includedFields = Array(1, 2),
      pojoFields = Array("f3", "f1"))

    implicit val typeInfo = createTypeInformation[(String, Int)]
    data.writeAsText(resultPath, WriteMode.OVERWRITE)

    env.execute()

    expected = "ABC,0.00,3\nDEF,0.00,5\nDEF,0.00,1\nGHI,0.00,10"
  }
}

class POJOItem(var f1: String, var f2: Double, var f3: Int) {
  def this() {
    this("", 0.0, 0)
  }

  override def toString: String = "%s,%.02f,%d".formatLocal(Locale.US, f1, f2, f3)
}
