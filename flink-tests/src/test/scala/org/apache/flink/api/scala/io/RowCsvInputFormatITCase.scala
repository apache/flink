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

import org.apache.flink.types.Row
import org.junit.{Before, Rule, Test}
import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.Assert.assertTrue

@RunWith(classOf[Parameterized])
class RowCsvInputFormatITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
  private val _tempFolder = new TemporaryFolder()
  private var resultPath: String = _
  private val fullRowSize = 29
  private val fileContent: String =
    "1,2,3," + 4 + "," + 5.0d + "," + true +
      ",7,8,9,11,22,33,44,55,66,77,88,99,00," +
      "111,222,333,444,555,666,777,888,999,000\n" +
      "a,b,c," + 40 + "," + 50.0d + "," + false +
      ",g,h,i,aa,bb,cc,dd,ee,ff,gg,hh,ii,mm," +
      "aaa,bbb,ccc,ddd,eee,fff,ggg,hhh,iii,mmm\n"

  @Rule
  def tempFolder = _tempFolder

  @Before
  def before(): Unit = {
    resultPath = tempFolder.newFile("result").toURI.toString
  }

  def createInputData(data: String): String = {
    val dataFile = tempFolder.newFile("data")
    Files.write(data, dataFile, Charsets.UTF_8)
    dataFile.toURI.toString
  }

  @Test
  def testWideRowType(): Unit = {
    val dataPath = createInputData(fileContent)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.readCsvFileAsRow[String](dataPath, fullRowSize)

    data.writeAsText(resultPath, WriteMode.OVERWRITE)

    env.execute()

    TestBaseUtils.compareResultsByLinesInMemory(fileContent, resultPath)
  }

  @Test
  def testWideRowTypeWithAdditionalTypeMapAndIncludedFields(): Unit = {
    val rowSize = 6
    val dataPath = createInputData(fileContent)
    val env = ExecutionEnvironment.getExecutionEnvironment

    val typeMap = Map(
      2 -> classOf[Int],
      3 -> classOf[Double],
      4 -> classOf[Boolean])
    val including = Array(1, 2, 3, 4, 5, 6)

    val data = env.readCsvFileAsRow[String](
      dataPath,
      rowSize,
      typeMap,
      includedFields = including)
    val rows: Seq[Row] = data.collect()

    rows.foreach { f =>
      assertTrue(f.getField(2).isInstanceOf[Int])
      assertTrue(f.getField(3).isInstanceOf[Double])
      assertTrue(f.getField(4).isInstanceOf[Boolean])
    }
  }

  @Test
  def testWideRowTypeWithIncludedFields(): Unit = {
    val rowSize = 6
    val dataPath = createInputData(fileContent)
    val env = ExecutionEnvironment.getExecutionEnvironment

    val including = Array(1, 2, 3, 4, 5, 6)

    val data = env.readCsvFileAsRow[String](
      dataPath,
      rowSize,
      includedFields = including)

    data.writeAsText(resultPath, WriteMode.OVERWRITE)

    env.execute()

    val expected =
      "2,3,4,5.0,true,7\n" +
        "b,c,40,50.0,false,g\n"

    TestBaseUtils.compareResultsByLinesInMemory(expected, resultPath)
  }
}
