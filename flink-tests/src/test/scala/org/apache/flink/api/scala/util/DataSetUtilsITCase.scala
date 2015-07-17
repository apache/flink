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

package org.apache.flink.api.scala.util

import java.io.{FileWriter, PrintWriter, File}

import org.apache.flink.api.scala._
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.{After, Before, Rule, Test}
import org.apache.flink.api.scala.DataSetUtils.utilsToDataSet

@RunWith(classOf[Parameterized])
class DataSetUtilsITCase (mode: MultipleProgramsTestBase.TestExecutionMode) extends
MultipleProgramsTestBase(mode){

  private var resultPath: String = null
  private var expectedResult: String = null
  private val tempFolder: TemporaryFolder = new TemporaryFolder()

  @Rule
  def getFolder = tempFolder

  @Before
  @throws(classOf[Exception])
  def before(): Unit = {
    resultPath = tempFolder.newFile.toURI.toString
  }

  @Test
  @throws(classOf[Exception])
  def testZipWithIndex(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val input: DataSet[String] = env.fromElements("A", "B", "C", "D", "E", "F")
    val result: DataSet[(Long, String)] = input.zipWithIndex

    result.writeAsCsv(resultPath, "\n", ",")
    env.execute()

    expectedResult = "0,A\n" + "1,B\n" + "2,C\n" + "3,D\n" + "4,E\n" + "5,F"
  }

  @Test
  @throws(classOf[Exception])
  def testZipWithUniqueId(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val input: DataSet[String] = env.fromElements("A", "B", "C", "D", "E", "F")
    val result: DataSet[(Long, String)] = input.zipWithUniqueId

    result.writeAsCsv(resultPath, "\n", ",")
    env.execute()

    expectedResult = "0,A\n" + "2,B\n" + "4,C\n" + "6,D\n" + "8,E\n" + "10,F"
  }

  @Test
  @throws(classOf[Exception])
  def testRandomSplit(): Unit = {
    val tempFile = File.createTempFile("flinkTmpFile", "randomSplitTest")
    val writer = new PrintWriter(new FileWriter(tempFile))
    for (i <- 1 to 600) {
      writer.write(i + "\n")
    }
    writer.close()
    val data = ExecutionEnvironment.getExecutionEnvironment
      .readTextFile(tempFile.toString).setParallelism(2)
      .map(_.toInt)

    val splits = data.randomSplit(List(0.1, 0.3, 0.6))

    assertEquals(data.collect().sorted.toList, splits.flatMap(_.collect()).sorted)
    assertTrue(math.abs(splits.head.count() - 60) < 15) // std =  2.45
    assertTrue(math.abs(splits(1).count() - 180) < 30) // std =  7.35
    assertTrue(math.abs(splits(2).count() - 360) < 50) // std =  14.7
    expectedResult = ""
  }

  @After
  @throws(classOf[Exception])
  def after(): Unit = {
    TestBaseUtils.compareResultsByLinesInMemory(expectedResult, resultPath)
  }
}
