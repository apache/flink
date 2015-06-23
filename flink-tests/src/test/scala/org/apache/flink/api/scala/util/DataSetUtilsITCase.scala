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

import org.apache.flink.api.scala._
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Rule, Test}
import org.apache.flink.api.scala.DataSetUtils.utilsToDataSet

@RunWith(classOf[Parameterized])
class DataSetUtilsITCase (mode: MultipleProgramsTestBase.TestExecutionMode) extends
MultipleProgramsTestBase(mode){

  private var resultPath: String = null
  private var expectedResult: String = null

  var tempFolder: TemporaryFolder = new TemporaryFolder()

  @Rule
  def getFolder(): TemporaryFolder = {
    tempFolder;
  }

  @Before
  @throws(classOf[Exception])
  def before {
    resultPath = tempFolder.newFile.toURI.toString
  }

  @Test
  @throws(classOf[Exception])
  def testZipWithIndex {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val input: DataSet[String] = env.fromElements("A", "B", "C", "D", "E", "F")
    val result: DataSet[(Long, String)] = input.zipWithIndex

    result.writeAsCsv(resultPath, "\n", ",")
    env.execute()

    expectedResult = "0,A\n" + "1,B\n" + "2,C\n" + "3,D\n" + "4,E\n" + "5,F"
  }

  @After
  @throws(classOf[Exception])
  def after {
    TestBaseUtils.compareResultsByLinesInMemory(expectedResult, resultPath)
  }
}
