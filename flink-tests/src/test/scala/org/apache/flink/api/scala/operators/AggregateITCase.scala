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
package org.apache.flink.api.scala.operators

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{TestBaseUtils, MultipleProgramsTestBase}
import org.junit.{Test, After, Before, Rule}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import org.apache.flink.api.scala._

@RunWith(classOf[Parameterized])
class AggregateITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  val _tempFolder = new TemporaryFolder()

  private var resultPath: String = null
  private var expectedResult: String = null

  @Rule
  def tempFolder = _tempFolder

  @Before
  def before(): Unit = {
    resultPath = tempFolder.newFile().toURI.toString
  }

  @After
  def after(): Unit = {
    TestBaseUtils.compareResultsByLinesInMemory(expectedResult, resultPath)
  }

  @Test
  def testFullAggregate(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val ds = CollectionDataSets.get3TupleDataSet(env)

    val aggregateDs = ds
      .aggregate(Aggregations.SUM,0)
      .and(Aggregations.MAX, 1)
      // Ensure aggregate operator correctly copies other fields
      .filter(_._3 != null)
      .map{ t => (t._1, t._2) }

    aggregateDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)

    env.execute()

    // return expected result
    expectedResult = "231,6\n"
  }

  @Test
  def testGroupedAggregate(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)

    val aggregateDs = ds
      .groupBy(1)
      .aggregate(Aggregations.SUM, 0)
      // Ensure aggregate operator correctly copies other fields
      .filter(_._3 != null)
      .map { t => (t._2, t._1) }

    aggregateDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)

    env.execute()

    // return expected result
    expectedResult = "1,1\n" + "2,5\n" + "3,15\n" + "4,34\n" + "5,65\n" + "6,111\n"
  }

  @Test
  def testNestedAggregate(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)

    val aggregateDs = ds
      .groupBy(1)
      .aggregate(Aggregations.MIN, 0)
      .aggregate(Aggregations.MIN, 0)
      // Ensure aggregate operator correctly copies other fields
      .filter(_._3 != null)
      .map { t => new Tuple1(t._1) }

    aggregateDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)

    env.execute()

    // return expected result
    expectedResult = "1\n"
  }
}

