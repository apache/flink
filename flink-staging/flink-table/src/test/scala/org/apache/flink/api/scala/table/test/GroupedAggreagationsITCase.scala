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
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.MultipleProgramsTestBase
import org.apache.flink.test.util.AbstractMultipleProgramsTestBase.TestExecutionMode
import org.junit._
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class GroupedAggreagationsITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
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

  @Test(expected = classOf[ExpressionException])
  def testGroupingOnNonExistentField: Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
      .groupBy('_foo)
      .select('a.avg)

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = ""
  }

  @Test
  def testGroupedAggregate: Unit = {

    // the grouping key needs to be forwarded to the intermediate DataSet, even
    // if we don't want the key in the output

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
      .groupBy('b)
      .select('b, 'a.sum)

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1\n" + "2,5\n" + "3,15\n" + "4,34\n" + "5,65\n" + "6,111\n"
  }

  @Test
  def testGroupingKeyForwardIfNotUsed: Unit = {

    // the grouping key needs to be forwarded to the intermediate DataSet, even
    // if we don't want the key in the output

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
      .groupBy('b)
      .select('a.sum)

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "1\n" + "5\n" + "15\n" + "34\n" + "65\n" + "111\n"
  }
}
