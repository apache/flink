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

import org.apache.flink.api.common.operators.Order
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
class FirstNITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
  private var resultPath: String = null
  private var expected: String = null
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
  def testFirstNOnUngroupedDataSet(): Unit = {
    /*
     * First-n on ungrouped data set
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val seven = ds.first(7).map( t => new Tuple1(1) ).sum(0)
    seven.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "(7)\n"
  }

  @Test
  def testFirstNOnGroupedDataSet(): Unit = {
    /*
     * First-n on grouped data set
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val first = ds.groupBy(1).first(4).map( t => (t._2, 1)).groupBy(0).sum(1)
    first.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "(1,1)\n(2,2)\n(3,3)\n(4,4)\n(5,4)\n(6,4)\n"
  }

  @Test
  def testFirstNOnGroupedAndSortedDataSet(): Unit = {
    /*
     * First-n on grouped and sorted data set
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val first = ds.groupBy(1)
      .sortGroup(0, Order.DESCENDING)
      .first(3)
      .map ( t => (t._2, t._1))
    first.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "(1,1)\n" + "(2,3)\n(2,2)\n" + "(3,6)\n(3,5)\n(3,4)\n" + "(4,10)\n(4,9)\n(4," +
      "8)\n" + "(5,15)\n(5,14)\n(5,13)\n" + "(6,21)\n(6,20)\n(6,19)\n"
  }
}
