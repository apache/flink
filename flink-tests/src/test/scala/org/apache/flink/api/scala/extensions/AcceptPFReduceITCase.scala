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
package org.apache.flink.api.scala.extensions

import org.apache.flink.api.common.functions.RichReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.util.CollectionDataSets.MutableTuple3
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Rule, Test}

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class AcceptPFReduceITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
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
  def testReduceOnTuplesWithKeyFieldSelector(): Unit = {
    /*
     * Reduce on tuples with key field selector
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val reduceDs = ds.groupBy(1)
      .reduceWith { case ((i1, l1, _), (i2, _, _)) => (i1 + i2, l1, "B-)") }
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,Hi\n" + "5,2,B-)\n" + "15,3,B-)\n" + "34,4,B-)\n" + "65,5,B-)\n" + "111,6,B-)\n"
  }

  @Test
  def testReduceOnTuplesWithMultipleKeyFieldSelectors(): Unit = {
    /*
     * Reduce on tuples with multiple key field selectors
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get5TupleDataSet(env)
    val reduceDs = ds.groupBy(4, 0)
      .reduceWith {
        case ((in11, in12, _, _, in15), (_, in22, _, _, _)) => (in11, in12 + in22, 0, "P-)", in15) }
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,0,Hallo,1\n" + "2,3,2,Hallo Welt wie,1\n" + "2,2,1,Hallo Welt,2\n" + "3,9,0," +
      "P-),2\n" + "3,6,5,BCD,3\n" + "4,17,0,P-),1\n" + "4,17,0,P-),2\n" + "5,11,10,GHI," +
      "1\n" + "5,29,0,P-),2\n" + "5,25,0,P-),3\n"
  }

  @Test
  def testReduceOnTuplesWithKeyExtractor(): Unit = {
    /*
     * Reduce on tuples with key extractor
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val reduceDs = ds.groupBy(_._2)
      .reduceWith { case ((i1, l1, _), (i2, _, _)) => (i1 + i2, l1, "B-)") }
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,Hi\n" + "5,2,B-)\n" + "15,3,B-)\n" + "34,4,B-)\n" + "65,5,B-)\n" + "111,6,B-)\n"
  }

  @Test
  def testAllReduceForTuple(): Unit = {
    /*
     * All-reduce for tuple
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val reduceDs =
      ds.reduceWith { case ((i1, l1, _), (i2, l2, _)) => (i1 + i2, l1 + l2, "Hello World") }
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "231,91,Hello World\n"
  }

  @Test
  def testReduceWithATupleReturningKeySelector(): Unit = {
    /*
     * Reduce with a Tuple-returning KeySelector
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get5TupleDataSet(env)
    val reduceDs = ds.groupBy(t => (t._1, t._5))
      .reduceWith { case ((in11, in12, _, _, in15), (_, in22, _, _, _)) =>
        (in11, in12 + in22, 0, "P-)", in15) }
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,0,Hallo,1\n" + "2,3,2,Hallo Welt wie,1\n" + "2,2,1,Hallo Welt,2\n" + "3,9,0," +
      "P-),2\n" + "3,6,5,BCD,3\n" + "4,17,0,P-),1\n" + "4,17,0,P-),2\n" + "5,11,10,GHI," +
      "1\n" + "5,29,0,P-),2\n" + "5,25,0,P-),3\n"
  }

  @Test
  def testReduceOnGroupedDSByExpressionKey(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get5TupleDataSet(env)
    val reduceDs = ds.groupBy("_5", "_1")
      .reduceWith { case ((in11, in12, _, _, in15), (_, in22, _, _, _)) =>
        (in11, in12 + in22, 0, "P-)", in15) }
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,0,Hallo,1\n" + "2,3,2,Hallo Welt wie,1\n" + "2,2,1,Hallo Welt,2\n" + "3,9,0," +
      "P-),2\n" + "3,6,5,BCD,3\n" + "4,17,0,P-),1\n" + "4,17,0,P-),2\n" + "5,11,10,GHI," +
      "1\n" + "5,29,0,P-),2\n" + "5,25,0,P-),3\n"
  }

}
