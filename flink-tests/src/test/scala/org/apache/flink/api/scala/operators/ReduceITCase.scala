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

import org.apache.flink.api.common.functions.RichReduceFunction
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.util.CollectionDataSets.MutableTuple3
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{TestBaseUtils, MultipleProgramsTestBase}
import org.junit.{Test, After, Before, Rule}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

import org.apache.flink.api.scala._

@RunWith(classOf[Parameterized])
class ReduceITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
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
      .reduce { (in1, in2) => (in1._1 + in2._1, in1._2, "B-)") }
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
      .reduce { (in1, in2) => (in1._1, in1._2 + in2._2, 0, "P-)", in1._5) }
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
      .reduce { (in1, in2) => (in1._1 + in2._1, in1._2, "B-)") }
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,Hi\n" + "5,2,B-)\n" + "15,3,B-)\n" + "34,4,B-)\n" + "65,5,B-)\n" + "111,6,B-)\n"
  }

  @Test
  def testReduceOnCustomTypeWithKeyExtractor(): Unit = {
    /*
     * Reduce on custom type with key extractor
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getCustomTypeDataSet(env)
    val reduceDs = ds.groupBy(_.myInt)
      .reduce { (in1, in2) =>
      in1.myLong += in2.myLong
      in1.myString = "Hello!"
      in1
    }
    reduceDs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "1,0,Hi\n" + "2,3,Hello!\n" + "3,12,Hello!\n" + "4,30,Hello!\n" + "5,60," +
      "Hello!\n" + "6,105,Hello!\n"
  }

  @Test
  def testAllReduceForTuple(): Unit = {
    /*
     * All-reduce for tuple
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val reduceDs =
      ds.reduce { (in1, in2) => (in1._1 + in2._1, in1._2 + in2._2, "Hello World") }
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "231,91,Hello World\n"
  }

  @Test
  def testAllReduceForCustomTypes(): Unit = {
    /*
     * All-reduce for custom types
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getCustomTypeDataSet(env)
    val reduceDs = ds
      .reduce { (in1, in2) =>
      in1.myInt += in2.myInt
      in1.myLong += in2.myLong
      in1.myString = "Hello!"
      in1
    }
    reduceDs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "91,210,Hello!"
  }

  @Test
  def testReduceWithBroadcastSet(): Unit = {
    /*
     * Reduce with broadcast set
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val intDs = CollectionDataSets.getIntDataSet(env)
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val reduceDs = ds.groupBy(1).reduce(
      new RichReduceFunction[(Int, Long, String)] {
        private var f2Replace = ""

        override def open(config: Configuration) {
          val ints = this.getRuntimeContext.getBroadcastVariable[Int]("ints").asScala
          f2Replace = ints.sum + ""
        }

        override def reduce(
                             in1: (Int, Long, String),
                             in2: (Int, Long, String)): (Int, Long, String) = {
          (in1._1 + in2._1, in1._2, f2Replace)
        }
      }).withBroadcastSet(intDs, "ints")
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,Hi\n" + "5,2,55\n" + "15,3,55\n" + "34,4,55\n" + "65,5,55\n" + "111,6,55\n"
  }

  @Test
  def testReduceWithUDFThatReturnsTheSecondInputObject(): Unit = {
    /*
     * Reduce with UDF that returns the second input object (check mutable object handling)
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env).map (t => MutableTuple3(t._1, t._2, t._3))
    val reduceDs = ds.groupBy(1).reduce(
      new RichReduceFunction[MutableTuple3[Int, Long, String]] {
        override def reduce(
                             in1: MutableTuple3[Int, Long, String],
                             in2: MutableTuple3[Int, Long, String]): MutableTuple3[Int, Long,
          String] = {
          in2._1 = in1._1 + in2._1
          in2._3 = "Hi again!"
          in2
        }
      })
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,Hi\n" + "5,2,Hi again!\n" + "15,3,Hi again!\n" + "34,4,Hi again!\n" + "65,5," +
      "Hi again!\n" + "111,6,Hi again!\n"
  }

  @Test
  def testReduceWithATupleReturningKeySelector(): Unit = {
    /*
     * Reduce with a Tuple-returning KeySelector
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get5TupleDataSet(env)
    val reduceDs = ds.groupBy(t => (t._1, t._5))
      .reduce { (in1, in2) => (in1._1, in1._2 + in2._2, 0, "P-)", in1._5) }
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
      .reduce { (in1, in2) => (in1._1, in1._2 + in2._2, 0, "P-)", in1._5) }
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,0,Hallo,1\n" + "2,3,2,Hallo Welt wie,1\n" + "2,2,1,Hallo Welt,2\n" + "3,9,0," +
      "P-),2\n" + "3,6,5,BCD,3\n" + "4,17,0,P-),1\n" + "4,17,0,P-),2\n" + "5,11,10,GHI," +
      "1\n" + "5,29,0,P-),2\n" + "5,25,0,P-),3\n"
  }

  @Test
  def testReduceOnGroupedDSByExpressionKeyWithHashHint(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get5TupleDataSet(env)
    val reduceDs = ds.groupBy("_5", "_1")
      .reduce((in1, in2) => (in1._1, in1._2 + in2._2, 0, "P-)", in1._5), CombineHint.HASH)
    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,0,Hallo,1\n" + "2,3,2,Hallo Welt wie,1\n" + "2,2,1,Hallo Welt,2\n" + "3,9,0," +
      "P-),2\n" + "3,6,5,BCD,3\n" + "4,17,0,P-),1\n" + "4,17,0,P-),2\n" + "5,11,10,GHI," +
      "1\n" + "5,29,0,P-),2\n" + "5,25,0,P-),3\n"
  }

}
