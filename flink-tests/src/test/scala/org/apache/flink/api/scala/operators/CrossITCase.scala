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

import org.apache.flink.api.common.functions.{OpenContext, RichCrossFunction}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.util.CollectionDataSets.CustomType
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode

import org.junit.{After, Before, Rule, Test}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class CrossITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
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
  def testCorrectnessOfCrossOnTwoTupleInputs(): Unit = {
    /*
     * check correctness of cross on two tuple inputs
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getSmall5TupleDataSet(env)
    val ds2 = CollectionDataSets.getSmall5TupleDataSet(env)
    val crossDs = ds.cross(ds2)((l, r) => (l._3 + r._3, l._4 + r._4))
    crossDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()

    expected = "0,HalloHallo\n" + "1,HalloHallo Welt\n" + "2,HalloHallo Welt wie\n" + "1," +
      "Hallo WeltHallo\n" + "2,Hallo WeltHallo Welt\n" + "3,Hallo WeltHallo Welt wie\n" + "2," +
      "Hallo Welt wieHallo\n" + "3,Hallo Welt wieHallo Welt\n" + "4," +
      "Hallo Welt wieHallo Welt wie\n"
  }

  @Test
  def testCorrectnessOfCrossIfUDFReturnsLeftInput(): Unit = {
    /*
     * check correctness of cross if UDF returns left input object
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.getSmall5TupleDataSet(env)
    val crossDs = ds.cross(ds2)((l, r) => l)
    crossDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()

    expected = "1,1,Hi\n" + "1,1,Hi\n" + "1,1,Hi\n" + "2,2,Hello\n" + "2,2,Hello\n" + "2,2," +
      "Hello\n" + "3,2,Hello world\n" + "3,2,Hello world\n" + "3,2,Hello world\n"
  }

  @Test
  def testCorrectnessOfCrossIfUDFReturnsRightInput(): Unit = {
    /*
     * check correctness of cross if UDF returns right input object
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.getSmall5TupleDataSet(env)
    val crossDs = ds.cross(ds2)((l, r) => r)
    crossDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()

    expected = "1,1,0,Hallo,1\n" + "1,1,0,Hallo,1\n" + "1,1,0,Hallo,1\n" + "2,2,1,Hallo Welt," +
      "2\n" + "2,2,1,Hallo Welt,2\n" + "2,2,1,Hallo Welt,2\n" + "2,3,2,Hallo Welt wie," +
      "1\n" + "2,3,2,Hallo Welt wie,1\n" + "2,3,2,Hallo Welt wie,1\n"
  }

  @Test
  def testCorrectnessOfCrossWithBroadcastSet(): Unit = {
    /*
     * check correctness of cross with broadcast set
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val intDs = CollectionDataSets.getIntDataSet(env)
    val ds = CollectionDataSets.getSmall5TupleDataSet(env)
    val ds2 = CollectionDataSets.getSmall5TupleDataSet(env)
    val crossDs = ds
      .cross(ds2)
      .apply(
        new RichCrossFunction[
          (Int, Long, Int, String, Long),
          (Int, Long, Int, String, Long),
          (Int, Int, Int)] {
          private var broadcast = 41

          override def open(openContext: OpenContext) {
            val ints = this.getRuntimeContext.getBroadcastVariable[Int]("ints").asScala
            broadcast = ints.sum
          }

          override def cross(
              first: (Int, Long, Int, String, Long),
              second: (Int, Long, Int, String, Long)): (Int, Int, Int) = {
            (first._1 + second._1, first._3.toInt * second._3.toInt, broadcast)
          }

        })
      .withBroadcastSet(intDs, "ints")
    crossDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "2,0,55\n" + "3,0,55\n" + "3,0,55\n" + "3,0,55\n" + "4,1,55\n" + "4,2,55\n" + "3," +
      "0,55\n" + "4,2,55\n" + "4,4,55\n"
  }

  @Test
  def testCorrectnessOfCrossWithHuge(): Unit = {
    /*
     * check correctness of crossWithHuge (only correctness of result -> should be the same
     * as with normal cross)
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getSmall5TupleDataSet(env)
    val ds2 = CollectionDataSets.getSmall5TupleDataSet(env)
    val crossDs = ds.crossWithHuge(ds2)((l, r) => (l._3 + r._3, l._4 + r._4))
    crossDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "0,HalloHallo\n" + "1,HalloHallo Welt\n" + "2,HalloHallo Welt wie\n" + "1," +
      "Hallo WeltHallo\n" + "2,Hallo WeltHallo Welt\n" + "3,Hallo WeltHallo Welt wie\n" + "2," +
      "Hallo Welt wieHallo\n" + "3,Hallo Welt wieHallo Welt\n" + "4," +
      "Hallo Welt wieHallo Welt wie\n"
  }

  @Test
  def testCorrectnessOfCrossWithTiny(): Unit = {
    /*
     * check correctness of crossWithTiny (only correctness of result -> should be the same
     * as with normal cross)
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets
      .getSmall5TupleDataSet(env)
    val ds2 = CollectionDataSets
      .getSmall5TupleDataSet(env)
    val crossDs = ds.crossWithTiny(ds2)((l, r) => (l._3 + r._3, l._4 + r._4))
    crossDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "0,HalloHallo\n" + "1,HalloHallo Welt\n" + "2,HalloHallo Welt wie\n" + "1," +
      "Hallo WeltHallo\n" + "2,Hallo WeltHallo Welt\n" + "3,Hallo WeltHallo Welt wie\n" + "2," +
      "Hallo Welt wieHallo\n" + "3,Hallo Welt wieHallo Welt\n" + "4," +
      "Hallo Welt wieHallo Welt wie\n"
  }

  @Test
  def testCorrectnessOfDefaultCross(): Unit = {
    /*
     * check correctness of default cross
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.getSmall5TupleDataSet(env)
    val crossDs = ds.cross(ds2)
    crossDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "(1,1,Hi),(2,2,1,Hallo Welt,2)\n" + "(1,1,Hi),(1,1,0,Hallo,1)\n" + "(1,1,Hi),(2,3," +
      "2,Hallo Welt wie,1)\n" + "(2,2,Hello),(2,2,1,Hallo Welt,2)\n" + "(2,2,Hello),(1,1,0," +
      "Hallo,1)\n" + "(2,2,Hello),(2,3,2,Hallo Welt wie,1)\n" + "(3,2,Hello world),(2,2,1," +
      "Hallo Welt,2)\n" + "(3,2,Hello world),(1,1,0,Hallo,1)\n" + "(3,2,Hello world),(2,3,2," +
      "Hallo Welt wie,1)\n"
  }

  @Test
  def testCorrectnessOfCrossOnTwoCutomTypeInputs(): Unit = {
    /*
     * check correctness of cross on two custom type inputs
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getSmallCustomTypeDataSet(env)
    val ds2 = CollectionDataSets.getSmallCustomTypeDataSet(env)
    val crossDs = ds.cross(ds2) {
      (l, r) => new CustomType(l.myInt * r.myInt, l.myLong + r.myLong, l.myString + r.myString)
    }
    crossDs.writeAsText(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,0,HiHi\n" + "2,1,HiHello\n" + "2,2,HiHello world\n" + "2,1,HelloHi\n" + "4,2," +
      "HelloHello\n" + "4,3,HelloHello world\n" + "2,2,Hello worldHi\n" + "4,3," +
      "Hello worldHello\n" + "4,4,Hello worldHello world"
  }

  @Test
  def testCorrectnessOfcrossTupleInputAndCustomTypeInput(): Unit = {
    /*
     * check correctness of cross a tuple input and a custom type input
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getSmall5TupleDataSet(env)
    val ds2 = CollectionDataSets.getSmallCustomTypeDataSet(env)
    val crossDs = ds.cross(ds2)((l, r) => (l._1 + r.myInt, l._3 * r.myLong, l._4 + r.myString))
    crossDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "2,0,HalloHi\n" + "3,0,HalloHello\n" + "3,0,HalloHello world\n" + "3,0," +
      "Hallo WeltHi\n" + "4,1,Hallo WeltHello\n" + "4,2,Hallo WeltHello world\n" + "3,0," +
      "Hallo Welt wieHi\n" + "4,2,Hallo Welt wieHello\n" + "4,4,Hallo Welt wieHello world\n"
  }

}
