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

import org.apache.flink.api.common.functions.RichCrossFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.util.CollectionDataSets.CustomType
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
class AcceptPFCrossITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
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
    val crossDs = ds.cross(ds2).projecting {
      case ((_, _, l3, l4), (_, _, r3, r4)) => (l3 + r3, l4 + r4)
    }
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
    val crossDs = ds.cross(ds2).projecting {
      case (l @ (_, _, _), _) => l
    }
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
    val crossDs = ds.cross(ds2).projecting {
      case (_, r @ (_, _, _)) => r
    }
    crossDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()

    expected = "1,1,0,Hallo,1\n" + "1,1,0,Hallo,1\n" + "1,1,0,Hallo,1\n" + "2,2,1,Hallo Welt," +
      "2\n" + "2,2,1,Hallo Welt,2\n" + "2,2,1,Hallo Welt,2\n" + "2,3,2,Hallo Welt wie," +
      "1\n" + "2,3,2,Hallo Welt wie,1\n" + "2,3,2,Hallo Welt wie,1\n"
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
    val crossDs = ds.crossWithHuge(ds2).projecting {
      case ((_, _, l3, l4), (_, _, r3, r4)) => (l3 + r3, l4 + r4)
    }
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
    val crossDs = ds.crossWithTiny(ds2).projecting {
      case ((_, _, l3, l4), (_, _, r3, r4)) => (l3 + r3, l4 + r4)
    }
    crossDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "0,HalloHallo\n" + "1,HalloHallo Welt\n" + "2,HalloHallo Welt wie\n" + "1," +
      "Hallo WeltHallo\n" + "2,Hallo WeltHallo Welt\n" + "3,Hallo WeltHallo Welt wie\n" + "2," +
      "Hallo Welt wieHallo\n" + "3,Hallo Welt wieHallo Welt\n" + "4," +
      "Hallo Welt wieHallo Welt wie\n"
  }

}
