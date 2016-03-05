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

import org.apache.flink.api.common.functions.RichJoinFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
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
class AccetPFJoinITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
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
  def testUDFJoinOnTuplesWithKeyFieldPositions(): Unit = {
    /*
     * UDF Join on tuples with key field positions
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val joinDs = ds1.join(ds2).where(1).equalTo(1).projecting {
      case ((_, _, l), (_, _, _, r, _)) => l -> r
    }
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n"
  }

  @Test
  def testUDFJoinOnTuplesWithMultipleKeyFieldPositions(): Unit = {
    /*
     * UDF Join on tuples with multiple key field positions
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.get3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val joinDs = ds1.join(ds2).where(0, 1).equalTo(0, 4).projecting {
      case ((_, _, l), (_, _, _, r, _)) => l -> r
    }
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt wie gehts?\n" +
      "Hello world,ABC\n" + "I am fine.,HIJ\n" + "I am fine.,IJK\n"
  }

  @Test
  def testJoinWithHuge(): Unit = {
    /*
     * Join with Huge
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val joinDs = ds1.joinWithHuge(ds2).where(1).equalTo(1).projecting {
      case ((_, _, l), (_, _, _, r, _)) => l -> r
    }
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n"
  }

  @Test
  def testJoinWithTiny(): Unit = {
    /*
     * Join with Tiny
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val joinDs = ds1.joinWithTiny(ds2).where(1).equalTo(1).projecting {
      case ((_, _, l), (_, _, _, r, _)) => l -> r
    }
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n"
  }

  @Test
  def testJoinThatReturnsTheLeftInputObject(): Unit = {
    /*
     * Join that returns the left input object
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val joinDs = ds1.join(ds2).where(1).equalTo(1).projecting { case (l @ (_, _, _), _) => l }
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n"
  }

  @Test
  def testJoinThatReturnsTheRightInputObject(): Unit = {
    /*
     * Join that returns the right input object
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val joinDs = ds1.join(ds2).where(1).equalTo(1).projecting { case (_, r @ (_, _, _)) => r }
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,0,Hallo,1\n" + "2,2,1,Hallo Welt,2\n" + "2,2,1,Hallo Welt,2\n"
  }

  @Test
  def testUDFJoinOnTuplesWithTupleReturningKeySelectors(): Unit = {
    /*
     * UDF Join on tuples with tuple-returning key selectors
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.get3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val joinDs = ds1.join(ds2).where( t => (t._1, t._2)).equalTo( t => (t._1, t._5)).projecting {
      case ((_, _, l), (_, _, _, r, _)) => l -> r
    }
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt wie gehts?\n" +
      "Hello world,ABC\n" + "I am fine.,HIJ\n" + "I am fine.,IJK\n"
  }

}
