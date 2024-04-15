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

import org.apache.flink.api.common.functions.{OpenContext, RichJoinFunction}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
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
class JoinITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
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
    val joinDs = ds1.join(ds2).where(1).equalTo(1)((l, r) => (l._3, r._4))
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
    val joinDs = ds1.join(ds2).where(0, 1).equalTo(0, 4)((l, r) => (l._3, r._4))
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt wie gehts?\n" +
      "Hello world,ABC\n" + "I am fine.,HIJ\n" + "I am fine.,IJK\n"
  }

  @Test
  def testDefaultJoinOnTuples(): Unit = {
    /*
     * Default Join on tuples
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val joinDs = ds1.join(ds2).where(0).equalTo(2)
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "(1,1,Hi),(2,2,1,Hallo Welt,2)\n" + "(2,2,Hello),(2,3,2,Hallo Welt wie," +
      "1)\n" + "(3,2,Hello world),(3,4,3,Hallo Welt wie gehts?,2)\n"
  }

  @Test
  def testJoinWithHuge(): Unit = {
    /*
     * Join with Huge
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val joinDs = ds1.joinWithHuge(ds2).where(1).equalTo(1)((l, r) => (l._3, r._4))
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
    val joinDs = ds1.joinWithTiny(ds2).where(1).equalTo(1)((l, r) => (l._3, r._4))
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
    val joinDs = ds1.join(ds2).where(1).equalTo(1)((l, r) => l)
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
    val joinDs = ds1.join(ds2).where(1).equalTo(1)((l, r) => r)
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,0,Hallo,1\n" + "2,2,1,Hallo Welt,2\n" + "2,2,1,Hallo Welt,2\n"
  }

  @Test
  def testJoinWithBroadcastSet(): Unit = {
    /*
     * Join with broadcast set
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val intDs = CollectionDataSets.getIntDataSet(env)
    val ds1 = CollectionDataSets.get3TupleDataSet(env)
    val ds2 = CollectionDataSets.getSmall5TupleDataSet(env)
    val joinDs = ds1
      .join(ds2)
      .where(1)
      .equalTo(4)
      .apply(
        new RichJoinFunction[
          (Int, Long, String),
          (Int, Long, Int, String, Long),
          (String, String, Int)] {
          private var broadcast = 41

          override def open(openContext: OpenContext) {
            val ints = this.getRuntimeContext.getBroadcastVariable[Int]("ints").asScala
            broadcast = ints.sum
          }

          override def join(
              first: (Int, Long, String),
              second: (Int, Long, Int, String, Long)): (String, String, Int) = {
            (first._3, second._4, broadcast)
          }
        }
      )
      .withBroadcastSet(intDs, "ints")
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "Hi,Hallo,55\n" + "Hi,Hallo Welt wie,55\n" + "Hello,Hallo Welt," +
      "55\n" + "Hello world,Hallo Welt,55\n"
  }

  @Test
  def testJoinOnCustomTypeInputWithKeyExtractorAndTupleInputWithKeyFieldSelector(): Unit = {
    /*
     * Join on a tuple input with key field selector and a custom type input with key extractor
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmallCustomTypeDataSet(env)
    val ds2 = CollectionDataSets.get3TupleDataSet(env)
    val joinDs = ds1.join(ds2).where(_.myInt).equalTo(0)((l, r) => (l.myString, r._3))
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "Hi,Hi\n" + "Hello,Hello\n" + "Hello world,Hello\n"
  }

  @Test
  def testJoinOnTupleInputWithKeyFieldSelectorAndCustomTypeInputWithKeyExtractor(): Unit = {
    /*
     * Join on a tuple input with key field selector and a custom type input with key extractor
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.getCustomTypeDataSet(env)
    val joinDs = ds1.join(ds2).where(1).equalTo(_.myLong).apply((l, r) => (l._3, r.myString))
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "Hi,Hello\n" + "Hello,Hello world\n" + "Hello world,Hello world\n"
  }

  @Test
  def testDefaultJoinOnTwoCustomTypeInputsWithKeyExtractors(): Unit = {
    /*
     * (Default) Join on two custom type inputs with key extractors
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getCustomTypeDataSet(env)
    val ds2 = CollectionDataSets.getSmallCustomTypeDataSet(env)
    val joinDs = ds1.join(ds2).where(_.myInt).equalTo(_.myInt)
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,0,Hi,1,0,Hi\n" + "2,1,Hello,2,1,Hello\n" + "2,1,Hello,2,2,Hello world\n" + "2," +
      "2,Hello world,2,1,Hello\n" + "2,2,Hello world,2,2,Hello world\n"
  }

  @Test
  def testUDFJoinOnTuplesWithTupleReturningKeySelectors(): Unit = {
    /*
     * UDF Join on tuples with tuple-returning key selectors
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.get3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val joinDs = ds1.join(ds2).where(t => (t._1, t._2)).equalTo(t => (t._1, t._5)).apply {
      (l, r) => (l._3, r._4)
    }
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt wie gehts?\n" +
      "Hello world,ABC\n" + "I am fine.,HIJ\n" + "I am fine.,IJK\n"
  }

  @Test
  def testNestedPojoAgainstTupleAsString(): Unit = {
    /*
     * Join nested pojo against tuple (selected using a string)
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmallPojoDataSet(env)
    val ds2 = CollectionDataSets.getSmallTuplebasedPojoMatchingDataSet(env)
    val joinDs = ds1.join(ds2).where("nestedPojo.longNumber").equalTo("_7")
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One," +
      "10000)\n" + "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two," +
      "20000)\n" + "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n"
  }

  @Test
  def testJoinNestedPojoAgainstTupleAsInteger(): Unit = {
    /*
     * Join nested pojo against tuple (selected as an integer)
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmallPojoDataSet(env)
    val ds2 = CollectionDataSets.getSmallTuplebasedPojoMatchingDataSet(env)
    val joinDs = ds1.join(ds2).where("nestedPojo.longNumber").equalTo(6) // <-- difference
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One," +
      "10000)\n" + "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two," +
      "20000)\n" + "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n"
  }

  @Test
  def testSelectingMultipleFieldsUsingExpressionLanguage(): Unit = {
    /*
     * selecting multiple fields using expression language
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmallPojoDataSet(env)
    val ds2 = CollectionDataSets.getSmallTuplebasedPojoMatchingDataSet(env)
    val joinDs = ds1
      .join(ds2)
      .where("nestedPojo.longNumber", "number", "str")
      .equalTo("_7", "_1", "_2")
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.setParallelism(1)
    env.execute()
    expected = "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One," +
      "10000)\n" + "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two," +
      "20000)\n" + "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n"
  }

  @Test
  def testNestedIntoTuple(): Unit = {
    /*
     * nested into tuple
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmallPojoDataSet(env)
    val ds2 = CollectionDataSets.getSmallTuplebasedPojoMatchingDataSet(env)
    val joinDs = ds1
      .join(ds2)
      .where("nestedPojo.longNumber", "number", "nestedTupleWithCustom._1")
      .equalTo("_7", "_1", "_3")
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.setParallelism(1)
    env.execute()
    expected = "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One," +
      "10000)\n" + "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two," +
      "20000)\n" + "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n"
  }

  @Test
  def testNestedIntoTupleIntoPojo(): Unit = {
    /*
     * nested into tuple into pojo
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmallPojoDataSet(env)
    val ds2 = CollectionDataSets.getSmallTuplebasedPojoMatchingDataSet(env)
    val joinDs = ds1
      .join(ds2)
      .where(
        "nestedTupleWithCustom._1",
        "nestedTupleWithCustom._2.myInt",
        "nestedTupleWithCustom._2.myLong")
      .equalTo("_3", "_4", "_5")
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.setParallelism(1)
    env.execute()
    expected = "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One," +
      "10000)\n" + "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two," +
      "20000)\n" + "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n"
  }

  @Test
  def testNonPojoFullTuple(): Unit = {
    /*
     * Non-POJO test to verify that full-tuple keys are working.
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmallNestedTupleDataSet(env)
    val ds2 = CollectionDataSets.getSmallNestedTupleDataSet(env)
    val joinDs = ds1.join(ds2).where(0).equalTo("_1._1", "_1._2")
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.setParallelism(1)
    env.execute()
    expected = "((1,1),one),((1,1),one)\n" + "((2,2),two),((2,2),two)\n" + "((3,3),three),((3,3)," +
      "three)\n"

  }

  @Test
  def testNonPojoNestedTuple(): Unit = {
    /*
     * Non-POJO test to verify "nested" tuple-element selection.
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmallNestedTupleDataSet(env)
    val ds2 = CollectionDataSets.getSmallNestedTupleDataSet(env)
    val joinDs = ds1.join(ds2).where("_1._1").equalTo("_1._1")
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.setParallelism(1)
    env.execute()
    expected = "((1,1),one),((1,1),one)\n" + "((2,2),two),((2,2),two)\n" + "((3,3),three),((3,3)," +
      "three)\n"
  }

  @Test
  def testFullPojoWithFullTuple(): Unit = {
    /*
     * full pojo with full tuple
     */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmallPojoDataSet(env)
    val ds2 = CollectionDataSets.getSmallTuplebasedDataSetMatchingPojo(env)
    val joinDs = ds1.join(ds2).where("*").equalTo("*")
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.setParallelism(1)
    env.execute()
    expected = "1 First (10,100,1000,One) 10000,(10000,10,100,1000,One,1,First)\n" +
      "2 Second (20,200,2000,Two) 20000,(20000,20,200,2000,Two,2,Second)\n" +
      "3 Third (30,300,3000,Three) 30000,(30000,30,300,3000,Three,3,Third)\n"
  }

  @Test
  def testWithAtomic1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = env.fromElements(0, 1, 2)
    val joinDs = ds1.join(ds2).where(0).equalTo("*")
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "(1,1,Hi),1\n(2,2,Hello),2"
  }

  @Test
  def testWithAtomic2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(0, 1, 2)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env)
    val joinDs = ds1.join(ds2).where("*").equalTo(0)
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,(1,1,Hi)\n2,(2,2,Hello)"
  }

  @Test
  def testWithScalaOptionValues(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(None, Some("a"), Some("b"))
    val ds2 = env.fromElements(None, Some("a"))
    val joinDs = ds1.join(ds2).where("_").equalTo("_")
    joinDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "None,None\nSome(a),Some(a)"
  }
}
