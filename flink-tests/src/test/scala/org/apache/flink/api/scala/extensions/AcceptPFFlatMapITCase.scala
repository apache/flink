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

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit._
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class AcceptPFFlatMapITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
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
  def testNonPassingFlatMap(): Unit = {
    /*
     * Test non-passing flatmap
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getStringDataSet(env)
    val nonPassingFlatMapDs = ds.flatMapWith( in => if (in.contains("banana")) Some(in) else None )
    nonPassingFlatMapDs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "\n"
  }

  @Test
  def testDataDuplicatingFlatMap(): Unit = {
    /*
     * Test data duplicating flatmap
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getStringDataSet(env)
    val duplicatingFlatMapDs = ds.flatMapWith( in => Seq(in, in.toUpperCase) )
    duplicatingFlatMapDs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "Hi\n" + "HI\n" + "Hello\n" + "HELLO\n" + "Hello world\n" + "HELLO WORLD\n" +
      "Hello world, how are you?\n" + "HELLO WORLD, HOW ARE YOU?\n" + "I am fine.\n" + "I AM " +
      "FINE.\n" + "Luke Skywalker\n" + "LUKE SKYWALKER\n" + "Random comment\n" + "RANDOM " +
      "COMMENT\n" + "LOL\n" + "LOL\n"
  }

  @Test
  def testFlatMapWithVaryingNumberOfEmittedTuples(): Unit = {
    /*
     * Test flatmap with varying number of emitted tuples
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val varyingTuplesMapDs = ds.flatMapWith {
      case in @ (i, l, s) =>
        val numTuples = i % 3
        (0 until numTuples) map { i => in }
    }
    varyingTuplesMapDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,Hi\n" + "2,2,Hello\n" + "2,2,Hello\n" + "4,3,Hello world, " +
      "how are you?\n" + "5,3,I am fine.\n" + "5,3,I am fine.\n" + "7,4,Comment#1\n" + "8,4," +
      "Comment#2\n" + "8,4,Comment#2\n" + "10,4,Comment#4\n" + "11,5,Comment#5\n" + "11,5," +
      "Comment#5\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" + "14,5,Comment#8\n" + "16,6," +
      "Comment#10\n" + "17,6,Comment#11\n" + "17,6,Comment#11\n" + "19,6,Comment#13\n" + "20," +
      "6,Comment#14\n" + "20,6,Comment#14\n"
  }

  @Test
  def testTypeConversionFlatMapperCustomToTuple(): Unit = {
    /*
     * Test type conversion flatmapper (Custom -> Tuple)
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getCustomTypeDataSet(env)
    val typeConversionFlatMapDs = ds.flatMapWith { in => Some((in.myInt, in.myLong, in.myString)) }
    typeConversionFlatMapDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,0,Hi\n" + "2,1,Hello\n" + "2,2,Hello world\n" + "3,3,Hello world, " +
      "how are you?\n" + "3,4,I am fine.\n" + "3,5,Luke Skywalker\n" + "4,6," +
      "Comment#1\n" + "4,7,Comment#2\n" + "4,8,Comment#3\n" + "4,9,Comment#4\n" + "5,10," +
      "Comment#5\n" + "5,11,Comment#6\n" + "5,12,Comment#7\n" + "5,13,Comment#8\n" + "5,14," +
      "Comment#9\n" + "6,15,Comment#10\n" + "6,16,Comment#11\n" + "6,17,Comment#12\n" + "6," +
      "18,Comment#13\n" + "6,19,Comment#14\n" + "6,20,Comment#15\n"
  }

  @Test
  def testTypeConversionFlatMapperTupleToBasic(): Unit = {
    /*
         * Test type conversion flatmapper (Tuple -> Basic)
         */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val typeConversionFlatMapDs = ds.flatMapWith { case (_, _, in) => Some(in) }
    typeConversionFlatMapDs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "Hi\n" + "Hello\n" + "Hello world\n" + "Hello world, how are you?\n" + "I am fine" +
      ".\n" + "Luke Skywalker\n" + "Comment#1\n" + "Comment#2\n" + "Comment#3\n" +
      "Comment#4\n" + "Comment#5\n" + "Comment#6\n" + "Comment#7\n" + "Comment#8\n" +
      "Comment#9\n" + "Comment#10\n" + "Comment#11\n" + "Comment#12\n" + "Comment#13\n" +
      "Comment#14\n" + "Comment#15\n"
  }

}
