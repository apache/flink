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
class DistinctITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

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
  def testCorrectnessOfDistinctOnTuplesWithKeyFieldSelector(): Unit = {
    /*
     * Check correctness of distinct on tuples with key field selector
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getSmall3TupleDataSet(env)

    val distinctDs = ds.union(ds).distinct(0, 1, 2)
    distinctDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)

    env.execute()

    expected = "1,1,Hi\n" +
      "2,2,Hello\n" +
      "3,2,Hello world\n"
  }

  @Test
  def testCorrectnessOfDistinctOnTuplesWithKeyFieldSelectorNotAllFieldsSelected(): Unit = {
    /*
     * check correctness of distinct on tuples with key field selector with not all fields
     * selected
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getSmall5TupleDataSet(env)

    val distinctDs = ds.union(ds).distinct(0).map(_._1)

    distinctDs.writeAsText(resultPath, writeMode =  WriteMode.OVERWRITE)
    env.execute()
    expected = "1\n" + "2\n"
  }

  @Test
  def testCorrectnessOfDistinctOnTuplesWithKeyExtractor(): Unit ={
    /*
     * check correctness of distinct on tuples with key extractor
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getSmall5TupleDataSet(env)

    val reduceDs = ds.union(ds).distinct(_._1).map(_._1)

    reduceDs.writeAsText(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1\n" + "2\n"
  }

  @Test
  def testCorrectnessOfDistinctOnCustomTypeWithTypeExtractor(): Unit = {
    /*
     * check correctness of distinct on custom type with type extractor
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getCustomTypeDataSet(env)

    val reduceDs = ds.distinct(_.myInt).map( t => new Tuple1(t.myInt))

    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n"
  }

  @Test
  def testCorrectnessOfDistinctOnTuples(): Unit = {
    /*
     * check correctness of distinct on tuples
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getSmall3TupleDataSet(env)

    val distinctDs = ds.union(ds).distinct()

    distinctDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n"
  }

  @Test
  def testCorrectnessOfDistinctOnCustomTypeWithTupleReturningTypeExtractor(): Unit = {
    /*
     * check correctness of distinct on custom type with tuple-returning type extractor
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get5TupleDataSet(env)

    val reduceDs = ds.distinct( t => (t._1, t._5)).map( t => (t._1, t._5) )

    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1\n" + "2,1\n" + "2,2\n" + "3,2\n" + "3,3\n" + "4,1\n" + "4,2\n" + "5," +
      "1\n" + "5,2\n" + "5,3\n"
  }

  @Test
  def testCorrectnessOfDistinctOnTuplesWithFieldExpressions(): Unit = {
    /*
     * check correctness of distinct on tuples with field expressions
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getSmall5TupleDataSet(env)

    val reduceDs = ds.union(ds).distinct("_1").map(t => new Tuple1(t._1))

    reduceDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1\n" + "2\n"
  }

  @Test
  def testCorrectnessOfDistinctOnPojos(): Unit = {
    /*
     * check correctness of distinct on Pojos
     */

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getDuplicatePojoDataSet(env)

    val reduceDs = ds.distinct("nestedPojo.longNumber").map(_.nestedPojo.longNumber.toInt)

    reduceDs.writeAsText(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "10000\n20000\n30000\n"
  }

  @Test
  def testCorrectnessOfDistinctOnAtomic(): Unit = {
    /*
     * check correctness of distinct on Integers
     */

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getIntDataSet(env)

    val reduceDs = ds.distinct

    reduceDs.writeAsText(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "1\n2\n3\n4\n5"
  }

  @Test
  def testCorrectnessOfDistinctOnAtomicWithSelectAllChar(): Unit = {
    /*
     * check correctness of distinct on Strings, using Keys.ExpressionKeys.SELECT_ALL_CHAR
     */

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getStringDataSet(env)
    val reduceDs = ds.union(ds).distinct("_")

    reduceDs.writeAsText(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expected = "I am fine.\n" +
      "Luke Skywalker\n" +
      "LOL\n" +
      "Hello world, how are you?\n" +
      "Hi\n" +
      "Hello world\n" +
      "Hello\n" +
      "Random comment\n"
  }

}


