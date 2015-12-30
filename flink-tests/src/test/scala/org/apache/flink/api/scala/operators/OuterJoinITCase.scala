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

import org.apache.flink.api.common.functions.RichJoinFunction
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.scala.operators.ScalaCsvOutputFormat.{DEFAULT_FIELD_DELIMITER, DEFAULT_LINE_DELIMITER}
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.util.CollectionDataSets.CustomType
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Rule, Test}

import scala.collection.JavaConverters._


@RunWith(classOf[Parameterized])
class OuterJoinITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
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
    if (expected != null) {
      TestBaseUtils.compareResultsByLinesInMemory(expected, resultPath)
    }
  }

  def writeAsCsv[T](ds: DataSet[T]) = {
    val of = new ScalaCsvOutputFormat[Product](new Path(resultPath),
      DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER)
    of.setAllowNullValues(true)
    of.setWriteMode(WriteMode.OVERWRITE)
    ds.output(of.asInstanceOf[OutputFormat[T]])
  }

  def mapToString: ((String, String)) => (String, String) = {
    (tuple: (String, String)) => (String.valueOf(tuple._1), String.valueOf(tuple._2))
  }

  @Test
  @throws(classOf[Exception])
  def testUDFLeftOuterJoinOnTuplesWithKeyFieldPositions {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.getSmall5TupleDataSet(env)
    val joinDs = ds1.leftOuterJoin(ds2).where(0).equalTo(0).apply(T3T5FlatJoin)
    writeAsCsv(joinDs.map(mapToString))
    env.execute()
    expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello,Hallo Welt wie\n" + "Hello world,null\n"
  }

  @Test
  @throws(classOf[Exception])
  def testUDFRightOuterJoinOnTuplesWithKeyFieldPositions {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.getSmall5TupleDataSet(env)
    val joinDs = ds1.rightOuterJoin(ds2).where(1).equalTo(1).apply(T3T5FlatJoin)
    writeAsCsv(joinDs.map(mapToString))
    env.execute()
    expected = "Hi,Hallo\n" +
      "Hello,Hallo Welt\n" +
      "null,Hallo Welt wie\n" +
      "Hello world,Hallo Welt\n"
  }

  @Test
  @throws(classOf[Exception])
  def testUDFFullOuterJoinOnTuplesWithKeyFieldPositions {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.getSmall5TupleDataSet(env)
    val joinDs = ds1.fullOuterJoin(ds2).where(0).equalTo(2).apply(T3T5FlatJoin)
    writeAsCsv(joinDs.map(mapToString))
    env.execute()
    expected = "null,Hallo\n" + "Hi,Hallo Welt\n" + "Hello,Hallo Welt wie\n" + "Hello world,null\n"
  }

  @Test
  @throws(classOf[Exception])
  def testUDFJoinOnTuplesWithMultipleKeyFieldPositions {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.getSmall5TupleDataSet(env)
    val joinDs = ds1.fullOuterJoin(ds2).where(0, 1).equalTo(0, 4).apply(T3T5FlatJoin)
    writeAsCsv(joinDs.map(mapToString))
    env.execute()
    expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,null\n" + "null,Hallo Welt wie\n"
  }

  @Test
  @throws(classOf[Exception])
  def testJoinWithBroadcastSet {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val intDs = CollectionDataSets.getIntDataSet(env)
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.getSmall5TupleDataSet(env)
    val joinDs = ds1.fullOuterJoin(ds2).where(1).equalTo(4).apply(
      new RichJoinFunction[
        (Int, Long, String),
        (Int, Long, Int, String, Long),
        (String, String, Int)] {
        private var broadcast = 41

        override def open(config: Configuration) {
          val ints = this.getRuntimeContext.getBroadcastVariable[Int]("ints").asScala
          broadcast = ints.sum
        }

        override def join(l: (Int, Long, String),
                          r: (Int, Long, Int, String, Long)): (String, String, Int) = {
          (if (l == null) "null" else l._3, if (r == null) "null" else r._4, broadcast)
        }
      }
    ).withBroadcastSet(intDs, "ints")
    writeAsCsv(joinDs)
    env.execute()
    expected = "Hi,Hallo,55\n" +
      "Hi,Hallo Welt wie,55\n" +
      "Hello,Hallo Welt,55\n" +
      "Hello world,Hallo Welt,55\n"
  }

  @Test
  @throws(classOf[Exception])
  def testJoinOnACustomTypeInputWithKeyExtractorAndATupleInputWithKeyFieldSelector {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmallCustomTypeDataSet(env)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env)
    val joinDs = ds1.fullOuterJoin(ds2).where(t => t.myInt).equalTo(0).apply(CustT3Join)
    writeAsCsv(joinDs.map(mapToString))
    env.execute()
    expected = "Hi,Hi\n" + "Hello,Hello\n" + "Hello world,Hello\n" + "null,Hello world\n"
  }

  @Test
  @throws(classOf[Exception])
  def testJoinOnATupleInputWithKeyFieldSelectorAndACustomTypeInputWithKeyExtractor {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.getSmallCustomTypeDataSet(env)
    val joinDs = ds1.fullOuterJoin(ds2).where(1).equalTo(t => t.myLong).apply(T3CustJoin)
    writeAsCsv(joinDs.map(mapToString))
    env.execute()
    expected = "null,Hi\n" + "Hi,Hello\n" + "Hello,Hello world\n" + "Hello world,Hello world\n"
  }

  @Test
  @throws(classOf[Exception])
  def testUDFJoinOnTuplesWithTupleReturningKeySelectors {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.getSmall5TupleDataSet(env)
    val joinDs = ds1.fullOuterJoin(ds2)
      .where(t => (t._1, t._2)).equalTo(t => (t._1, t._5))
      .apply(T3T5FlatJoin)
    writeAsCsv(joinDs.map(mapToString))
    env.execute()
    expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,null\n" + "null,Hallo Welt wie\n"
  }


  def T3T5FlatJoin: ((Int, Long, String), (Int, Long, Int, String, Long)) => (String, String) = {
    (first, second) => {
      (if (first == null) null else first._3, if (second == null) null else second._4)
    }
  }

  def CustT3Join: (CustomType, (Int, Long, String)) => (String, String) = {
    (first, second) => {
      (if (first == null) null else first.myString, if (second == null) null else second._3)
    }
  }

  def T3CustJoin: ((Int, Long, String), CustomType) => (String, String) = {
    (first, second) => {
      (if (first == null) null else first._3, if (second == null) null else second.myString)
    }
  }

}
