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

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.util.CollectionDataSets.CustomType
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.apache.flink.util.Collector
import org.junit._
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class AcceptPFCoGroupITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
  val _tempFolder = new TemporaryFolder()
  var resultPath: String = _
  var expectedResult: String = _

  @Rule
  def tempFolder = _tempFolder

  @Before
  def before(): Unit = {
    resultPath = tempFolder.newFile().toURI.toString
  }

  @After
  def after(): Unit = {
    TestBaseUtils.compareResultsByLinesInMemory(expectedResult, resultPath)
  }

  @Test
  def testCoGroupOnTuplesWithKeyFieldSelector(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get5TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val coGroupDs = ds.coGroup(ds2).where(0).equalTo(0).projecting {
      (first, second) =>
        var sum = 0
        var id = 0
        for (t <- first) {
          sum += t._3
          id = t._1
        }
        for (t <- second) {
          sum += t._3
          id = t._1
        }
        (id, sum)
    }
    coGroupDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expectedResult = "1,0\n" + "2,6\n" + "3,24\n" + "4,60\n" + "5,120\n"
  }

  @Test
  def testCoGroupOnTwoCustomInputsWithKeyExtractors(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getCustomTypeDataSet(env)
    val ds2 = CollectionDataSets.getCustomTypeDataSet(env)

    val coGroupDs = ds.coGroup(ds2).where(_.myInt).equalTo(_.myInt) projecting {
      (first, second) =>
        val o = new CustomType(0, 0, "test")
        for (c <- first) {
          o.myInt = c.myInt
          o.myLong += c.myLong
        }
        for (c <- second) {
          o.myInt = c.myInt
          o.myLong += c.myLong
        }
        o
    }
    coGroupDs.writeAsText(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expectedResult = "1,0,test\n" + "2,6,test\n" + "3,24,test\n" + "4,60,test\n" + "5,120,test\n" +
      "6," + "210,test\n"
  }

  @Test
  def testCorrectnessIfCoGroupReturnsRightInputObjects(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get5TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val coGroupDs = ds.coGroup(ds2).where(0).equalTo(0) {
      (first, second, out: Collector[(Int, Long, Int, String, Long)]) =>
        for (t <- second) {
          if (t._1 < 4) {
            out.collect(t)
          }
        }
    }
    coGroupDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expectedResult = "1,1,0,Hallo,1\n" + "2,2,1,Hallo Welt,2\n" + "2,3,2,Hallo Welt wie,1\n" +
      "3,4,3," + "Hallo Welt wie gehts?,2\n" + "3,5,4,ABC,2\n" + "3,6,5,BCD,3\n"
  }

  @Test
  def testCoGroupOnTupleWithKeyFieldSelectorAndCustomTypeWithKeyExtractor(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get5TupleDataSet(env)
    val ds2 = CollectionDataSets.getCustomTypeDataSet(env)
    val coGroupDs = ds.coGroup(ds2).where(2).equalTo(_.myInt) projecting {
      (first, second) =>
        var sum = 0L
        var id = 0
        for (t <- first) {
          sum += t._1
          id = t._3
        }
        for (t <- second) {
          sum += t.myLong
          id = t.myInt
        }
        (id, sum, "test")
    }
    coGroupDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expectedResult = "0,1,test\n" + "1,2,test\n" + "2,5,test\n" + "3,15,test\n" + "4,33,test\n" +
      "5," + "63,test\n" + "6,109,test\n" + "7,4,test\n" + "8,4,test\n" + "9,4,test\n" + "10,5," +
      "test\n" + "11,5,test\n" + "12,5,test\n" + "13,5,test\n" + "14,5,test\n"
  }

  @Test
  def testCoGroupOnCustomTypeWithKeyExtractorAndTupleInputKeyFieldSelector(): Unit = {
    /*
         * CoGroup on a tuple input with key field selector and a custom type input with
         * key extractor
         */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get5TupleDataSet(env)
    val ds2 = CollectionDataSets.getCustomTypeDataSet(env)
    val coGroupDs = ds2.coGroup(ds).where(_.myInt).equalTo(2).projecting {
      (first, second) =>
        var sum = 0L
        var id = 0
        for (t <- first) {
          sum += t.myLong
          id = t.myInt
        }
        for (t <- second) {
          sum += t._1
          id = t._3
        }

        new CustomType(id, sum, "test")
    }
    coGroupDs.writeAsText(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expectedResult = "0,1,test\n" + "1,2,test\n" + "2,5,test\n" + "3,15,test\n" + "4,33,test\n" +
      "5," + "63,test\n" + "6,109,test\n" + "7,4,test\n" + "8,4,test\n" + "9,4,test\n" + "10,5," +
      "test\n" + "11,5,test\n" + "12,5,test\n" + "13,5,test\n" + "14,5,test\n"
  }

  @Test
  def testCoGroupWithMultipleKeyExtractors(): Unit = {
    /*
        * CoGroup with multiple key extractors
        */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets
      .get5TupleDataSet(env)
    val ds2 = CollectionDataSets.get3TupleDataSet(env)
    val coGrouped = ds1.coGroup(ds2).where(t => (t._1, t._5)).equalTo(t => (t._1, t._2))
      .apply {
      (first, second, out: Collector[(Int, Long, String)]) =>
        val strs = first map(_._4)
        for (t <- second) {
          for (s <- strs) {
            out.collect((t._1, t._2, s))
          }
        }
    }

    coGrouped.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expectedResult = "1,1,Hallo\n" + "2,2,Hallo Welt\n" + "3,2,Hallo Welt wie gehts?\n" + "3,2," +
      "ABC\n" + "5,3,HIJ\n" + "5,3,IJK\n"

  }

  @Test
  def testCoGroupOnTwoCustomTypesUsingExpressionKeys(): Unit = {
    /*
     * CoGroup on two custom type inputs using expression keys
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getCustomTypeDataSet(env)
    val ds2 = CollectionDataSets.getCustomTypeDataSet(env)
    val coGroupDs = ds.coGroup(ds2).where("myInt").equalTo("myInt").projecting {
      (first, second) =>
        val o = new CustomType(0, 0, "test")
        for (t <- first) {
          o.myInt = t.myInt
          o.myLong += t.myLong
        }
        for (t <- second) {
          o.myInt = t.myInt
          o.myLong += t.myLong
        }
        o
    }
    coGroupDs.writeAsText(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expectedResult = "1,0,test\n" + "2,6,test\n" + "3,24,test\n" + "4,60,test\n" + "5,120,test\n" +
      "6," + "210,test\n"
  }

}
