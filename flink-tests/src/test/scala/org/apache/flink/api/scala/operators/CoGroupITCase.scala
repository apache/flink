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

import org.apache.flink.api.common.functions.RichCoGroupFunction
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.util.CollectionDataSets.CustomType
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{TestBaseUtils, MultipleProgramsTestBase}
import org.apache.flink.util.Collector
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit._

import scala.collection.JavaConverters._

import org.apache.flink.api.scala._

@RunWith(classOf[Parameterized])
class CoGroupITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
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
    val coGroupDs = ds.coGroup(ds2).where(0).equalTo(0) {
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

    val coGroupDs = ds.coGroup(ds2).where(_.myInt).equalTo(_.myInt) apply {
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
  def testCorrectnessIfCoGroupReturnsLeftInputObjects(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val ds2 = CollectionDataSets.get3TupleDataSet(env)
    val coGroupDs = ds.coGroup(ds2).where(0).equalTo(0) {
      (
        first: Iterator[(Int, Long, String)],
        second: Iterator[(Int, Long, String)],
        out: Collector[(Int, Long, String)] ) =>
          for (t <- first) {
            if (t._1 < 6) {
              out.collect(t)
            }
          }
    }
    coGroupDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expectedResult = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" + "4,3,Hello world, " +
      "how are you?\n" + "5,3,I am fine.\n"
  }

  @Test
  def testCorrectnessIfCoGroupReturnsRightInputObjects(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get5TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val coGroupDs = ds.coGroup(ds2).where(0).equalTo(0) {
      (
        first: Iterator[(Int, Long, Int, String, Long)],
        second: Iterator[(Int, Long, Int, String, Long)],
        out: Collector[(Int, Long, Int, String, Long)]) =>
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
  def testCoGroupWithBroadcastVariable(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val intDs = CollectionDataSets.getIntDataSet(env)
    val ds = CollectionDataSets.get5TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val coGroupDs = ds.coGroup(ds2).where(0).equalTo(0).apply(
      new RichCoGroupFunction[
        (Int, Long, Int, String, Long),
        (Int, Long, Int, String, Long),
        (Int, Int, Int)] {
        private var broadcast = 41

        override def open(config: Configuration) {
          val ints = this.getRuntimeContext.getBroadcastVariable[Int]("ints").asScala
          broadcast = ints.sum
        }

        override def coGroup(
                              first: java.lang.Iterable[(Int, Long, Int, String, Long)],
                              second: java.lang.Iterable[(Int, Long, Int, String, Long)],
                              out: Collector[(Int, Int, Int)]): Unit = {
          var sum = 0
          var id = 0
          for (t <- first.asScala) {
            sum += t._3
            id = t._1
          }
          for (t <- second.asScala) {
            sum += t._3
            id = t._1
          }
          out.collect((id, sum, broadcast))
        }

      }).withBroadcastSet(intDs, "ints")
    coGroupDs.writeAsCsv(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expectedResult = "1,0,55\n" + "2,6,55\n" + "3,24,55\n" + "4,60,55\n" + "5,120,55\n"
  }

  @Test
  def testCoGroupOnTupleWithKeyFieldSelectorAndCustomTypeWithKeyExtractor(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get5TupleDataSet(env)
    val ds2 = CollectionDataSets.getCustomTypeDataSet(env)
    val coGroupDs = ds.coGroup(ds2).where(2).equalTo(_.myInt) apply {
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
    val coGroupDs = ds2.coGroup(ds).where(_.myInt).equalTo(2) {
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
  def testCoGroupWithMultipleKeyFields(): Unit = {
    /*
        * CoGroup with multiple key fields
        */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.get5TupleDataSet(env)
    val ds2 = CollectionDataSets.get3TupleDataSet(env)
    val coGrouped = ds1.coGroup(ds2).where(0,4).equalTo(0, 1) {
      (
        first: Iterator[(Int, Long, Int, String, Long)],
        second: Iterator[(Int, Long, String)],
        out: Collector[(Int, Long, String)]) =>
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
      (
        first: Iterator[(Int, Long, Int, String, Long)],
        second: Iterator[(Int, Long, String)],
        out: Collector[(Int, Long, String)]) =>
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
    val coGroupDs = ds.coGroup(ds2).where("myInt").equalTo("myInt") {
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

  @Test
  def testCoGroupOnTwoCustomTypesUsingExpressionKeysAndFieldSelector(): Unit = {
    /*
     * CoGroup on two custom type inputs using expression keys
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getSmallPojoDataSet(env)
    val ds2 = CollectionDataSets.getSmallTuplebasedPojoMatchingDataSet(env)
    val coGroupDs = ds.coGroup(ds2).where("nestedPojo.longNumber").equalTo(6) {
      (
        first: Iterator[CollectionDataSets.POJO],
        second: Iterator[(Int, String, Int, Int, Long, String, Long)],
        out: Collector[CustomType]) =>
          for (p <- first) {
            for (t <- second) {
              Assert.assertTrue(p.nestedPojo.longNumber == t._7)
              out.collect(new CustomType(-1, p.nestedPojo.longNumber, "Flink"))
            }
          }
    }
    coGroupDs.writeAsText(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expectedResult = "-1,20000,Flink\n" + "-1,10000,Flink\n" + "-1,30000,Flink\n"
  }

  @Test
  def testCoGroupFieldSelectorAndKeySelector(): Unit = {
    /*
     * CoGroup field-selector (expression keys) + key selector function
     * The key selector is unnecessary complicated (Tuple1) ;)
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getSmallPojoDataSet(env)
    val ds2 = CollectionDataSets.getSmallTuplebasedPojoMatchingDataSet(env)
    val coGroupDs = ds.coGroup(ds2).where(t => new Tuple1(t.nestedPojo.longNumber)).equalTo(6) {
      (
        first: Iterator[CollectionDataSets.POJO],
        second: Iterator[(Int, String, Int, Int, Long, String, Long)],
        out: Collector[CustomType]) =>
          for (p <- first) {
            for (t <- second) {
              Assert.assertTrue(p.nestedPojo.longNumber == t._7)
              out.collect(new CustomType(-1, p.nestedPojo.longNumber, "Flink"))
            }
          }
    }
    coGroupDs.writeAsText(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expectedResult = "-1,20000,Flink\n" + "-1,10000,Flink\n" + "-1,30000,Flink\n"
  }

  @Test
  def testCoGroupKeySelectorAndFieldSelector(): Unit = {
    /*
         * CoGroup field-selector (expression keys) + key selector function
         * The key selector is simple here
         */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getSmallPojoDataSet(env)
    val ds2 = CollectionDataSets.getSmallTuplebasedPojoMatchingDataSet(env)
    val coGroupDs = ds.coGroup(ds2).where(_.nestedPojo.longNumber).equalTo(6) {
      (
        first: Iterator[CollectionDataSets.POJO],
        second: Iterator[(Int, String, Int, Int, Long, String, Long)],
        out: Collector[CustomType]) =>
          for (p <- first) {
            for (t <- second) {
              Assert.assertTrue(p.nestedPojo.longNumber == t._7)
              out.collect(new CustomType(-1, p.nestedPojo.longNumber, "Flink"))
            }
          }
    }
    coGroupDs.writeAsText(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expectedResult = "-1,20000,Flink\n" + "-1,10000,Flink\n" + "-1,30000,Flink\n"
  }

  @Test
  def testCoGroupWithAtomic1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = env.fromElements(0, 1, 2)
    val coGroupDs = ds1.coGroup(ds2).where(0).equalTo("*") {
      (
        first: Iterator[(Int, Long, String)],
        second: Iterator[Int],
        out: Collector[(Int, Long, String)]) =>
          for (p <- first) {
            for (t <- second) {
              if (p._1 == t) {
                out.collect(p)
              }
            }
          }
    }

    coGroupDs.writeAsText(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expectedResult = "(1,1,Hi)\n(2,2,Hello)"
  }

  @Test
  def testCoGroupWithAtomic2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(0, 1, 2)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env)
    val coGroupDs = ds1.coGroup(ds2).where("*").equalTo(0) {
      (
        first: Iterator[Int],
        second: Iterator[(Int, Long, String)],
        out: Collector[(Int, Long, String)]) =>
          for (p <- first) {
            for (t <- second) {
              if (p == t._1) {
                out.collect(t)
              }
            }
          }
    }

    coGroupDs.writeAsText(resultPath, writeMode = WriteMode.OVERWRITE)
    env.execute()
    expectedResult = "(1,1,Hi)\n(2,2,Hello)"
  }
}

