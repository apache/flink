/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.test.util.JavaProgramTestBase
import org.apache.flink.util.Collector
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.junit.Assert

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.flink.api.scala._


object CoGroupProgs {
  var NUM_PROGRAMS: Int = 13

  def runProgram(progId: Int, resultPath: String): String = {
    progId match {
      case 1 =>
        /*
         * CoGroup on tuples with key field selector
         */
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
        coGroupDs.writeAsCsv(resultPath)
        env.execute()
        "1,0\n" + "2,6\n" + "3,24\n" + "4,60\n" + "5,120\n"

      case 2 =>
        /*
         * CoGroup on two custom type inputs with key extractors
         */
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
        coGroupDs.writeAsText(resultPath)
        env.execute()
        "1,0,test\n" + "2,6,test\n" + "3,24,test\n" + "4,60,test\n" + "5,120,test\n" + "6," +
          "210,test\n"

      case 3 =>
        /*
         * check correctness of cogroup if UDF returns left input objects
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env)
        val ds2 = CollectionDataSets.get3TupleDataSet(env)
        val coGroupDs = ds.coGroup(ds2).where(0).equalTo(0) {
          (first, second, out: Collector[(Int, Long, String)] ) =>
            for (t <- first) {
              if (t._1 < 6) {
                out.collect(t)
              }
            }
        }
        coGroupDs.writeAsCsv(resultPath)
        env.execute()
        "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" + "4,3,Hello world, " +
          "how are you?\n" + "5,3,I am fine.\n"

      case 4 =>
        /*
         * check correctness of cogroup if UDF returns right input objects
         */
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
        coGroupDs.writeAsCsv(resultPath)
        env.execute()
        "1,1,0,Hallo,1\n" + "2,2,1,Hallo Welt,2\n" + "2,3,2,Hallo Welt wie,1\n" + "3,4,3," +
          "Hallo Welt wie gehts?,2\n" + "3,5,4,ABC,2\n" + "3,6,5,BCD,3\n"

      case 5 =>
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
        coGroupDs.writeAsCsv(resultPath)
        env.execute()
        "1,0,55\n" + "2,6,55\n" + "3,24,55\n" + "4,60,55\n" + "5,120,55\n"

      case 6 =>
        /*
         * CoGroup on a tuple input with key field selector and a custom type input with
         * key extractor
         */
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
        coGroupDs.writeAsCsv(resultPath)
        env.execute()
        "0,1,test\n" + "1,2,test\n" + "2,5,test\n" + "3,15,test\n" + "4,33,test\n" + "5," +
          "63,test\n" + "6,109,test\n" + "7,4,test\n" + "8,4,test\n" + "9,4,test\n" + "10,5," +
          "test\n" + "11,5,test\n" + "12,5,test\n" + "13,5,test\n" + "14,5,test\n"

      case 7 =>
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
        coGroupDs.writeAsText(resultPath)
        env.execute()
        "0,1,test\n" + "1,2,test\n" + "2,5,test\n" + "3,15,test\n" + "4,33,test\n" + "5," +
          "63,test\n" + "6,109,test\n" + "7,4,test\n" + "8,4,test\n" + "9,4,test\n" + "10,5," +
          "test\n" + "11,5,test\n" + "12,5,test\n" + "13,5,test\n" + "14,5,test\n"

      case 8 =>
        /*
         * CoGroup with multiple key fields
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.get5TupleDataSet(env)
        val ds2 = CollectionDataSets.get3TupleDataSet(env)
        val coGrouped = ds1.coGroup(ds2).where(0,4).equalTo(0, 1) {
          (first, second, out: Collector[(Int, Long, String)]) =>
            val strs = first map(_._4)
            for (t <- second) {
              for (s <- strs) {
                out.collect((t._1, t._2, s))
              }
            }
        }

        coGrouped.writeAsCsv(resultPath)
        env.execute()
        "1,1,Hallo\n" + "2,2,Hallo Welt\n" + "3,2,Hallo Welt wie gehts?\n" + "3,2," +
          "ABC\n" + "5,3,HIJ\n" + "5,3,IJK\n"

      case 9 =>
        /*
         * CoGroup with multiple key fields
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

        coGrouped.writeAsCsv(resultPath)
        env.execute()
        "1,1,Hallo\n" + "2,2,Hallo Welt\n" + "3,2,Hallo Welt wie gehts?\n" + "3,2," +
          "ABC\n" + "5,3,HIJ\n" + "5,3,IJK\n"

      case 10 =>
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
        coGroupDs.writeAsText(resultPath)
        env.execute()
        "1,0,test\n" + "2,6,test\n" + "3,24,test\n" + "4,60,test\n" + "5,120,test\n" + "6," +
          "210,test\n"

      case 11 =>
        /*
         * CoGroup on two custom type inputs using expression keys
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getSmallPojoDataSet(env)
        val ds2 = CollectionDataSets.getSmallTuplebasedPojoMatchingDataSet(env)
        val coGroupDs = ds.coGroup(ds2).where("nestedPojo.longNumber").equalTo(6) {
          (first, second, out: Collector[CustomType]) =>
            for (p <- first) {
              for (t <- second) {
                Assert.assertTrue(p.nestedPojo.longNumber == t._7)
                out.collect(new CustomType(-1, p.nestedPojo.longNumber, "Flink"))
              }
            }
        }
        coGroupDs.writeAsText(resultPath)
        env.execute()
        "-1,20000,Flink\n" + "-1,10000,Flink\n" + "-1,30000,Flink\n"

      case 12 =>
        /*
         * CoGroup field-selector (expression keys) + key selector function
         * The key selector is unnecessary complicated (Tuple1) ;)
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getSmallPojoDataSet(env)
        val ds2 = CollectionDataSets.getSmallTuplebasedPojoMatchingDataSet(env)
        val coGroupDs = ds.coGroup(ds2).where(t => new Tuple1(t.nestedPojo.longNumber)).equalTo(6) {
          (first, second, out: Collector[CustomType]) =>
            for (p <- first) {
              for (t <- second) {
                Assert.assertTrue(p.nestedPojo.longNumber == t._7)
                out.collect(new CustomType(-1, p.nestedPojo.longNumber, "Flink"))
              }
            }
        }
        coGroupDs.writeAsText(resultPath)
        env.execute()
        "-1,20000,Flink\n" + "-1,10000,Flink\n" + "-1,30000,Flink\n"

      case 13 =>
        /*
         * CoGroup field-selector (expression keys) + key selector function
         * The key selector is simple here
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getSmallPojoDataSet(env)
        val ds2 = CollectionDataSets.getSmallTuplebasedPojoMatchingDataSet(env)
        val coGroupDs = ds.coGroup(ds2).where(_.nestedPojo.longNumber).equalTo(6) {
          (first, second, out: Collector[CustomType]) =>
            for (p <- first) {
              for (t <- second) {
                Assert.assertTrue(p.nestedPojo.longNumber == t._7)
                out.collect(new CustomType(-1, p.nestedPojo.longNumber, "Flink"))
              }
            }
        }
        coGroupDs.writeAsText(resultPath)
        env.execute()
        "-1,20000,Flink\n" + "-1,10000,Flink\n" + "-1,30000,Flink\n"

      case _ =>
        throw new IllegalArgumentException("Invalid program id")
    }
  }
}


@RunWith(classOf[Parameterized])
class CoGroupITCase(config: Configuration) extends JavaProgramTestBase(config) {

  private val curProgId: Int = config.getInteger("ProgramId", -1)
  private var resultPath: String = null
  private var expectedResult: String = null

  protected override def preSubmit(): Unit = {
    resultPath = getTempDirPath("result")
  }

  protected def testProgram(): Unit = {
    expectedResult = CoGroupProgs.runProgram(curProgId, resultPath)
  }

  protected override def postSubmit(): Unit = {
    compareResultsByLinesInMemory(expectedResult, resultPath)
  }
}

object CoGroupITCase {
  @Parameters
  def getConfigurations: java.util.Collection[Array[AnyRef]] = {
    val configs = mutable.MutableList[Array[AnyRef]]()
    for (i <- 1 to CoGroupProgs.NUM_PROGRAMS) {
      val config = new Configuration()
      config.setInteger("ProgramId", i)
      configs += Array(config)
    }

    configs.asJavaCollection
  }
}

