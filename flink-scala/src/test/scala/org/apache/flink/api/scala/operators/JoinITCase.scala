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

import org.apache.flink.api.common.functions.RichJoinFunction
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.util.CollectionDataSets.MutableTuple3
import org.apache.flink.configuration.Configuration
import org.apache.flink.test.util.JavaProgramTestBase
import org.apache.flink.util.Collector
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.flink.api.scala._


object JoinProgs {
  var NUM_PROGRAMS: Int = 19

  def runProgram(progId: Int, resultPath: String): String = {
    progId match {
      case 1 =>
        /*
         * UDF Join on tuples with key field positions
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
        val ds2 = CollectionDataSets.get5TupleDataSet(env)
        val joinDs = ds1.join(ds2).where(1).equalTo(1) { (l, r) => (l._3, r._4) }
        joinDs.writeAsCsv(resultPath)
        env.execute()
        "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n"
      
      case 2 =>
        /*
         * UDF Join on tuples with multiple key field positions
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.get3TupleDataSet(env)
        val ds2 = CollectionDataSets.get5TupleDataSet(env)
        val joinDs = ds1.join(ds2).where(0, 1).equalTo(0, 4) { (l, r) => (l._3, r._4) }
        joinDs.writeAsCsv(resultPath)
        env.execute()
        "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt wie gehts?\n" +
          "Hello world,ABC\n" + "I am fine.,HIJ\n" + "I am fine.,IJK\n"

      case 3 =>
        /*
         * Default Join on tuples
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
        val ds2 = CollectionDataSets.get5TupleDataSet(env)
        val joinDs = ds1.join(ds2).where(0).equalTo(2)
        joinDs.writeAsCsv(resultPath)
        env.execute()
        "(1,1,Hi),(2,2,1,Hallo Welt,2)\n" + "(2,2,Hello),(2,3,2,Hallo Welt wie," +
          "1)\n" + "(3,2,Hello world),(3,4,3,Hallo Welt wie gehts?,2)\n"

      case 4 =>
        /*
         * Join with Huge
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
        val ds2 = CollectionDataSets.get5TupleDataSet(env)
        val joinDs = ds1.joinWithHuge(ds2).where(1).equalTo(1) { (l, r) => (l._3, r._4) }
        joinDs.writeAsCsv(resultPath)
        env.execute()
        "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n"

      case 5 =>
        /*
         * Join with Tiny
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
        val ds2 = CollectionDataSets.get5TupleDataSet(env)
        val joinDs = ds1.joinWithTiny(ds2).where(1).equalTo(1) { (l, r) => (l._3, r._4) }
        joinDs.writeAsCsv(resultPath)
        env.execute()
        "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n"

      case 6 =>
        /*
         * Join that returns the left input object
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
        val ds2 = CollectionDataSets.get5TupleDataSet(env)
        val joinDs = ds1.join(ds2).where(1).equalTo(1) { (l, r) => l }
        joinDs.writeAsCsv(resultPath)
        env.execute()
        "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n"

      case 7 =>
        /*
         * Join that returns the right input object
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
        val ds2 = CollectionDataSets.get5TupleDataSet(env)
        val joinDs = ds1.join(ds2).where(1).equalTo(1) { (l, r) => r }
        joinDs.writeAsCsv(resultPath)
        env.execute()
        "1,1,0,Hallo,1\n" + "2,2,1,Hallo Welt,2\n" + "2,2,1,Hallo Welt,2\n"

      case 8 =>
        /*
         * Join with broadcast set
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val intDs = CollectionDataSets.getIntDataSet(env)
        val ds1 = CollectionDataSets.get3TupleDataSet(env)
        val ds2 = CollectionDataSets.getSmall5TupleDataSet(env)
        val joinDs = ds1.join(ds2).where(1).equalTo(4).apply(
          new RichJoinFunction[
            (Int, Long, String),
            (Int, Long, Int, String, Long),
            (String, String, Int)] {
            private var broadcast = 41

            override def open(config: Configuration) {
              val ints = this.getRuntimeContext.getBroadcastVariable[Int]("ints").asScala
              broadcast = ints.sum
            }

            override def join(
                first: (Int, Long, String),
                second: (Int, Long, Int, String, Long)): (String, String, Int) = {
              (first._3, second. _4, broadcast)
            }
          }
        ).withBroadcastSet(intDs, "ints")
        joinDs.writeAsCsv(resultPath)
        env.execute()
        "Hi,Hallo,55\n" + "Hi,Hallo Welt wie,55\n" + "Hello,Hallo Welt," +
          "55\n" + "Hello world,Hallo Welt,55\n"

      case 9 =>
        /*
         * Join on a tuple input with key field selector and a custom type input with key extractor
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.getSmallCustomTypeDataSet(env)
        val ds2 = CollectionDataSets.get3TupleDataSet(env)
        val joinDs = ds1.join(ds2).where( _.myInt ).equalTo(0) { (l, r) => (l.myString, r._3) }
        joinDs.writeAsCsv(resultPath)
        env.execute()
        "Hi,Hi\n" + "Hello,Hello\n" + "Hello world,Hello\n"

      case 10 => // 12 in Java ITCase
        /*
         * Join on a tuple input with key field selector and a custom type input with key extractor
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
        val ds2 = CollectionDataSets.getCustomTypeDataSet(env)
        val joinDs = ds1.join(ds2).where(1).equalTo(_.myLong) apply { (l, r) => (l._3, r.myString) }
        joinDs.writeAsCsv(resultPath)
        env.execute()
        "Hi,Hello\n" + "Hello,Hello world\n" + "Hello world,Hello world\n"

      case 11 => // 13 in Java ITCase
        /*
         * (Default) Join on two custom type inputs with key extractors
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.getCustomTypeDataSet(env)
        val ds2 = CollectionDataSets.getSmallCustomTypeDataSet(env)
        val joinDs = ds1.join(ds2).where(_.myInt).equalTo(_.myInt)
        joinDs.writeAsCsv(resultPath)
        env.execute()
        "1,0,Hi,1,0,Hi\n" + "2,1,Hello,2,1,Hello\n" + "2,1,Hello,2,2,Hello world\n" + "2," +
          "2,Hello world,2,1,Hello\n" + "2,2,Hello world,2,2,Hello world\n"

      case 12 => // 14 in Java ITCase
        /*
         * UDF Join on tuples with tuple-returning key selectors
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.get3TupleDataSet(env)
        val ds2 = CollectionDataSets.get5TupleDataSet(env)
        val joinDs = ds1.join(ds2).where( t => (t._1, t._2)).equalTo( t => (t._1, t._5)) apply {
          (l, r) => (l._3, r._4)
        }
        joinDs.writeAsCsv(resultPath)
        env.execute()
        "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt wie gehts?\n" +
          "Hello world,ABC\n" + "I am fine.,HIJ\n" + "I am fine.,IJK\n"

      /**
       * Joins with POJOs
       */
      case 13 => // 15 in Java ITCase
        /*
         * Join nested pojo against tuple (selected using a string)
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.getSmallPojoDataSet(env)
        val ds2 = CollectionDataSets.getSmallTuplebasedPojoMatchingDataSet(env)
        val joinDs = ds1.join(ds2).where("nestedPojo.longNumber").equalTo("_7")
        joinDs.writeAsCsv(resultPath)
        env.execute()
        "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One," +
          "10000)\n" + "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two," +
          "20000)\n" + "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n"

      case 14 => // 16 in Java ITCase
        /*
         * Join nested pojo against tuple (selected as an integer)
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.getSmallPojoDataSet(env)
        val ds2 = CollectionDataSets.getSmallTuplebasedPojoMatchingDataSet(env)
        val joinDs = ds1.join(ds2).where("nestedPojo.longNumber").equalTo(6) // <-- difference
        joinDs.writeAsCsv(resultPath)
        env.execute()
        "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One," +
          "10000)\n" + "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two," +
          "20000)\n" + "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n"

      case 15 => // 17 in Java ITCase
        /*
         * selecting multiple fields using expression language
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.getSmallPojoDataSet(env)
        val ds2 = CollectionDataSets.getSmallTuplebasedPojoMatchingDataSet(env)
        val joinDs = ds1.join(ds2)
          .where("nestedPojo.longNumber", "number", "str")
          .equalTo("_7", "_1", "_2")
        joinDs.writeAsCsv(resultPath)
        env.setDegreeOfParallelism(1)
        env.execute()
        "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One," +
          "10000)\n" + "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two," +
          "20000)\n" + "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n"

      case 16 => // 18 in Java ITCase
        /*
         * nested into tuple
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.getSmallPojoDataSet(env)
        val ds2 = CollectionDataSets.getSmallTuplebasedPojoMatchingDataSet(env)
        val joinDs = ds1.join(ds2).where("nestedPojo.longNumber", "number",
          "nestedTupleWithCustom._1").equalTo("_7", "_1", "_3")
        joinDs.writeAsCsv(resultPath)
        env.setDegreeOfParallelism(1)
        env.execute()
        "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One," +
          "10000)\n" + "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two," +
          "20000)\n" + "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n"

      case 17 => // 19 in Java ITCase
        /*
         * nested into tuple into pojo
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.getSmallPojoDataSet(env)
        val ds2 = CollectionDataSets.getSmallTuplebasedPojoMatchingDataSet(env)
        val joinDs = ds1.join(ds2)
          .where("nestedTupleWithCustom._1",
            "nestedTupleWithCustom._2.myInt",
            "nestedTupleWithCustom._2.myLong")
          .equalTo("_3", "_4", "_5")
        joinDs.writeAsCsv(resultPath)
        env.setDegreeOfParallelism(1)
        env.execute()
        "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One," +
          "10000)\n" + "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two," +
          "20000)\n" + "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n"

      case 18 => // 20 in Java ITCase
        /*
         * Non-POJO test to verify that full-tuple keys are working.
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.getSmallNestedTupleDataSet(env)
        val ds2 = CollectionDataSets.getSmallNestedTupleDataSet(env)
        val joinDs = ds1.join(ds2).where(0).equalTo("_1._1", "_1._2")
        joinDs.writeAsCsv(resultPath)
        env.setDegreeOfParallelism(1)
        env.execute()
        "((1,1),one),((1,1),one)\n" + "((2,2),two),((2,2),two)\n" + "((3,3),three),((3,3)," +
          "three)\n"

      case 19 => // 21 in Java ITCase
        /*
         * Non-POJO test to verify "nested" tuple-element selection.
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.getSmallNestedTupleDataSet(env)
        val ds2 = CollectionDataSets.getSmallNestedTupleDataSet(env)
        val joinDs = ds1.join(ds2).where("_1._1").equalTo("_1._1")
        joinDs.writeAsCsv(resultPath)
        env.setDegreeOfParallelism(1)
        env.execute()
        "((1,1),one),((1,1),one)\n" + "((2,2),two),((2,2),two)\n" + "((3,3),three),((3,3),three)\n"

      case 20 => // 22 in Java ITCase
        /*
         * full pojo with full tuple
         */
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val ds1 = CollectionDataSets.getSmallPojoDataSet(env)
        val ds2 = CollectionDataSets.getSmallTuplebasedPojoMatchingDataSet(env)
        val joinDs = ds1.join(ds2).where("*").equalTo("*")
        joinDs.writeAsCsv(resultPath)
        env.setDegreeOfParallelism(1)
        env.execute()
        "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One,10000)\n" + "2 Second (20,200," +
          "2000,Two) 20000,(2,Second,20,200,2000,Two,20000)\n" + "3 Third (30,300,3000," +
          "Three) 30000,(3,Third,30,300,3000,Three,30000)\n"
      
      case _ =>
        throw new IllegalArgumentException("Invalid program id: " + progId)
    }
  }
}


@RunWith(classOf[Parameterized])
class JoinITCase(config: Configuration) extends JavaProgramTestBase(config) {

  private var curProgId: Int = config.getInteger("ProgramId", -1)
  private var resultPath: String = null
  private var expectedResult: String = null

  protected override def preSubmit(): Unit = {
    resultPath = getTempDirPath("result")
  }

  protected def testProgram(): Unit = {
    expectedResult = JoinProgs.runProgram(curProgId, resultPath)
  }

  protected override def postSubmit(): Unit = {
    compareResultsByLinesInMemory(expectedResult, resultPath)
  }
}

object JoinITCase {
  @Parameters
  def getConfigurations: java.util.Collection[Array[AnyRef]] = {
    val configs = mutable.MutableList[Array[AnyRef]]()
    for (i <- 1 to JoinProgs.NUM_PROGRAMS) {
      val config = new Configuration()
      config.setInteger("ProgramId", i)
      configs += Array(config)
    }

    configs.asJavaCollection
  }
}

