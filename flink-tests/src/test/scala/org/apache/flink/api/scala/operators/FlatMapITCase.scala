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

import org.apache.flink.api.common.functions.RichFlatMapFunction
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


object FlatMapProgs {
  var NUM_PROGRAMS: Int = 7

  def runProgram(progId: Int, resultPath: String): String = {
    progId match {
      case 1 =>
        /*
         * Test non-passing flatmap
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getStringDataSet(env)
        val nonPassingFlatMapDs = ds.flatMap( in => if (in.contains("banana")) Some(in) else None )
        nonPassingFlatMapDs.writeAsText(resultPath)
        env.execute()
        "\n"
        
      case 2 =>
        /*
         * Test data duplicating flatmap
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getStringDataSet(env)
        val duplicatingFlatMapDs = ds.flatMap( in => Seq(in, in.toUpperCase) )
        duplicatingFlatMapDs.writeAsText(resultPath)
        env.execute()
        "Hi\n" + "HI\n" + "Hello\n" + "HELLO\n" + "Hello world\n" + "HELLO WORLD\n" +
          "Hello world, how are you?\n" + "HELLO WORLD, HOW ARE YOU?\n" + "I am fine.\n" + "I AM " +
          "FINE.\n" + "Luke Skywalker\n" + "LUKE SKYWALKER\n" + "Random comment\n" + "RANDOM " +
          "COMMENT\n" + "LOL\n" + "LOL\n"

      case 3 =>
        /*
         * Test flatmap with varying number of emitted tuples
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env)
        val varyingTuplesMapDs = ds.flatMap {
          in =>
            val numTuples = in._1 % 3
            (0 until numTuples) map { i => in }
        }
        varyingTuplesMapDs.writeAsCsv(resultPath)
        env.execute()
        "1,1,Hi\n" + "2,2,Hello\n" + "2,2,Hello\n" + "4,3,Hello world, " +
          "how are you?\n" + "5,3,I am fine.\n" + "5,3,I am fine.\n" + "7,4,Comment#1\n" + "8,4," +
          "Comment#2\n" + "8,4,Comment#2\n" + "10,4,Comment#4\n" + "11,5,Comment#5\n" + "11,5," +
          "Comment#5\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" + "14,5,Comment#8\n" + "16,6," +
          "Comment#10\n" + "17,6,Comment#11\n" + "17,6,Comment#11\n" + "19,6,Comment#13\n" + "20," +
          "6,Comment#14\n" + "20,6,Comment#14\n"

      case 4 =>
        /*
         * Test type conversion flatmapper (Custom -> Tuple)
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getCustomTypeDataSet(env)
        val typeConversionFlatMapDs = ds.flatMap { in => Some((in.myInt, in.myLong, in.myString)) }
        typeConversionFlatMapDs.writeAsCsv(resultPath)
        env.execute()
        "1,0,Hi\n" + "2,1,Hello\n" + "2,2,Hello world\n" + "3,3,Hello world, " +
          "how are you?\n" + "3,4,I am fine.\n" + "3,5,Luke Skywalker\n" + "4,6," +
          "Comment#1\n" + "4,7,Comment#2\n" + "4,8,Comment#3\n" + "4,9,Comment#4\n" + "5,10," +
          "Comment#5\n" + "5,11,Comment#6\n" + "5,12,Comment#7\n" + "5,13,Comment#8\n" + "5,14," +
          "Comment#9\n" + "6,15,Comment#10\n" + "6,16,Comment#11\n" + "6,17,Comment#12\n" + "6," +
          "18,Comment#13\n" + "6,19,Comment#14\n" + "6,20,Comment#15\n"

      case 5 =>
        /*
         * Test type conversion flatmapper (Tuple -> Basic)
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env)
        val typeConversionFlatMapDs = ds.flatMap ( in => Some(in._3) )
        typeConversionFlatMapDs.writeAsText(resultPath)
        env.execute()
        "Hi\n" + "Hello\n" + "Hello world\n" + "Hello world, how are you?\n" + "I am fine" +
          ".\n" + "Luke Skywalker\n" + "Comment#1\n" + "Comment#2\n" + "Comment#3\n" +
          "Comment#4\n" + "Comment#5\n" + "Comment#6\n" + "Comment#7\n" + "Comment#8\n" +
          "Comment#9\n" + "Comment#10\n" + "Comment#11\n" + "Comment#12\n" + "Comment#13\n" +
          "Comment#14\n" + "Comment#15\n"

      case 6 =>
        /*
         * Test flatmapper if UDF returns input object
         * multiple times and changes it in between
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env).map {
          t => MutableTuple3(t._1, t._2, t._3)
        }
        val inputObjFlatMapDs = ds.flatMap {
          (in, out: Collector[MutableTuple3[Int, Long, String]]) =>
            val numTuples = in._1 % 4
            (0 until numTuples) foreach { i => in._1 = i; out.collect(in) }
        }
        inputObjFlatMapDs.writeAsCsv(resultPath)
        env.execute()
        "0,1,Hi\n" + "0,2,Hello\n" + "1,2,Hello\n" + "0,2,Hello world\n" + "1,2," +
          "Hello world\n" + "2,2,Hello world\n" + "0,3,I am fine.\n" + "0,3," +
          "Luke Skywalker\n" + "1,3,Luke Skywalker\n" + "0,4,Comment#1\n" + "1,4," +
          "Comment#1\n" + "2,4,Comment#1\n" + "0,4,Comment#3\n" + "0,4,Comment#4\n" + "1,4," +
          "Comment#4\n" + "0,5,Comment#5\n" + "1,5,Comment#5\n" + "2,5,Comment#5\n" + "0,5," +
          "Comment#7\n" + "0,5,Comment#8\n" + "1,5,Comment#8\n" + "0,5,Comment#9\n" + "1,5," +
          "Comment#9\n" + "2,5,Comment#9\n" + "0,6,Comment#11\n" + "0,6,Comment#12\n" + "1,6," +
          "Comment#12\n" + "0,6,Comment#13\n" + "1,6,Comment#13\n" + "2,6,Comment#13\n" + "0,6," +
          "Comment#15\n"

      case 7 =>
        /*
         * Test flatmap with broadcast set
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ints = CollectionDataSets.getIntDataSet(env)
        val ds = CollectionDataSets.get3TupleDataSet(env).map {
          t => MutableTuple3(t._1, t._2, t._3)
        }
        val bcFlatMapDs = ds.flatMap(
          new RichFlatMapFunction[MutableTuple3[Int, Long, String],
            MutableTuple3[Int, Long, String]] {
            private var f2Replace = 0
            private val outTuple = MutableTuple3(0, 0L, "")
            override def open(config: Configuration): Unit = {
              val ints = getRuntimeContext.getBroadcastVariable[Int]("ints").asScala
              f2Replace = ints.sum
            }
            override def flatMap(
                value: MutableTuple3[Int, Long, String],
                out: Collector[MutableTuple3[Int, Long, String]]): Unit = {
                outTuple._1 = f2Replace
                outTuple._2 = value._2
                outTuple._3 = value._3
                out.collect(outTuple)
            }
          }).withBroadcastSet(ints, "ints")
        bcFlatMapDs.writeAsCsv(resultPath)
        env.execute()
        "55,1,Hi\n" + "55,2,Hello\n" + "55,2,Hello world\n" + "55,3,Hello world, " +
          "how are you?\n" + "55,3,I am fine.\n" + "55,3,Luke Skywalker\n" + "55,4," +
          "Comment#1\n" + "55,4,Comment#2\n" + "55,4,Comment#3\n" + "55,4,Comment#4\n" + "55,5," +
          "Comment#5\n" + "55,5,Comment#6\n" + "55,5,Comment#7\n" + "55,5,Comment#8\n" + "55,5," +
          "Comment#9\n" + "55,6,Comment#10\n" + "55,6,Comment#11\n" + "55,6,Comment#12\n" + "55," +
          "6,Comment#13\n" + "55,6,Comment#14\n" + "55,6,Comment#15\n"

      case _ =>
        throw new IllegalArgumentException("Invalid program id")
    }
  }
}


@RunWith(classOf[Parameterized])
class FlatMapITCase(config: Configuration) extends JavaProgramTestBase(config) {

  private var curProgId: Int = config.getInteger("ProgramId", -1)
  private var resultPath: String = null
  private var expectedResult: String = null

  protected override def preSubmit(): Unit = {
    resultPath = getTempDirPath("result")
  }

  protected def testProgram(): Unit = {
    expectedResult = FlatMapProgs.runProgram(curProgId, resultPath)
  }

  protected override def postSubmit(): Unit = {
    compareResultsByLinesInMemory(expectedResult, resultPath)
  }
}

object FlatMapITCase {
  @Parameters
  def getConfigurations: java.util.Collection[Array[AnyRef]] = {
    val configs = mutable.MutableList[Array[AnyRef]]()
    for (i <- 1 to FlatMapProgs.NUM_PROGRAMS) {
      val config = new Configuration()
      config.setInteger("ProgramId", i)
      configs += Array(config)
    }

    configs.asJavaCollection
  }
}

