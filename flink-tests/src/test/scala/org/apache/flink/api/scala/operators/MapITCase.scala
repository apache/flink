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

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.util.CollectionDataSets.MutableTuple3
import org.apache.flink.configuration.Configuration
import org.apache.flink.test.util.JavaProgramTestBase
import org.apache.flink.util.Collector
import org.junit.Assert
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.flink.api.scala._


object MapProgs {
  var NUM_PROGRAMS: Int = 9

  def runProgram(progId: Int, resultPath: String): String = {
    progId match {
      case 1 =>
        /*
         * Test identity map with basic type
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getStringDataSet(env)
        val identityMapDs = ds.map( t => t)
        identityMapDs.writeAsText(resultPath)
        env.execute()
        "Hi\n" + "Hello\n" + "Hello world\n" + "Hello world, how are you?\n" + "I am fine" +
          ".\n" + "Luke Skywalker\n" + "Random comment\n" + "LOL\n"
      
      case 2 =>
        /*
         * Test identity map with a tuple
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env)
        val identityMapDs = ds.map( t => t )
        identityMapDs.writeAsCsv(resultPath)
        env.execute()
        "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" + "4,3,Hello world, " +
          "how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" + "7,4," +
          "Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" + "11,5," +
          "Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" + "15,5," +
          "Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" + "19," +
          "6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"
      
      case 3 =>
        /*
         * Test type conversion mapper (Custom -> Tuple)
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getCustomTypeDataSet(env)
        val typeConversionMapDs = ds.map( c => (c.myInt, c.myLong, c.myString) )
        typeConversionMapDs.writeAsCsv(resultPath)
        env.execute()
        "1,0,Hi\n" + "2,1,Hello\n" + "2,2,Hello world\n" + "3,3,Hello world, " +
          "how are you?\n" + "3,4,I am fine.\n" + "3,5,Luke Skywalker\n" + "4,6," +
          "Comment#1\n" + "4,7,Comment#2\n" + "4,8,Comment#3\n" + "4,9,Comment#4\n" + "5,10," +
          "Comment#5\n" + "5,11,Comment#6\n" + "5,12,Comment#7\n" + "5,13,Comment#8\n" + "5,14," +
          "Comment#9\n" + "6,15,Comment#10\n" + "6,16,Comment#11\n" + "6,17,Comment#12\n" + "6," +
          "18,Comment#13\n" + "6,19,Comment#14\n" + "6,20,Comment#15\n"
      
      case 4 =>
        /*
         * Test type conversion mapper (Tuple -> Basic)
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env)
        val typeConversionMapDs = ds.map(_._3)
        typeConversionMapDs.writeAsText(resultPath)
        env.execute()
        "Hi\n" + "Hello\n" + "Hello world\n" + "Hello world, how are you?\n" + "I am fine" +
          ".\n" + "Luke Skywalker\n" + "Comment#1\n" + "Comment#2\n" + "Comment#3\n" +
          "Comment#4\n" + "Comment#5\n" + "Comment#6\n" + "Comment#7\n" + "Comment#8\n" +
          "Comment#9\n" + "Comment#10\n" + "Comment#11\n" + "Comment#12\n" + "Comment#13\n" +
          "Comment#14\n" + "Comment#15\n"
      
      case 5 =>
        /*
         * Test mapper on tuple - Increment Integer field, reorder second and third fields
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env)
        val tupleMapDs = ds.map( t => (t._1 + 1, t._3, t._2) )
        tupleMapDs.writeAsCsv(resultPath)
        env.execute()
        "2,Hi,1\n" + "3,Hello,2\n" + "4,Hello world,2\n" + "5,Hello world, how are you?," +
          "3\n" + "6,I am fine.,3\n" + "7,Luke Skywalker,3\n" + "8,Comment#1,4\n" + "9,Comment#2," +
          "4\n" + "10,Comment#3,4\n" + "11,Comment#4,4\n" + "12,Comment#5,5\n" + "13,Comment#6," +
          "5\n" + "14,Comment#7,5\n" + "15,Comment#8,5\n" + "16,Comment#9,5\n" + "17,Comment#10," +
          "6\n" + "18,Comment#11,6\n" + "19,Comment#12,6\n" + "20,Comment#13,6\n" + "21," +
          "Comment#14,6\n" + "22,Comment#15,6\n"
      
      case 6 =>
        /*
         * Test mapper on Custom - lowercase myString
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getCustomTypeDataSet(env)
        val customMapDs = ds.map { c => c.myString = c.myString.toLowerCase; c }
        customMapDs.writeAsText(resultPath)
        env.execute()
        "1,0,hi\n" + "2,1,hello\n" + "2,2,hello world\n" + "3,3,hello world, " +
          "how are you?\n" + "3,4,i am fine.\n" + "3,5,luke skywalker\n" + "4,6," +
          "comment#1\n" + "4,7,comment#2\n" + "4,8,comment#3\n" + "4,9,comment#4\n" + "5,10," +
          "comment#5\n" + "5,11,comment#6\n" + "5,12,comment#7\n" + "5,13,comment#8\n" + "5,14," +
          "comment#9\n" + "6,15,comment#10\n" + "6,16,comment#11\n" + "6,17,comment#12\n" + "6," +
          "18,comment#13\n" + "6,19,comment#14\n" + "6,20,comment#15\n"
      
      case 7 =>
        /*
         * Test mapper if UDF returns input object - increment first field of a tuple
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env).map {
          t => MutableTuple3(t._1, t._2, t._3)
        }
        val inputObjMapDs = ds.map { t => t._1 = t._1 + 1; t }
        inputObjMapDs.writeAsCsv(resultPath)
        env.execute()
        "2,1,Hi\n" + "3,2,Hello\n" + "4,2,Hello world\n" + "5,3,Hello world, " +
          "how are you?\n" + "6,3,I am fine.\n" + "7,3,Luke Skywalker\n" + "8,4," +
          "Comment#1\n" + "9,4,Comment#2\n" + "10,4,Comment#3\n" + "11,4,Comment#4\n" + "12,5," +
          "Comment#5\n" + "13,5,Comment#6\n" + "14,5,Comment#7\n" + "15,5,Comment#8\n" + "16,5," +
          "Comment#9\n" + "17,6,Comment#10\n" + "18,6,Comment#11\n" + "19,6,Comment#12\n" + "20," +
          "6,Comment#13\n" + "21,6,Comment#14\n" + "22,6,Comment#15\n"
      
      case 8 =>
        /*
         * Test map with broadcast set
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ints = CollectionDataSets.getIntDataSet(env)
        val ds = CollectionDataSets.get3TupleDataSet(env)
        val bcMapDs = ds.map(
          new RichMapFunction[(Int, Long, String), (Int, Long, String)] {
            var f2Replace = 0
            override def open(config: Configuration): Unit = {
              val ints = getRuntimeContext.getBroadcastVariable[Int]("ints").asScala
              f2Replace = ints.sum
            }
            override def map(in: (Int, Long, String)): (Int, Long, String) = {
              in.copy(_1 = f2Replace)
            }
          }).withBroadcastSet(ints, "ints")
        bcMapDs.writeAsCsv(resultPath)
        env.execute()
        "55,1,Hi\n" + "55,2,Hello\n" + "55,2,Hello world\n" + "55,3,Hello world, " +
          "how are you?\n" + "55,3,I am fine.\n" + "55,3,Luke Skywalker\n" + "55,4," +
          "Comment#1\n" + "55,4,Comment#2\n" + "55,4,Comment#3\n" + "55,4,Comment#4\n" + "55,5," +
          "Comment#5\n" + "55,5,Comment#6\n" + "55,5,Comment#7\n" + "55,5,Comment#8\n" + "55,5," +
          "Comment#9\n" + "55,6,Comment#10\n" + "55,6,Comment#11\n" + "55,6,Comment#12\n" + "55," +
          "6,Comment#13\n" + "55,6,Comment#14\n" + "55,6,Comment#15\n"
      
      case 9 =>
        /*
         * Test passing configuration object.
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getSmall3TupleDataSet(env)
        val conf = new Configuration
        val testKey = "testVariable"
        val testValue = 666
        conf.setInteger(testKey, testValue)
        val bcMapDs = ds.map(
          new RichMapFunction[(Int, Long, String), (Int, Long, String)] {
            override def open(config: Configuration): Unit = {
              val fromConfig = config.getInteger(testKey, -1)
              Assert.assertEquals(testValue, fromConfig)
            }
            override def map(in: (Int, Long, String)): (Int, Long, String) = {
              in
            }
          }).withParameters(conf)
        bcMapDs.writeAsCsv(resultPath)
        env.execute()
        "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world"
      
      case _ =>
        throw new IllegalArgumentException("Invalid program id")
    }
  }
}


@RunWith(classOf[Parameterized])
class MapITCase(config: Configuration) extends JavaProgramTestBase(config) {

  private var curProgId: Int = config.getInteger("ProgramId", -1)
  private var resultPath: String = null
  private var expectedResult: String = null

  protected override def preSubmit(): Unit = {
    resultPath = getTempDirPath("result")
  }

  protected def testProgram(): Unit = {
    expectedResult = MapProgs.runProgram(curProgId, resultPath)
  }

  protected override def postSubmit(): Unit = {
    compareResultsByLinesInMemory(expectedResult, resultPath)
  }
}

object MapITCase {
  @Parameters
  def getConfigurations: java.util.Collection[Array[AnyRef]] = {
    val configs = mutable.MutableList[Array[AnyRef]]()
    for (i <- 1 to MapProgs.NUM_PROGRAMS) {
      val config = new Configuration()
      config.setInteger("ProgramId", i)
      configs += Array(config)
    }

    configs.asJavaCollection
  }
}

