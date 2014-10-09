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

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.configuration.Configuration
import org.apache.flink.test.util.JavaProgramTestBase
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.flink.api.scala._


object FilterProgs {
  var NUM_PROGRAMS: Int = 7

  def runProgram(progId: Int, resultPath: String): String = {
    progId match {
      case 1 =>
        /*
         * Test all-rejecting filter.
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env)
        val filterDs = ds.filter( t => false )
        filterDs.writeAsCsv(resultPath)
        env.execute()
        "\n"

      case 2 =>
        /*
         * Test all-passing filter.
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env)
        val filterDs = ds.filter( t => true )
        filterDs.writeAsCsv(resultPath)
        env.execute()
        "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" + "4,3,Hello world, " +
          "how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" + "7,4," +
          "Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" + "11,5," +
          "Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" + "15,5," +
          "Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" + "19," +
          "6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"

      case 3 =>
        /*
         * Test filter on String tuple field.
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env)
        val filterDs = ds.filter( _._3.contains("world") )
        filterDs.writeAsCsv(resultPath)
        env.execute()
        "3,2,Hello world\n" + "4,3,Hello world, how are you?\n"

      case 4 =>
        /*
         * Test filter on Integer tuple field.
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env)
        val filterDs = ds.filter( _._1 % 2 == 0 )
        filterDs.writeAsCsv(resultPath)
        env.execute()
        "2,2,Hello\n" + "4,3,Hello world, how are you?\n" + "6,3,Luke Skywalker\n" + "8,4," +
          "Comment#2\n" + "10,4,Comment#4\n" + "12,5,Comment#6\n" + "14,5,Comment#8\n" + "16,6," +
          "Comment#10\n" + "18,6,Comment#12\n" + "20,6,Comment#14\n"

      case 5 =>
        /*
         * Test filter on basic type
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getStringDataSet(env)
        val filterDs = ds.filter( _.startsWith("H") )
        filterDs.writeAsText(resultPath)
        env.execute()
        "Hi\n" + "Hello\n" + "Hello world\n" + "Hello world, how are you?\n"

      case 6 =>
        /*
         * Test filter on custom type
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getCustomTypeDataSet(env)
        val filterDs = ds.filter( _.myString.contains("a") )
        filterDs.writeAsText(resultPath)
        env.execute()
        "3,3,Hello world, how are you?\n" + "3,4,I am fine.\n" + "3,5,Luke Skywalker\n"

      case 7 =>
        /*
         * Test filter on String tuple field.
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ints = CollectionDataSets.getIntDataSet(env)
        val ds = CollectionDataSets.get3TupleDataSet(env)
        val filterDs = ds.filter( new RichFilterFunction[(Int, Long, String)] {
          var literal = -1
          override def open(config: Configuration): Unit = {
            val ints = getRuntimeContext.getBroadcastVariable[Int]("ints")
            for (i <- ints.asScala) {
              literal = if (literal < i) i else literal
            }
          }
          override def filter(value: (Int, Long, String)): Boolean = value._1 < literal
        }).withBroadcastSet(ints, "ints")
        filterDs.writeAsCsv(resultPath)
        env.execute()
        "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" + "4,3,Hello world, how are you?\n"

      case _ =>
        throw new IllegalArgumentException("Invalid program id")
    }
  }
}


@RunWith(classOf[Parameterized])
class FilterITCase(config: Configuration) extends JavaProgramTestBase(config) {

  private var curProgId: Int = config.getInteger("ProgramId", -1)
  private var resultPath: String = null
  private var expectedResult: String = null

  protected override def preSubmit(): Unit = {
    resultPath = getTempDirPath("result")
  }

  protected def testProgram(): Unit = {
    expectedResult = FilterProgs.runProgram(curProgId, resultPath)
  }

  protected override def postSubmit(): Unit = {
    compareResultsByLinesInMemory(expectedResult, resultPath)
  }
}

object FilterITCase {
  @Parameters
  def getConfigurations: java.util.Collection[Array[AnyRef]] = {
    val configs = mutable.MutableList[Array[AnyRef]]()
    for (i <- 1 to FilterProgs.NUM_PROGRAMS) {
      val config = new Configuration()
      config.setInteger("ProgramId", i)
      configs += Array(config)
    }

    configs.asJavaCollection
  }
}

