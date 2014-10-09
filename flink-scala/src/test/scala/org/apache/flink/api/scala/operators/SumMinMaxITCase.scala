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

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.configuration.Configuration
import org.apache.flink.test.util.JavaProgramTestBase
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.flink.api.scala._



/**
 * These tests are copied from [[AggregateITCase]] replacing calls to aggregate with calls to sum,
 * min, and max
 */
object SumMinMaxProgs {
  var NUM_PROGRAMS: Int = 3

  def runProgram(progId: Int, resultPath: String): String = {
    progId match {
      case 1 =>
        // Full aggregate
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.setDegreeOfParallelism(10)
        //        val ds = CollectionDataSets.get3TupleDataSet(env)
        val ds = CollectionDataSets.get3TupleDataSet(env)

        val aggregateDs = ds
          .sum(0)
          .andMax(1)
          // Ensure aggregate operator correctly copies other fields
          .filter(_._3 != null)
          .map{ t => (t._1, t._2) }

        aggregateDs.writeAsCsv(resultPath)

        env.execute()

        // return expected result
        "231,6\n"

      case 2 =>
        // Grouped aggregate
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env)

        val aggregateDs = ds
          .groupBy(1)
          .sum(0)
          // Ensure aggregate operator correctly copies other fields
          .filter(_._3 != null)
          .map { t => (t._2, t._1) }

        aggregateDs.writeAsCsv(resultPath)

        env.execute()

        // return expected result
        "1,1\n" + "2,5\n" + "3,15\n" + "4,34\n" + "5,65\n" + "6,111\n"

      case 3 =>
        // Nested aggregate
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env)

        val aggregateDs = ds
          .groupBy(1)
          .min(0)
          .min(0)
          // Ensure aggregate operator correctly copies other fields
          .filter(_._3 != null)
          .map { t => new Tuple1(t._1) }

        aggregateDs.writeAsCsv(resultPath)

        env.execute()

        // return expected result
        "1\n"


      case _ =>
        throw new IllegalArgumentException("Invalid program id")
    }
  }
}


@RunWith(classOf[Parameterized])
class SumMinMaxITCase(config: Configuration) extends JavaProgramTestBase(config) {

  private var curProgId: Int = config.getInteger("ProgramId", -1)
  private var resultPath: String = null
  private var expectedResult: String = null

  protected override def preSubmit(): Unit = {
    resultPath = getTempDirPath("result")
  }

  protected def testProgram(): Unit = {
    expectedResult = SumMinMaxProgs.runProgram(curProgId, resultPath)
  }

  protected override def postSubmit(): Unit = {
    compareResultsByLinesInMemory(expectedResult, resultPath)
  }
}

object SumMinMaxITCase {
  @Parameters
  def getConfigurations: java.util.Collection[Array[AnyRef]] = {
    val configs = mutable.MutableList[Array[AnyRef]]()
    for (i <- 1 to SumMinMaxProgs.NUM_PROGRAMS) {
      val config = new Configuration()
      config.setInteger("ProgramId", i)
      configs += Array(config)
    }

    configs.asJavaCollection
  }
}

