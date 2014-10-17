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


object DistinctProgs {
  var NUM_PROGRAMS: Int = 8


  def runProgram(progId: Int, resultPath: String): String = {
    progId match {
      case 1 =>
        /*
         * Check correctness of distinct on tuples with key field selector
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getSmall3TupleDataSet(env)

        val distinctDs = ds.union(ds).distinct(0, 1, 2)
        distinctDs.writeAsCsv(resultPath)

        env.execute()

        // return expected result
        "1,1,Hi\n" +
          "2,2,Hello\n" +
          "3,2,Hello world\n"
        
      case 2 =>
        /*
         * check correctness of distinct on tuples with key field selector with not all fields
         * selected
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getSmall5TupleDataSet(env)

        val distinctDs = ds.union(ds).distinct(0).map(_._1)
        
        distinctDs.writeAsText(resultPath)
        env.execute()
        "1\n" + "2\n"
        
      case 3 =>
        /*
         * check correctness of distinct on tuples with key extractor
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getSmall5TupleDataSet(env)

        val reduceDs = ds.union(ds).distinct(_._1).map(_._1)

        reduceDs.writeAsText(resultPath)
        env.execute()
        "1\n" + "2\n"

      case 4 =>
        /*
         * check correctness of distinct on custom type with type extractor
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getCustomTypeDataSet(env)

        val reduceDs = ds.distinct(_.myInt).map( t => new Tuple1(t.myInt))

        reduceDs.writeAsCsv(resultPath)
        env.execute()
        "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n"

      case 5 =>
        /*
         * check correctness of distinct on tuples
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getSmall3TupleDataSet(env)

        val distinctDs = ds.union(ds).distinct()

        distinctDs.writeAsCsv(resultPath)
        env.execute()
        "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n"

      case 6 =>
        /*
         * check correctness of distinct on custom type with tuple-returning type extractor
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get5TupleDataSet(env)

        val reduceDs = ds.distinct( t => (t._1, t._5)).map( t => (t._1, t._5) )

        reduceDs.writeAsCsv(resultPath)
        env.execute()
        "1,1\n" + "2,1\n" + "2,2\n" + "3,2\n" + "3,3\n" + "4,1\n" + "4,2\n" + "5," +
          "1\n" + "5,2\n" + "5,3\n"

      case 7 =>
        /*
         * check correctness of distinct on tuples with field expressions
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getSmall5TupleDataSet(env)

        val reduceDs = ds.union(ds).distinct("_1").map(t => new Tuple1(t._1))

        reduceDs.writeAsCsv(resultPath)
        env.execute()
        "1\n" + "2\n"

      case 8 =>
        /*
         * check correctness of distinct on Pojos
         */

        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.getDuplicatePojoDataSet(env)

        val reduceDs = ds.distinct("nestedPojo.longNumber").map(_.nestedPojo.longNumber.toInt)

        reduceDs.writeAsText(resultPath)
        env.execute()
        "10000\n20000\n30000\n"

      case _ =>
        throw new IllegalArgumentException("Invalid program id")
    }
  }
}


@RunWith(classOf[Parameterized])
class DistinctITCase(config: Configuration) extends JavaProgramTestBase(config) {

  private var curProgId: Int = config.getInteger("ProgramId", -1)
  private var resultPath: String = null
  private var expectedResult: String = null

  protected override def preSubmit(): Unit = {
    resultPath = getTempDirPath("result")
  }

  protected def testProgram(): Unit = {
    expectedResult = DistinctProgs.runProgram(curProgId, resultPath)
  }

  protected override def postSubmit(): Unit = {
    compareResultsByLinesInMemory(expectedResult, resultPath)
  }
}

object DistinctITCase {
  @Parameters
  def getConfigurations: java.util.Collection[Array[AnyRef]] = {
    val configs = mutable.MutableList[Array[AnyRef]]()
    for (i <- 1 to DistinctProgs.NUM_PROGRAMS) {
      val config = new Configuration()
      config.setInteger("ProgramId", i)
      configs += Array(config)
    }

    configs.asJavaCollection
  }
}

