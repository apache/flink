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

import org.apache.flink.api.common.functions.{RichFilterFunction, RichMapFunction}
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


object PartitionProgs {
  var NUM_PROGRAMS: Int = 7

  def runProgram(progId: Int, resultPath: String, onCollection: Boolean): String = {
    progId match {
      case 1 =>
        /*
         * Test hash partition by tuple field
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env)

        val unique = ds.partitionByHash(1).mapPartition( _.map(_._2).toSet )

        unique.writeAsText(resultPath)
        env.execute()

        "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n"

      case 2 =>
        /*
         * Test hash partition by key selector
         */
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env)
        val unique = ds.partitionByHash( _._2 ).mapPartition( _.map(_._2).toSet )

        unique.writeAsText(resultPath)
        env.execute()
        "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n"

      case 3 =>
        /*
         * Test forced rebalancing
         */
      val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = env.generateSequence(1, 3000)

        val skewed = ds.filter(_ > 780)
        val rebalanced = skewed.rebalance()

        val countsInPartition = rebalanced.map( new RichMapFunction[Long, (Int, Long)] {
          def map(in: Long) = {
            (getRuntimeContext.getIndexOfThisSubtask, 1)
          }
        })
          .groupBy(0)
          .reduce { (v1, v2) => (v1._1, v1._2 + v2._2) }
          // round counts to mitigate runtime scheduling effects (lazy split assignment)
          .map { in => (in._1, in._2 / 10) }

        countsInPartition.writeAsText(resultPath)
        env.execute()

        val numPerPartition : Int = 2220 / env.getDegreeOfParallelism / 10
        var result = ""
        for (i <- 0 until env.getDegreeOfParallelism) {
          result += "(" + i + "," + numPerPartition + ")\n"
        }
        result

      case 4 =>
        // Verify that mapPartition operation after repartition picks up correct
        // DOP
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env)
        env.setDegreeOfParallelism(1)

        val unique = ds.partitionByHash(1)
          .setParallelism(4)
          .mapPartition( _.map(_._2).toSet )

        unique.writeAsText(resultPath)
        env.execute()

        "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n"

      case 5 =>
        // Verify that map operation after repartition picks up correct
        // DOP
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env)
        env.setDegreeOfParallelism(1)

        val count = ds.partitionByHash(0).setParallelism(4).map(
          new RichMapFunction[(Int, Long, String), Tuple1[Int]] {
            var first = true
            override def map(in: (Int, Long, String)): Tuple1[Int] = {
              // only output one value with count 1
              if (first) {
                first = false
                Tuple1(1)
              } else {
                Tuple1(0)
              }
            }
          }).sum(0)

        count.writeAsText(resultPath)
        env.execute()

        if (onCollection) "(1)\n" else "(4)\n"

      case 6 =>
        // Verify that filter operation after repartition picks up correct
        // DOP
        val env = ExecutionEnvironment.getExecutionEnvironment
        val ds = CollectionDataSets.get3TupleDataSet(env)
        env.setDegreeOfParallelism(1)

        val count = ds.partitionByHash(0).setParallelism(4).filter(
          new RichFilterFunction[(Int, Long, String)] {
            var first = true
            override def filter(in: (Int, Long, String)): Boolean = {
              // only output one value with count 1
              if (first) {
                first = false
                true
              } else {
                false
              }
            }
        })
          .map( _ => Tuple1(1)).sum(0)

        count.writeAsText(resultPath)
        env.execute()

        if (onCollection) "(1)\n" else "(4)\n"

      case 7 =>
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.setDegreeOfParallelism(3)
        val ds = CollectionDataSets.getDuplicatePojoDataSet(env)
        val uniqLongs = ds
          .partitionByHash("nestedPojo.longNumber")
          .setParallelism(4)
          .mapPartition( _.map(_.nestedPojo.longNumber).toSet )

        uniqLongs.writeAsText(resultPath)
        env.execute()
        "10000\n" + "20000\n" + "30000\n"

      case _ =>
        throw new IllegalArgumentException("Invalid program id")
    }
  }
}


@RunWith(classOf[Parameterized])
class PartitionITCase(config: Configuration) extends JavaProgramTestBase(config) {

  private var curProgId: Int = config.getInteger("ProgramId", -1)
  private var resultPath: String = null
  private var expectedResult: String = null

  protected override def preSubmit(): Unit = {
    resultPath = getTempDirPath("result")
  }

  protected def testProgram(): Unit = {
    expectedResult = PartitionProgs.runProgram(curProgId, resultPath, isCollectionExecution)
  }

  protected override def postSubmit(): Unit = {
    compareResultsByLinesInMemory(expectedResult, resultPath)
  }
}

object PartitionITCase {
  @Parameters
  def getConfigurations: java.util.Collection[Array[AnyRef]] = {
    val configs = mutable.MutableList[Array[AnyRef]]()
    for (i <- 1 to PartitionProgs.NUM_PROGRAMS) {
      val config = new Configuration()
      config.setInteger("ProgramId", i)
      configs += Array(config)
    }

    configs.asJavaCollection
  }
}

