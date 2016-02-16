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

import org.apache.flink.api.common.functions.{RichFilterFunction, RichMapFunction}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{TestBaseUtils, MultipleProgramsTestBase}
import org.junit.{Test, After, Before, Rule}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import org.apache.flink.api.scala._

@RunWith(classOf[Parameterized])
class PartitionITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
  private var resultPath: String = null
  private var expected: String = null
  private val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder = _tempFolder

  @Before
  def before(): Unit = {
    resultPath = tempFolder.newFile().toURI.toString
  }

  @After
  def after(): Unit = {
    TestBaseUtils.compareResultsByLinesInMemory(expected, resultPath)
  }

  @Test
  def testEmptyHashPartition(): Unit = {
    /*
     * Test hash partition by tuple field
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromCollection(Seq[Tuple1[String]]())

    val unique = ds.partitionByHash(0)

    unique.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()

    expected = ""
  }

  @Test
  def testEmptyRangePartition(): Unit = {
    /*
     * Test hash partition by tuple field
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromCollection(Seq[Tuple1[String]]())

    val unique = ds.partitionByRange(0)

    unique.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()

    expected = ""
  }

  @Test
  def testHashPartitionByTupleField(): Unit = {
    /*
     * Test hash partition by tuple field
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)

    val unique = ds.partitionByHash(1).mapPartition( _.map(_._2).toSet )

    unique.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()

    expected = "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n"
  }

  @Test
  def testRangePartitionByTupleField(): Unit = {
    /*
     * Test hash partition by tuple field
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)

    val unique = ds.partitionByRange(1).mapPartition( _.map(_._2).toSet )

    unique.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()

    expected = "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n"
  }

  @Test
  def testHashPartitionByKeySelector(): Unit = {
    /*
     * Test hash partition by key selector
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val unique = ds.partitionByHash( _._2 ).mapPartition( _.map(_._2).toSet )

    unique.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n"
  }

  @Test
  def testRangePartitionByKeySelector(): Unit = {
    /*
     * Test hash partition by key selector
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val unique = ds.partitionByRange( _._2 ).mapPartition( _.map(_._2).toSet )

    unique.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n"
  }

  @Test
  def testForcedRebalancing(): Unit = {
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

    countsInPartition.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()

    val numPerPartition : Int = 2220 / env.getParallelism / 10
    expected = ""
    for (i <- 0 until env.getParallelism) {
      expected += "(" + i + "," + numPerPartition + ")\n"
    }
  }

  @Test
  def testMapPartitionAfterRepartitionHasCorrectParallelism(): Unit = {
    // Verify that mapPartition operation after repartition picks up correct
    // parallelism
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    env.setParallelism(1)

    val unique = ds.partitionByHash(1)
      .setParallelism(4)
      .mapPartition( _.map(_._2).toSet )

    unique.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()

    expected = "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n"
  }

  @Test
  def testMapPartitionAfterRepartitionHasCorrectParallelism2(): Unit = {
    // Verify that mapPartition operation after repartition picks up correct
    // parallelism
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    env.setParallelism(1)

    val unique = ds.partitionByRange(1)
      .setParallelism(4)
      .mapPartition( _.map(_._2).toSet )

    unique.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()

    expected = "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n"
  }

  @Test
  def testMapAfterRepartitionHasCorrectParallelism(): Unit = {
    // Verify that map operation after repartition picks up correct
    // parallelism
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    env.setParallelism(1)

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

    count.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()

    expected = if (mode == TestExecutionMode.COLLECTION) "(1)\n" else "(4)\n"
  }

  @Test
  def testMapAfterRepartitionHasCorrectParallelism2(): Unit = {
    // Verify that map operation after repartition picks up correct
    // parallelism
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    env.setParallelism(1)

    val count = ds.partitionByRange(0).setParallelism(4).map(
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

    count.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()

    expected = if (mode == TestExecutionMode.COLLECTION) "(1)\n" else "(4)\n"
  }


  @Test
  def testFilterAfterRepartitionHasCorrectParallelism(): Unit = {
    // Verify that filter operation after repartition picks up correct
    // parallelism
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    env.setParallelism(1)

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

    count.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()

    expected = if (mode == TestExecutionMode.COLLECTION) "(1)\n" else "(4)\n"
  }

  @Test
  def testFilterAfterRepartitionHasCorrectParallelism2(): Unit = {
    // Verify that filter operation after repartition picks up correct
    // parallelism
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    env.setParallelism(1)

    val count = ds.partitionByRange(0).setParallelism(4).filter(
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

    count.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()

    expected = if (mode == TestExecutionMode.COLLECTION) "(1)\n" else "(4)\n"
  }

  @Test
  def testHashPartitionNestedPojo(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val ds = CollectionDataSets.getDuplicatePojoDataSet(env)
    val uniqLongs = ds
      .partitionByHash("nestedPojo.longNumber")
      .setParallelism(4)
      .mapPartition( _.map(_.nestedPojo.longNumber).toSet )

    uniqLongs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "10000\n" + "20000\n" + "30000\n"
  }

  @Test
  def testRangePartitionNestedPojo(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val ds = CollectionDataSets.getDuplicatePojoDataSet(env)
    val uniqLongs = ds
      .partitionByRange("nestedPojo.longNumber")
      .setParallelism(4)
      .mapPartition( _.map(_.nestedPojo.longNumber).toSet )

    uniqLongs.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "10000\n" + "20000\n" + "30000\n"
  }
}
