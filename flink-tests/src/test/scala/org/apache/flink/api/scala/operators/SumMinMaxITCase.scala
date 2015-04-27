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

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.MultipleProgramsTestBase

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.Assert._

import org.apache.flink.api.scala._

@RunWith(classOf[Parameterized])
class SumMinMaxITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  @Test
  def testFullAggregate(): Unit = {
    // Full aggregate
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)

    val aggregateDs : DataSet[(Int, Long)] = ds
      .sum(0)
      .andMax(1)
      // Ensure aggregate operator correctly copies other fields
      .filter(_._3 != null)
      .map{ t => (t._1, t._2) }


    val result: Seq[(Int, Long)] = aggregateDs.collect()

    assertEquals(1, result.size)
    assertEquals((231, 6L), result.head)
  }

  @Test
  def testGroupedAggregate(): Unit = {
    // Grouped aggregate
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)

    val aggregateDs = ds
      .groupBy(1)
      .sum(0)
      // Ensure aggregate operator correctly copies other fields
      .filter(_._3 != null)
      .map { t => (t._2, t._1) }

    val result : Seq[(Long, Int)] = aggregateDs.collect().sortWith((a, b) => a._1 < b._1)

    val expected : Seq[(Long, Int)] = Seq((1L, 1), (2L, 5), (3L, 15), (4L, 34), (5L, 65), (6L, 111))
    assertEquals(expected, result)
  }

  @Test
  def testNestedAggregate(): Unit = {
    // Nested aggregate
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)

    val aggregateDs: DataSet[Int] = ds
      .groupBy(1)
      .min(0)
      .min(0)
      // Ensure aggregate operator correctly copies other fields
      .filter(_._3 != null)
      .map { t => t._1 }

    val result: Seq[Int] = aggregateDs.collect()

    assertEquals(1, result.size)
    assertEquals(Seq(1), result)
  }
}
