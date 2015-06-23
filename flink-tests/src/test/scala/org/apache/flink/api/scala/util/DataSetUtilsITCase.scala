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

package org.apache.flink.api.scala.util

import org.apache.flink.api.common.accumulators.{ContinuousHistogram, DiscreteHistogram}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils._
import org.apache.flink.test.util.MultipleProgramsTestBase
import org.junit.Assert.assertEquals
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class DataSetUtilsITCase (
    mode: MultipleProgramsTestBase.TestExecutionMode)
  extends MultipleProgramsTestBase(mode) {

  @Test
  @throws(classOf[Exception])
  def testZipWithIndex(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val expectedSize = 100L

    val numbers = env.generateSequence(0, expectedSize - 1)

    val result = numbers.zipWithIndex.collect()

    Assert.assertEquals(expectedSize, result.size)

    for( ((index, _), expected) <- result.sortBy(_._1).zipWithIndex) {
      Assert.assertEquals(expected, index)
    }
  }

  @Test
  @throws(classOf[Exception])
  def testZipWithUniqueId(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val expectedSize = 100L

    val numbers = env.generateSequence(1L, expectedSize)

    val result = numbers.zipWithUniqueId.collect().map(_._1).toSet

    Assert.assertEquals(expectedSize, result.size)
  }

  @Test
  @throws(classOf[Exception])
  def testHistogram(): Unit = {
    val data: DataSet[Double] = ExecutionEnvironment.getExecutionEnvironment.fromElements(1.0, 2.0,
      3.0, 5.0, 1.0, 7.0, 9.0, 1.0, 0.0, 1.0, 4.0, 6.0, 7.0, 9.0, 4.0, 3.0, 1.0, 4.0, 6.0, 8.0, 4.0,
      3.0, 6.0, 8.0, 4.0, 3.0, 6.0, 8.0, 9.0, 7.0, 8.0, 2.0, 3.0, 6.0, 0.0)
      .setParallelism(2)

    val histogram1: DiscreteHistogram = data.createDiscreteHistogram.collect().head
    assertEquals(35, histogram1.getTotal)
    assertEquals(10, histogram1.getSize)
    assertEquals(2, histogram1.count(0.0))
    assertEquals(5, histogram1.count(1.0))
    assertEquals(2, histogram1.count(2.0))
    assertEquals(5, histogram1.count(3.0))
    assertEquals(5, histogram1.count(4.0))
    assertEquals(1, histogram1.count(5.0))
    assertEquals(5, histogram1.count(6.0))
    assertEquals(3, histogram1.count(7.0))
    assertEquals(4, histogram1.count(8.0))
    assertEquals(3, histogram1.count(9.0))

    val histogram2: ContinuousHistogram = data.createContinuousHistogram(5).collect().head
    assertEquals(35, histogram2.getTotal)
    assertEquals(5, histogram2.getSize)
    assertEquals(4, histogram2.count(histogram2.quantile(0.1)))
    assertEquals(7, histogram2.count(histogram2.quantile(0.2)))
    assertEquals(18, histogram2.count(histogram2.quantile(0.5)))
    assertEquals(0.0, histogram2.min, 0.0)
    assertEquals(9.0, histogram2.max, 0.0)
    assertEquals(4.543, histogram2.mean, 1e-3)
    assertEquals(7.619, histogram2.variance, 1e-3)
  }
}
