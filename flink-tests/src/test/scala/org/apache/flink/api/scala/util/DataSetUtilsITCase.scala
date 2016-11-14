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

import org.apache.flink.api.java.Utils.ChecksumHashCode
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils._
import org.apache.flink.test.util.MultipleProgramsTestBase
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
    val env = ExecutionEnvironment.getExecutionEnvironment

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
    val env = ExecutionEnvironment.getExecutionEnvironment

    val expectedSize = 100L

    val numbers = env.generateSequence(1L, expectedSize)

    val result = numbers.zipWithUniqueId.collect().map(_._1).toSet

    Assert.assertEquals(expectedSize, result.size)
  }

  @Test
  def testIntegerDataSetChecksumHashCode(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val ds = CollectionDataSets.getIntDataSet(env)

    val checksum: ChecksumHashCode = ds.checksumHashCode()
    Assert.assertEquals(checksum.getCount, 15)
    Assert.assertEquals(checksum.getChecksum, 55)
  }

  @Test
  @throws(classOf[Exception])
  def testCountElementsPerPartition(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val expectedSize = 100L

    val numbers = env.generateSequence(0, expectedSize - 1)

    val ds = numbers.countElementsPerPartition

    Assert.assertEquals(env.getParallelism, ds.collect().size)
    Assert.assertEquals(expectedSize, ds.sum(1).collect().head._2)
  }
}
