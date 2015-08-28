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

package org.apache.flink.api.scala.actions

import org.apache.flink.api.scala._
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.MultipleProgramsTestBase

import org.junit.Test
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class CountCollectITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  @Test
  def testCountCollectOnSimpleJob(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val input = 1 to 10

    val inputDS = env.fromElements(input: _*)

    // count
    val numEntries = inputDS.count()
    assertEquals(input.length, numEntries)

    // collect
    val list = inputDS.collect()
    assertArrayEquals(input.toArray, list.toArray)
  }

  @Test
  def testCountCollectOnAdvancedJob(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableObjectReuse()

    val input1 = 1 to 10
    val input2 = 1 to 10

    val inputDS1 = env.fromElements(input1:_*)
    val inputDS2 = env.fromElements(input2:_*)

    val result = inputDS1 cross inputDS2

    val numEntries = result.count()
    assertEquals(input1.length * input2.length, numEntries)

    val list = result.collect()

    val marker = Array.fill(input1.length, input2.length)(false)

    for((x,y) <- list) {
      assertFalse(s"Element ($x,$y) seen twice.", marker(x-1)(y-1))
      marker(x-1)(y-1) = true
    }
  }
}
