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
package org.apache.flink.api.scala

import org.apache.flink.api.common.InvalidProgramException

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class MinByOperatorTest {
  private val emptyTupleData = List[(Int, Long, String, Long, Int)]()
  private val customTypeData = List[CustomType]()

  @Test
  def testMinByKeyFieldsDataset(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val collection = env.fromCollection(emptyTupleData)
    collection.minBy(4, 0, 1, 2, 3)
  }

  /**
   * This test validates that an index which is out of bounds throws an IndexOutOfBoundsException.
   */
  @Test
  def testOutOfTupleBoundsDataset1(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val collection = env.fromCollection(emptyTupleData)

    // should not work, key out of tuple bounds
    assertThatThrownBy(() => collection.minBy(5))
      .isInstanceOf(classOf[IndexOutOfBoundsException])
  }

  /**
   * This test validates that an index which is out of bounds throws an IndexOutOfBoundsException.
   */
  @Test
  def testOutOfTupleBoundsDataset2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val collection = env.fromCollection(emptyTupleData)

    // should not work, key out of tuple bounds
    assertThatThrownBy(() => collection.minBy(-1))
      .isInstanceOf(classOf[IndexOutOfBoundsException])
  }

  /**
   * This test validates that an index which is out of bounds throws an IndexOutOfBoundsException.
   */
  @Test
  def testOutOfTupleBoundsDataset3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val collection = env.fromCollection(emptyTupleData)

    // should not work, key out of tuple bounds
    assertThatThrownBy(() => collection.minBy(1, 2, 3, 4, -1))
      .isInstanceOf(classOf[IndexOutOfBoundsException])
  }

  /**
   * This test validates that an InvalidProgramException is thrown when minBy is used on a custom
   * data type.
   */
  @Test
  def testCustomKeyFieldsDataset(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val customDS = env.fromCollection(customTypeData)
    // should not work: groups on custom type
    assertThatThrownBy(() => customDS.minBy(0))
      .isInstanceOf(classOf[InvalidProgramException])
  }

  /** This test validates that no exceptions is thrown when an empty grouping calls minBy(). */
  @Test
  def testMinByKeyFieldsGrouping(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val groupDs = env.fromCollection(emptyTupleData).groupBy(0)
    // should work
    groupDs.minBy(4, 0, 1, 2, 3)
  }

  /**
   * This test validates that an InvalidProgramException is thrown when minBy is used on a custom
   * data type.
   */
  @Test
  def testCustomKeyFieldsGrouping(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    assertThatThrownBy(
      () => {
        val groupDs: GroupedDataSet[CustomType] = env.fromCollection(customTypeData).groupBy(0)

        groupDs.minBy(0)
      })
      .isInstanceOf(classOf[InvalidProgramException])
  }

  /**
   * This test validates that an index which is out of bounds throws an IndexOutOfBoundsException.
   */
  @Test
  def testOutOfTupleBoundsGrouping1(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val groupDs = env.fromCollection(emptyTupleData).groupBy(0)

    assertThatThrownBy(() => groupDs.minBy(5))
      .isInstanceOf(classOf[IndexOutOfBoundsException])
  }

  /**
   * This test validates that an index which is out of bounds throws an IndexOutOfBoundsException.
   */
  @Test
  def testOutOfTupleBoundsGrouping2(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val groupDs = env.fromCollection(emptyTupleData).groupBy(0)

    assertThatThrownBy(() => groupDs.minBy(-1))
      .isInstanceOf(classOf[IndexOutOfBoundsException])
  }

  /**
   * s This test validates that an index which is out of bounds throws an IndexOutOfBoundsException.
   */
  @Test
  def testOutOfTupleBoundsGrouping3(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val groupDs = env.fromCollection(emptyTupleData).groupBy(0)

    assertThatThrownBy(() => groupDs.minBy(1, 2, 3, 4, -1))
      .isInstanceOf(classOf[IndexOutOfBoundsException])
  }

  class CustomType(var myInt: Int, var myLong: Long, var myString: String) {
    def this() {
      this(0, 0, "")
    }

    override def toString: String = {
      myInt + "," + myLong + "," + myString
    }
  }
}
