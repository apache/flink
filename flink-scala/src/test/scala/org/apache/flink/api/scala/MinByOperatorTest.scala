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

import org.junit.Test
import org.junit.Assert

class MinByOperatorTest {
  private val emptyTupleData = List[(Int, Long, String, Long, Int)]()
  private val customTypeData = List[CustomType]()

  @Test
  def testMinByKeyFieldsDataset(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val collection = env.fromCollection(emptyTupleData)
    try {
      collection.minBy(4, 0, 1, 2, 3)
    } catch {
      case e : Exception => Assert.fail();
    }
  }

  /**
    * This test validates that an index which is out of bounds throws an
    * IndexOutOfBoundsException.
    */
  @Test(expected = classOf[IndexOutOfBoundsException])
  def testOutOfTupleBoundsDataset1() {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val collection = env.fromCollection(emptyTupleData)

    // should not work, key out of tuple bounds
    collection.minBy(5)
  }

  /**
    * This test validates that an index which is out of bounds throws an
    * IndexOutOfBoundsException.
    */
  @Test(expected = classOf[IndexOutOfBoundsException])
  def testOutOfTupleBoundsDataset2() {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val collection = env.fromCollection(emptyTupleData)

    // should not work, key out of tuple bounds
    collection.minBy(-1)
  }

  /**
    * This test validates that an index which is out of bounds throws an
    * IndexOutOfBoundsException.
    */
  @Test(expected = classOf[IndexOutOfBoundsException])
  def testOutOfTupleBoundsDataset3() {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val collection = env.fromCollection(emptyTupleData)

    // should not work, key out of tuple bounds
    collection.minBy(1, 2, 3, 4, -1)
  }

  /**
    * This test validates that an InvalidProgramException is thrown when minBy
    * is used on a custom data type.
    */
  @Test(expected = classOf[InvalidProgramException])
  def testCustomKeyFieldsDataset() {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val customDS = env.fromCollection(customTypeData)
    // should not work: groups on custom type
    customDS.minBy(0)
  }

  /**
    * This test validates that no exceptions is thrown when an empty grouping
    * calls minBy().
    */
  @Test
  def testMinByKeyFieldsGrouping() {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val groupDs = env.fromCollection(emptyTupleData).groupBy(0)
    // should work
    try {
      groupDs.minBy(4, 0, 1, 2, 3)
    } catch {
      case e : Exception => Assert.fail()
    }
  }

  /**
    * This test validates that an InvalidProgramException is thrown when minBy
    * is used on a custom data type.
    */
  @Test(expected = classOf[InvalidProgramException])
  def testCustomKeyFieldsGrouping() {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val groupDs: GroupedDataSet[CustomType] = env.fromCollection(customTypeData).groupBy(0)

    groupDs.minBy(0)
  }

  /**
    * This test validates that an index which is out of bounds throws an
    * IndexOutOfBoundsException.
    */
  @Test(expected = classOf[IndexOutOfBoundsException])
  def testOutOfTupleBoundsGrouping1() {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val groupDs = env.fromCollection(emptyTupleData).groupBy(0)

    groupDs.minBy(5)
  }

  /**
    * This test validates that an index which is out of bounds throws an
    * IndexOutOfBoundsException.
    */
  @Test(expected = classOf[IndexOutOfBoundsException])
  def testOutOfTupleBoundsGrouping2() {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val groupDs = env.fromCollection(emptyTupleData).groupBy(0)

    groupDs.minBy(-1)
  }

  /**s
    * This test validates that an index which is out of bounds throws an
    * IndexOutOfBoundsException.
    */
  @Test(expected = classOf[IndexOutOfBoundsException])
  def testOutOfTupleBoundsGrouping3() {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val groupDs = env.fromCollection(emptyTupleData).groupBy(0)

    groupDs.minBy(1, 2, 3, 4, -1)
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
