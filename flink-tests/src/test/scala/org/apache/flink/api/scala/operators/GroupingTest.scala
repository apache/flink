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

import java.util

import org.apache.flink.api.scala.util.CollectionDataSets.CustomType
import org.junit.Assert
import org.apache.flink.api.common.InvalidProgramException
import org.apache.flink.api.common.operators.Order
import org.junit.Test

import org.apache.flink.api.scala._


class GroupingTest {

  private val emptyTupleData = Array[(Int, Long, String, Long, Int)]()
  private val customTypeData = Array[CustomType](new CustomType())
  private val emptyLongData = Array[Long]()

  @Test
  def testGroupByKeyIndices1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tupleDs = env.fromCollection(emptyTupleData)

    // should work
    try {
      tupleDs.groupBy(0)
    }
    catch {
      case e: Exception => Assert.fail()
    }
  }

  @Test(expected = classOf[InvalidProgramException])
  def testGroupByKeyIndices2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val longDs = env.fromCollection(emptyLongData)

    // should not work, grouping on basic type
    longDs.groupBy(0)
  }

  @Test(expected = classOf[InvalidProgramException])
  def testGroupByKeyIndices3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val customDs = env.fromCollection(customTypeData)

    // should not work, field position key on custom type
    customDs.groupBy(0)
  }

  @Test(expected = classOf[IndexOutOfBoundsException])
  def testGroupByKeyIndices4(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tupleDs = env.fromCollection(emptyTupleData)

    // should not work, field position out of range
    tupleDs.groupBy(5)
  }

  @Test(expected = classOf[IndexOutOfBoundsException])
  def testGroupByKeyIndices5(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tupleDs = env.fromCollection(emptyTupleData)

    // should not work, negative field position
    tupleDs.groupBy(-1)
  }

  @Test
  def testGroupByKeyFields1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tupleDs = env.fromCollection(emptyTupleData)

    // should work
    try {
      tupleDs.groupBy("_1")
    }
    catch {
      case e: Exception => Assert.fail()
    }
  }

  @Test(expected = classOf[InvalidProgramException])
  def testGroupByKeyFields2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val longDs = env.fromCollection(emptyLongData)

    // should not work, grouping on basic type
    longDs.groupBy("_1")
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testGroupByKeyFields3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val customDs = env.fromCollection(customTypeData)

    // should not work, field key on custom type
    customDs.groupBy("_1")
  }

  @Test(expected = classOf[RuntimeException])
  def testGroupByKeyFields4(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tupleDs = env.fromCollection(emptyTupleData)

    // should not work, invalid field
    tupleDs.groupBy("foo")
  }

  @Test
  def testGroupByKeyFields5(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val customDs = env.fromCollection(customTypeData)

    // should not work
    customDs.groupBy("myInt")
  }

  @Test
  def testGroupByKeyExpressions1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromCollection(customTypeData)

    // should work
    try {
      ds.groupBy("myInt")
    }
    catch {
      case e: Exception => Assert.fail()
    }
  }

  @Test(expected = classOf[InvalidProgramException])
  def testGroupByKeyExpressions2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // should not work: groups on basic type
    val longDs = env.fromCollection(emptyLongData)
    longDs.groupBy("l")
  }

  @Test(expected = classOf[InvalidProgramException])
  def testGroupByKeyExpressions3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val customDs = env.fromCollection(customTypeData)

    // should not work: groups on custom type
    customDs.groupBy(0)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testGroupByKeyExpressions4(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromCollection(customTypeData)

    // should not work, non-existent field
    ds.groupBy("myNonExistent")
  }

  @Test
  def testGroupByKeySelector1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    try {
      val customDs = env.fromCollection(customTypeData)
      customDs.groupBy { _.myLong }
    }
    catch {
      case e: Exception => Assert.fail()
    }
  }

  @Test
  def testGroupSortKeyFields1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tupleDs = env.fromCollection(emptyTupleData)
    try {
      tupleDs.groupBy(0).sortGroup(0, Order.ASCENDING)
    }
    catch {
      case e: Exception => Assert.fail()
    }
  }

  @Test(expected = classOf[IndexOutOfBoundsException])
  def testGroupSortKeyFields2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tupleDs = env.fromCollection(emptyTupleData)

    // should not work, field position out of range
    tupleDs.groupBy(0).sortGroup(5, Order.ASCENDING)
  }

  @Test(expected = classOf[InvalidProgramException])
  def testGroupSortKeyFields3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val longDs = env.fromCollection(emptyLongData)
    longDs.groupBy { x: Long => x } .sortGroup(0, Order.ASCENDING)
  }

  @Test
  def testChainedGroupSortKeyFields(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tupleDs = env.fromCollection(emptyTupleData)
    try {
      tupleDs.groupBy(0).sortGroup(0, Order.ASCENDING).sortGroup(2, Order.DESCENDING)
    }
    catch {
      case e: Exception => Assert.fail()
    }
  }

  @Test
  def testAtomicValue1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(0, 1, 2)

    ds.groupBy("*")
  }

  @Test(expected = classOf[InvalidProgramException])
  def testAtomicValueInvalid1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(0, 1, 2)

    ds.groupBy("invalidKey")
  }

  @Test(expected = classOf[InvalidProgramException])
  def testAtomicValueInvalid2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(0, 1, 2)

    ds.groupBy("_", "invalidKey")
  }

  @Test(expected = classOf[InvalidProgramException])
  def testAtomicValueInvalid3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(new util.ArrayList[Integer]())

    ds.groupBy("*")
  }
}

