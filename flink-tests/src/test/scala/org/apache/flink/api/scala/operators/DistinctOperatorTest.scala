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

import org.apache.flink.api.scala.util.CollectionDataSets.CustomType
import org.junit.Assert
import org.apache.flink.api.common.InvalidProgramException
import org.junit.Test

import org.apache.flink.api.scala._

class DistinctOperatorTest {

  private val emptyTupleData = Array[(Int, Long, String, Long, Int)]()
  private val customTypeData = Array[CustomType](new CustomType())
  private val emptyLongData = Array[Long]()

  @Test
  def testDistinctByKeyIndices1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tupleDs = env.fromCollection(emptyTupleData)

    // Should work
    try {
      tupleDs.distinct(0)
    }
    catch {
      case e: Exception => Assert.fail()
    }
  }

  @Test(expected = classOf[InvalidProgramException])
  def testDistinctByKeyIndices2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val longDs = env.fromCollection(emptyLongData)

    // should not work: distinct on basic type
    longDs.distinct(0)
  }

  @Test(expected = classOf[InvalidProgramException])
  def testDistinctByKeyIndices3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val customDs = env.fromCollection(customTypeData)

    // should not work: field position key on custom type
    customDs.distinct(0)
  }

  @Test
  def testDistinctByKeyIndices4(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tupleDs = env.fromCollection(emptyTupleData)

    // should work
    tupleDs.distinct()
  }

  @Test(expected = classOf[IndexOutOfBoundsException])
  def testDistinctByKeyIndices6(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tupleDs = env.fromCollection(emptyTupleData)

    // should not work, negative field position key
    tupleDs.distinct(-1)
  }

  @Test
  def testDistinctByKeyIndices7(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val longDs = env.fromCollection(emptyLongData)

    // should work
    try {
      longDs.distinct()
    } catch {
      case e: Exception => Assert.fail()
    }
  }

  @Test
  def testDistinctByKeyFields1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tupleDs = env.fromCollection(emptyTupleData)

    // Should work
    try {
      tupleDs.distinct("_1")
    }
    catch {
      case e: Exception => Assert.fail()
    }
  }

  @Test(expected = classOf[RuntimeException])
  def testDistinctByKeyFields2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val longDs = env.fromCollection(emptyLongData)

    // should not work: distinct on basic type
    longDs.distinct("_1")
  }

  @Test(expected = classOf[RuntimeException])
  def testDistinctByKeyFields3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val customDs = env.fromCollection(customTypeData)

    // should not work: invalid fields
    customDs.distinct("_1")
  }

  @Test(expected = classOf[RuntimeException])
  def testDistinctByKeyFields4(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tupleDs = env.fromCollection(emptyTupleData)

    // should not work, invalid field
    tupleDs.distinct("foo")
  }

  @Test
  def testDistinctByKeyFields5(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val customDs = env.fromCollection(customTypeData)

    // should work
    customDs.distinct("myInt")
  }

  @Test
  def testDistinctByKeyFields6(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val longDs = env.fromCollection(emptyLongData)

    // should work
    try {
      longDs.distinct("_")
    } catch {
      case e: Exception => Assert.fail()
    }
  }

  @Test
  def testDistinctByKeySelector1(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    try {
      val customDs = env.fromCollection(customTypeData)
      customDs.distinct {_.myLong}
    }
    catch {
      case e: Exception => Assert.fail()
    }
  }
}

