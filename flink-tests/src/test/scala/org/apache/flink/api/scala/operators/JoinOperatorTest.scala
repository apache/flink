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

import org.apache.flink.api.common.InvalidProgramException
import org.apache.flink.api.common.operators.Keys
import Keys.IncompatibleKeysException
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets.CustomType
import org.junit.{Assert, Test}

class JoinOperatorTest {

  private val emptyTupleData = Array[(Int, Long, String, Long, Int)]()
  private val customTypeData = Array[CustomType](new CustomType())
  private val emptyLongData = Array[Long]()

  @Test
  def testJoinKeyIndices1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyTupleData)

    // should work
    try {
      ds1.join(ds2).where(0).equalTo(0)
    }
    catch {
      case e: Exception => Assert.fail()
    }
  }

  @Test(expected = classOf[IncompatibleKeysException])
  def testJoinKeyIndices2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyTupleData)

    // should not work, incompatible key types
    ds1.join(ds2).where(0).equalTo(2)
  }

  @Test(expected = classOf[IncompatibleKeysException])
  def testJoinKeyIndices3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyTupleData)

    // should not work, non-matching number of key indices
    ds1.join(ds2).where(0, 1).equalTo(2)
  }

  @Test(expected = classOf[IndexOutOfBoundsException])
  def testJoinKeyIndices4(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyTupleData)

    // should not work, index out of range
    ds1.join(ds2).where(5).equalTo(0)
  }

  @Test(expected = classOf[IndexOutOfBoundsException])
  def testJoinKeyIndices5(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyTupleData)

    // should not work, negative position
    ds1.join(ds2).where(-1).equalTo(-1)
  }

  @Test(expected = classOf[InvalidProgramException])
  def testJoinKeyIndices6(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(customTypeData)

    // should not work, key index on custom type
    ds1.join(ds2).where(4).equalTo(0)
  }

  @Test
  def testJoinKeyFields1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyTupleData)

    // should work
    try {
      ds1.join(ds2).where("_1").equalTo("_1")
    }
    catch {
      case e: Exception => Assert.fail()
    }
  }

  @Test(expected = classOf[IncompatibleKeysException])
  def testJoinKeyFields2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyTupleData)

    // should not work, incompatible field types
    ds1.join(ds2).where("_1").equalTo("_3")
  }

  @Test(expected = classOf[IncompatibleKeysException])
  def testJoinKeyFields3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyTupleData)

    // should not work, non-matching number of key indices

    ds1.join(ds2).where("_1", "_2").equalTo("_3")
  }

  @Test(expected = classOf[RuntimeException])
  def testJoinKeyFields4(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyTupleData)

    // should not work, non-existent key
    ds1.join(ds2).where("foo").equalTo("_1")
  }

  @Test(expected = classOf[RuntimeException])
  def testJoinKeyFields5(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyTupleData)

    // should not work, non-matching number of key indices
    ds1.join(ds2).where("_1").equalTo("bar")
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testJoinKeyFields6(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(customTypeData)

    // should not work, field key on custom type
    ds1.join(ds2).where("_2").equalTo("_1")
  }

  @Test
  def testJoinKeyExpressions1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(customTypeData)
    val ds2 = env.fromCollection(customTypeData)

    // should work
    try {
      ds1.join(ds2).where("myInt").equalTo("myInt")
    }
    catch {
      case e: Exception => Assert.fail()
    }
  }

  @Test(expected = classOf[IncompatibleKeysException])
  def testJoinKeyExpressions2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(customTypeData)
    val ds2 = env.fromCollection(customTypeData)

    // should not work, incompatible join key types
    ds1.join(ds2).where("myInt").equalTo("myString")
  }

  @Test(expected = classOf[IncompatibleKeysException])
  def testJoinKeyExpressions3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(customTypeData)
    val ds2 = env.fromCollection(customTypeData)

    // should not work, incompatible number of keys
    ds1.join(ds2).where("myInt", "myString").equalTo("myInt")
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testJoinKeyExpressions4(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(customTypeData)
    val ds2 = env.fromCollection(customTypeData)

    // should not work, join key non-existent
    ds1.join(ds2).where("myNonExistent").equalTo("i")
  }

  @Test
  def testJoinKeySelectors1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(customTypeData)
    val ds2 = env.fromCollection(customTypeData)

    // should work
    try {
      ds1.join(ds2).where { _.myLong} equalTo { _.myLong }
    }
    catch {
      case e: Exception => Assert.fail()
    }
  }

  @Test
  def testJoinKeyMixing1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(customTypeData)
    val ds2 = env.fromCollection(emptyTupleData)

    // should work
    try {
      ds1.join(ds2).where { _.myLong }.equalTo(3)
    }
    catch {
      case e: Exception => Assert.fail()
    }
  }

  @Test
  def testJoinKeyMixing2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(customTypeData)

    // should work
    try {
      ds1.join(ds2).where(3).equalTo { _.myLong }
    }
    catch {
      case e: Exception => Assert.fail()
    }
  }

  @Test(expected = classOf[IncompatibleKeysException])
  def testJoinKeyMixing3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(customTypeData)

    // should not work, incompatible types
    ds1.join(ds2).where(2).equalTo { _.myLong }
  }

  @Test(expected = classOf[IncompatibleKeysException])
  def testJoinKeyMixing4(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(customTypeData)

    // should not work, more than one field position key
    ds1.join(ds2).where(1, 3) equalTo { _.myLong }
  }

  @Test
  def testJoinWithAtomic(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyLongData)

    ds1.join(ds2).where(1).equalTo("*")
  }

  @Test(expected = classOf[InvalidProgramException])
  def testJoinWithInvalidAtomic1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyLongData)

    ds1.join(ds2).where(1).equalTo("invalidKey")
  }

  @Test(expected = classOf[InvalidProgramException])
  def testJoinWithInvalidAtomic2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyLongData)
    val ds2 = env.fromCollection(emptyTupleData)

    ds1.join(ds2).where("invalidKey").equalTo(1)
  }

  @Test(expected = classOf[InvalidProgramException])
  def testJoinWithInvalidAtomic3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyLongData)

    ds1.join(ds2).where(1).equalTo("_", "invalidKey")
  }

  @Test(expected = classOf[InvalidProgramException])
  def testJoinWithInvalidAtomic4(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyLongData)
    val ds2 = env.fromCollection(emptyTupleData)

    ds1.join(ds2).where("_", "invalidKey").equalTo(1)
  }

  @Test(expected = classOf[InvalidProgramException])
  def testJoinWithInvalidAtomic5(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(new util.ArrayList[Integer]())
    val ds2 = env.fromCollection(emptyLongData)

    ds1.join(ds2).where("*")
  }

  @Test(expected = classOf[InvalidProgramException])
  def testJoinWithInvalidAtomic6(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyLongData)
    val ds2 = env.fromElements(new util.ArrayList[Integer]())

    ds1.join(ds2).where("*").equalTo("*")
  }
}

