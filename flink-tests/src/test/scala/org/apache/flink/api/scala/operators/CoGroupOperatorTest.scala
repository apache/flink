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

import org.apache.flink.api.common.InvalidProgramException
import org.apache.flink.api.common.operators.Keys.IncompatibleKeysException
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets.CustomType
import org.junit.jupiter.api.{Assertions, Test}
import org.scalatest.Matchers.assertThrows

import java.util

class CoGroupOperatorTest {

  private val emptyTupleData = Array[(Int, Long, String, Long, Int)]()
  private var customTypeData = Array[CustomType](new CustomType())

  @Test
  def testCoGroupKeyFields1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyTupleData)

    // Should work
    try {
      ds1.coGroup(ds2).where(0).equalTo(0)
    }
    catch {
      case e: Exception => Assertions.fail()
    }
  }

  @Test
  def testCoGroupKeyFields2(): Unit = {
    assertThrows[IncompatibleKeysException] {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromCollection(emptyTupleData)
      val ds2 = env.fromCollection(emptyTupleData)

      // Should not work, incompatible key types
      ds1.coGroup(ds2).where(0).equalTo(2)
    }
  }

  @Test
  def testCoGroupKeyFields3(): Unit = {
    assertThrows[IncompatibleKeysException] {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromCollection(emptyTupleData)
      val ds2 = env.fromCollection(emptyTupleData)

      // Should not work, incompatible number of key fields
      ds1.coGroup(ds2).where(0, 1).equalTo(2)
    }
  }

  @Test
  def testCoGroupKeyFields4(): Unit = {
    assertThrows[IndexOutOfBoundsException] {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromCollection(emptyTupleData)
      val ds2 = env.fromCollection(emptyTupleData)

      // Should not work, field position out of range
      ds1.coGroup(ds2).where(5).equalTo(0)
    }
  }

  @Test
  def testCoGroupKeyFields5(): Unit = {
    assertThrows[IndexOutOfBoundsException] {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromCollection(emptyTupleData)
      val ds2 = env.fromCollection(emptyTupleData)

      // Should not work, negative field position
      ds1.coGroup(ds2).where(-1).equalTo(-1)
    }
  }

  @Test
  def testCoGroupKeyFields6(): Unit = {
    assertThrows[InvalidProgramException] {
      val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromCollection(emptyTupleData)
      val ds2 = env.fromCollection(customTypeData)

      // Should not work, field position key on custom data type
      ds1.coGroup(ds2).where(4).equalTo(0)
    }
  }

  @Test
  def testCoGroupKeyFieldNames1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyTupleData)

    // Should work
    try {
      ds1.coGroup(ds2).where("_1").equalTo("_1")
    }
    catch {
      case e: Exception => Assertions.fail()
    }
  }

  @Test
  def testCoGroupKeyFieldNames2(): Unit = {
    assertThrows[IncompatibleKeysException] {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromCollection(emptyTupleData)
      val ds2 = env.fromCollection(emptyTupleData)

      // Should not work, incompatible key types
      ds1.coGroup(ds2).where("_1").equalTo("_3")
    }
  }

  @Test
  def testCoGroupKeyFieldNames3(): Unit = {
    assertThrows[IncompatibleKeysException] {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromCollection(emptyTupleData)
      val ds2 = env.fromCollection(emptyTupleData)

      // Should not work, incompatible number of key fields
      ds1.coGroup(ds2).where("_1", "_2").equalTo("_3")
    }
  }

  @Test
  def testCoGroupKeyFieldNames4(): Unit = {
    assertThrows[RuntimeException] {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromCollection(emptyTupleData)
      val ds2 = env.fromCollection(emptyTupleData)

      // Should not work, invalid field name
      ds1.coGroup(ds2).where("_6").equalTo("_1")
    }
  }

  @Test
  def testCoGroupKeyFieldNames5(): Unit = {
    assertThrows[RuntimeException] {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromCollection(emptyTupleData)
      val ds2 = env.fromCollection(emptyTupleData)

      // Should not work, invalid field name
      ds1.coGroup(ds2).where("_1").equalTo("bar")
    }
  }

  @Test
  def testCoGroupKeyFieldNames6(): Unit = {
    assertThrows[RuntimeException] {
      val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromCollection(emptyTupleData)
      val ds2 = env.fromCollection(customTypeData)

      // Should not work, field position key on custom data type
      ds1.coGroup(ds2).where("_3").equalTo("_1")
    }
  }

  @Test
  def testCoGroupKeyExpressions1(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(customTypeData)
    val ds2 = env.fromCollection(customTypeData)

    // Should work
    try {
      //      ds1.coGroup(ds2).where("i").equalTo("i");

    }
    catch {
      case e: Exception => Assertions.fail()
    }
  }

  @Test
  def testCoGroupKeyExpressions2(): Unit = {
    assertThrows[IncompatibleKeysException] {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromCollection(customTypeData)
      val ds2 = env.fromCollection(customTypeData)

      // should not work, incompatible key types
      ds1.coGroup(ds2).where("myInt").equalTo("myString")
    }
  }

  @Test
  def testCoGroupKeyExpressions3(): Unit = {
    assertThrows[IncompatibleKeysException] {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromCollection(customTypeData)
      val ds2 = env.fromCollection(customTypeData)

      // should not work, incompatible number of keys
      ds1.coGroup(ds2).where("myInt", "myString").equalTo("myString")
    }
  }

  @Test
  def testCoGroupKeyExpressions4(): Unit = {
    assertThrows[IllegalArgumentException] {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromCollection(customTypeData)
      val ds2 = env.fromCollection(customTypeData)


      // should not work, key non-existent
      ds1.coGroup(ds2).where("myNonExistent").equalTo("i")
    }
  }

  @Test
  def testCoGroupKeySelectors1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(customTypeData)
    val ds2 = env.fromCollection(customTypeData)

    // Should work
    try {
      ds1.coGroup(ds2).where {
        _.myLong
      } equalTo {
        _.myLong
      }
    }
    catch {
      case e: Exception => Assertions.fail()
    }
  }

  @Test
  def testCoGroupKeyMixing1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(customTypeData)
    val ds2 = env.fromCollection(emptyTupleData)

    // Should work
    try {
      ds1.coGroup(ds2).where {
        _.myLong
      }.equalTo(3)
    }
    catch {
      case e: Exception => Assertions.fail()
    }
  }

  @Test
  def testCoGroupKeyMixing2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(customTypeData)

    // Should work
    try {
      ds1.coGroup(ds2).where(3).equalTo {
        _.myLong
      }
    }
    catch {
      case e: Exception => Assertions.fail()
    }
  }

  @Test
  def testCoGroupKeyMixing3(): Unit = {
    assertThrows[IncompatibleKeysException] {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromCollection(emptyTupleData)
      val ds2 = env.fromCollection(customTypeData)

      // Should not work, incompatible types
      ds1.coGroup(ds2).where(2).equalTo {
        _.myLong
      }
    }
  }

  @Test
  def testCoGroupKeyMixing4(): Unit = {
    assertThrows[IncompatibleKeysException] {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromCollection(emptyTupleData)
      val ds2 = env.fromCollection(customTypeData)

      // Should not work, more than one field position key
      ds1.coGroup(ds2).where(1, 3).equalTo {
        _.myLong
      }
    }
  }

  @Test
  def testCoGroupWithAtomic1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromElements(0, 1, 2)

    ds1.coGroup(ds2).where(0).equalTo("*")
  }

  @Test
  def testCoGroupWithAtomic2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(0, 1, 2)
    val ds2 = env.fromCollection(emptyTupleData)

    ds1.coGroup(ds2).where("*").equalTo(0)
  }

  @Test
  def testCoGroupWithInvalidAtomic1(): Unit = {
    assertThrows[InvalidProgramException] {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromElements(0, 1, 2)
      val ds2 = env.fromCollection(emptyTupleData)

      ds1.coGroup(ds2).where("invalidKey")
    }
  }

  @Test
  def testCoGroupWithInvalidAtomic2(): Unit = {
    assertThrows[InvalidProgramException] {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromCollection(emptyTupleData)
      val ds2 = env.fromElements(0, 1, 2)

      ds1.coGroup(ds2).where(0).equalTo("invalidKey")
    }
  }

  @Test
  def testCoGroupWithInvalidAtomic3(): Unit = {
    assertThrows[InvalidProgramException] {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromElements(new util.ArrayList[Integer]())
      val ds2 = env.fromElements(0, 0, 0)

      ds1.coGroup(ds2).where("*")
    }
  }

  @Test
  def testCoGroupWithInvalidAtomic4(): Unit = {
    assertThrows[InvalidProgramException] {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val ds1 = env.fromElements(0, 0, 0)
      val ds2 = env.fromElements(new util.ArrayList[Integer]())

      ds1.coGroup(ds2).where("*").equalTo("*")
    }
  }
}


