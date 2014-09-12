/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.scala.operators

import org.junit.Assert
import org.apache.flink.api.common.InvalidProgramException
import org.junit.Ignore
import org.junit.Test

import org.apache.flink.api.scala._

class JoinOperatorTest {

  private val emptyTupleData = Array[(Int, Long, String, Long, Int)]()
  private val customTypeData = Array[CustomType](new CustomType())
  private val emptyLongData = Array[Long]()

  @Test
  def testJoinKeyFields1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyTupleData)
    try {
      ds1.join(ds2).where(0).equalTo(0)
    }
    catch {
      case e: Exception => Assert.fail()
    }
  }

  @Test(expected = classOf[InvalidProgramException])
  def testJoinKeyFields2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyTupleData)
    ds1.join(ds2).where(0).equalTo(2)
  }

  @Test(expected = classOf[InvalidProgramException])
  def testJoinKeyFields3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyTupleData)
    ds1.join(ds2).where(0, 1).equalTo(2)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testJoinKeyFields4(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyTupleData)
    ds1.join(ds2).where(5).equalTo(0)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testJoinKeyFields5(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(emptyTupleData)
    ds1.join(ds2).where(-1).equalTo(-1)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testJoinKeyFields6(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(customTypeData)
    ds1.join(ds2).where(5).equalTo(0)
  }

  @Ignore
  @Test
  def testJoinKeyExpressions1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(customTypeData)
    val ds2 = env.fromCollection(customTypeData)

    // should work
    try {
//      ds1.join(ds2).where("i").equalTo("i")
    }
    catch {
      case e: Exception => Assert.fail()
    }
  }

  @Ignore
  @Test(expected = classOf[InvalidProgramException])
  def testJoinKeyExpressions2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(customTypeData)
    val ds2 = env.fromCollection(customTypeData)

    // should not work, incompatible join key types
//    ds1.join(ds2).where("i").equalTo("s")
  }

  @Ignore
  @Test(expected = classOf[InvalidProgramException])
  def testJoinKeyExpressions3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(customTypeData)
    val ds2 = env.fromCollection(customTypeData)

    // should not work, incompatible number of keys
//    ds1.join(ds2).where("i", "s").equalTo("i")
  }

  @Ignore
  @Test(expected = classOf[IllegalArgumentException])
  def testJoinKeyExpressions4(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(customTypeData)
    val ds2 = env.fromCollection(customTypeData)

    // should not work, join key non-existent
//    ds1.join(ds2).where("myNonExistent").equalTo("i")
  }

  @Test
  def testJoinKeySelectors1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(customTypeData)
    val ds2 = env.fromCollection(customTypeData)

    // should work
    try {
      ds1.join(ds2).where { _.l} equalTo { _.l }
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
      ds1.join(ds2).where { _.l }.equalTo(3)
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
      ds1.join(ds2).where(3).equalTo { _.l }
    }
    catch {
      case e: Exception => Assert.fail()
    }
  }

  @Test(expected = classOf[InvalidProgramException])
  def testJoinKeyMixing3(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(customTypeData)

    // should not work, incompatible types
    ds1.join(ds2).where(2).equalTo { _.l }
  }

  @Test(expected = classOf[InvalidProgramException])
  def testJoinKeyMixing4(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(emptyTupleData)
    val ds2 = env.fromCollection(customTypeData)

    // should not work, more than one field position key
    ds1.join(ds2).where(1, 3) equalTo { _.l }
  }
}

