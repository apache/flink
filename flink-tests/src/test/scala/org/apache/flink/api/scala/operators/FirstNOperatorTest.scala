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
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.junit.{Assert, Test}

import org.apache.flink.api.scala._

class FirstNOperatorTest {

  private val emptyTupleData = Array[(Int, Long, String, Long, Int)]()

  @Test
  def testUngroupedFirstN(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val tupleDs = env.fromCollection(emptyTupleData)

    try {
      tupleDs.first(1)
    }
    catch {
      case e: Exception => {
        Assert.fail()
      }
    }
    try {
      tupleDs.first(10)
    }
    catch {
      case e: Exception => {
        Assert.fail()
      }
    }
    try {
      tupleDs.first(0)
      Assert.fail()
    }
    catch {
      case ipe: InvalidProgramException => {
      }
      case e: Exception => {
        Assert.fail()
      }
    }
    try {
      tupleDs.first(-1)
      Assert.fail()
    }
    catch {
      case ipe: InvalidProgramException => {
      }
      case e: Exception => {
        Assert.fail()
      }
    }
  }

  @Test
  def testGroupedFirstN(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tupleDs = env.fromCollection(emptyTupleData)

    try {
      tupleDs.groupBy(2).first(1)
    }
    catch {
      case e: Exception => {
        Assert.fail()
      }
    }
    try {
      tupleDs.groupBy(1, 3).first(10)
    }
    catch {
      case e: Exception => {
        Assert.fail()
      }
    }
    try {
      tupleDs.groupBy(0).first(0)
      Assert.fail()
    }
    catch {
      case ipe: InvalidProgramException => {
      }
      case e: Exception => {
        Assert.fail()
      }
    }
    try {
      tupleDs.groupBy(2).first(-1)
      Assert.fail()
    }
    catch {
      case ipe: InvalidProgramException => {
      }
      case e: Exception => {
        Assert.fail()
      }
    }
  }

  @Test
  def testGroupedSortedFirstN(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tupleDs = env.fromCollection(emptyTupleData)
    
    try {
      tupleDs.groupBy(2).sortGroup(4, Order.ASCENDING).first(1)
    }
    catch {
      case e: Exception => {
        Assert.fail()
      }
    }
    try {
      tupleDs.groupBy(1, 3).sortGroup(4, Order.ASCENDING).first(10)
    }
    catch {
      case e: Exception => {
        Assert.fail()
      }
    }
    try {
      tupleDs.groupBy(0).sortGroup(4, Order.ASCENDING).first(0)
      Assert.fail()
    }
    catch {
      case ipe: InvalidProgramException => {
      }
      case e: Exception => {
        Assert.fail()
      }
    }
    try {
      tupleDs.groupBy(2).sortGroup(4, Order.ASCENDING).first(-1)
      Assert.fail()
    }
    catch {
      case ipe: InvalidProgramException => {
      }
      case e: Exception => {
        Assert.fail()
      }
    }
  }

}

