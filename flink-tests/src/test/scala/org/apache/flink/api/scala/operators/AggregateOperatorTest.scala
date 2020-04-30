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

import org.junit.Assert
import org.apache.flink.api.common.InvalidProgramException
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.java.aggregation.UnsupportedAggregationTypeException
import org.junit.Test

import org.apache.flink.api.scala._

class AggregateOperatorTest {

  private final val emptyTupleData = Array[(Int, Long, String, Long, Int)]()
  private final val tupleTypeInfo = createTypeInformation[(Int, Long, String, Long, Int)]
  private final val emptyLongData = Array[Long]()

  @Test
  def testFieldsAggregate(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val tupleDs = env.fromCollection(emptyTupleData)

    // should work
    try {
      tupleDs.aggregate(Aggregations.SUM, 1)
    } catch {
      case e: Exception => Assert.fail()
    }

    // should not work: index out of bounds
    try {
      tupleDs.aggregate(Aggregations.SUM, 10)
      Assert.fail()
    } catch {
      case iae: IllegalArgumentException =>
      case e: Exception => Assert.fail()
    }

    val longDs = env.fromCollection(emptyLongData)

    // should not work: not applied to tuple DataSet
    try {
      longDs.aggregate(Aggregations.MIN, 1)
      Assert.fail()
    } catch {
      case uoe: InvalidProgramException =>
      case e: Exception => Assert.fail()
    }
  }

  @Test
  def testFieldNamesAggregate(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val tupleDs = env.fromCollection(emptyTupleData)

    // should work
    try {
      tupleDs.aggregate(Aggregations.SUM, "_2")
    } catch {
      case e: Exception => Assert.fail()
    }

    // should not work: invalid field
    try {
      tupleDs.aggregate(Aggregations.SUM, "foo")
      Assert.fail()
    } catch {
      case iae: IllegalArgumentException =>
      case e: Exception => Assert.fail()
    }

    val longDs = env.fromCollection(emptyLongData)

    // should not work: not applied to tuple DataSet
    try {
      longDs.aggregate(Aggregations.MIN, "_1")
      Assert.fail()
    } catch {
      case uoe: InvalidProgramException =>
      case uoe: UnsupportedOperationException =>
      case e: Exception => Assert.fail()
    }
  }

  @Test
  def testAggregationTypes(): Unit = {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment

      val tupleDs = env.fromCollection(emptyTupleData)

      // should work: multiple aggregates
      tupleDs.aggregate(Aggregations.SUM, 0).and(Aggregations.MIN, 4)

      // should work: nested aggregates
      tupleDs.aggregate(Aggregations.MIN, 2).and(Aggregations.SUM, 1)

      // should not work: average on string
      try {
        tupleDs.aggregate(Aggregations.SUM, 2)
        Assert.fail()
      } catch {
        case iae: UnsupportedAggregationTypeException =>
      }
    } catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        Assert.fail(e.getMessage)
      }
    }
  }

}

