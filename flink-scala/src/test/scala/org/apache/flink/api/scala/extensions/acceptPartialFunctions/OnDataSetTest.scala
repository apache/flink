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
package org.apache.flink.api.scala.extensions.impl.acceptPartialFunctions

import org.apache.flink.api.java.operators._
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.api.scala.extensions.base.AcceptPFTestBase
import org.apache.flink.api.scala.extensions.data.KeyValuePair
import org.junit.Test

class OnDataSetTest extends AcceptPFTestBase {

  @Test
  def testMapWithOnTuple(): Unit = {
    val test =
      tuples.mapWith {
        case (id, value) => s"$id $value"
      }
    assert(test.javaSet.isInstanceOf[MapOperator[_, _]],
      "mapWith should produce a MapOperator")
  }

  @Test
  def testMapWithOnCaseClass(): Unit = {
    val test =
      caseObjects.mapWith {
        case KeyValuePair(id, value) => s"$id $value"
      }
    assert(test.javaSet.isInstanceOf[MapOperator[_, _]],
      "mapWith should produce a MapOperator")
  }

  @Test
  def testMapPartitionWithOnTuple(): Unit = {
    val test =
      tuples.mapPartitionWith {
        case (id, value) #:: _ => s"$id $value"
      }
    assert(test.javaSet.isInstanceOf[MapPartitionOperator[_, _]],
      "mapPartitionWith should produce a MapPartitionOperator")
  }

  @Test
  def testMapPartitionWithOnCaseClass(): Unit = {
    val test =
      caseObjects.mapPartitionWith {
        case KeyValuePair(id, value) #:: _ => s"$id $value"
      }
    assert(test.javaSet.isInstanceOf[MapPartitionOperator[_, _]],
      "mapPartitionWith should produce a MapPartitionOperator")
  }

  @Test
  def testFlatMapWithOnTuple(): Unit = {
    val test =
      tuples.flatMapWith {
        case (id, value) => List(id.toString, value)
      }
    assert(test.javaSet.isInstanceOf[FlatMapOperator[_, _]],
      "flatMapWith should produce a FlatMapOperator")
  }

  @Test
  def testFlatMapWithOnCaseClass(): Unit = {
    val test =
      caseObjects.flatMapWith {
        case KeyValuePair(id, value) => List(id.toString, value)
      }
    assert(test.javaSet.isInstanceOf[FlatMapOperator[_, _]],
      "flatMapWith should produce a FlatMapOperator")
  }

  @Test
  def testFilterWithOnTuple(): Unit = {
    val test =
      tuples.filterWith {
        case (id, value) => id == 1
      }
    assert(test.javaSet.isInstanceOf[FilterOperator[_]],
      "filterWith should produce a FilterOperator")
  }

  @Test
  def testFilterWithOnCaseClass(): Unit = {
    val test =
      caseObjects.filterWith {
        case KeyValuePair(id, value) => id == 1
      }
    assert(test.javaSet.isInstanceOf[FilterOperator[_]],
      "filterWith should produce a FilterOperator")
  }

  @Test
  def testReduceWithOnTuple(): Unit = {
    val test =
      tuples.reduceWith {
        case ((_, v1), (_, v2)) => (0, s"$v1 $v2")
      }
    assert(test.javaSet.isInstanceOf[ReduceOperator[_]],
      "reduceWith should produce a ReduceOperator")
  }

  @Test
  def testReduceWithOnCaseClass(): Unit = {
    val test =
      caseObjects.reduceWith {
        case (KeyValuePair(_, v1), KeyValuePair(_, v2)) => KeyValuePair(0, s"$v1 $v2")
      }
    assert(test.javaSet.isInstanceOf[ReduceOperator[_]],
      "reduceWith should produce a ReduceOperator")
  }

  @Test
  def testReduceGroupWithOnTuple(): Unit = {
    val accumulator: StringBuffer = new StringBuffer()
    val test =
      tuples.reduceGroupWith {
        case (_, value) #:: _ => accumulator.append(value).append('\n')
      }
    assert(test.javaSet.isInstanceOf[GroupReduceOperator[_, _]],
      "reduceGroupWith should produce a GroupReduceOperator")
  }

  @Test
  def testReduceGroupWithOnCaseClass(): Unit = {
    val accumulator: StringBuffer = new StringBuffer()
    val test =
      caseObjects.reduceGroupWith {
        case KeyValuePair(_, value) #:: _ => accumulator.append(value).append('\n')
      }
    assert(test.javaSet.isInstanceOf[GroupReduceOperator[_, _]],
      "reduceGroupWith should produce a GroupReduceOperator")
  }

  @Test
  def testGroupingByOnTuple(): Unit = {
    val test =
      tuples.groupingBy {
        case (id, _) => id
      }
    assert(test.isInstanceOf[GroupedDataSet[_]],
      "groupingBy should produce a GroupedDataSet")
  }

  @Test
  def testGroupingByOnCaseClass(): Unit = {
    val test =
      caseObjects.groupingBy {
        case KeyValuePair(id, _) => id
      }
    assert(test.isInstanceOf[GroupedDataSet[_]],
      "groupingBy should produce a GroupedDataSet")
  }

}
