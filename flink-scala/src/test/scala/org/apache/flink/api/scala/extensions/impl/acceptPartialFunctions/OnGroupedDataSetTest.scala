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

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.operators._
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.api.scala.extensions.base.AcceptPFTestBase
import org.apache.flink.api.scala.extensions.data.KeyValuePair
import org.junit.Test

class OnGroupedDataSetTest extends AcceptPFTestBase {

  @Test
  def testSortGroupWithOnTuple(): Unit = {
    val test =
      groupedTuples.sortGroupWith(Order.ASCENDING) {
        case (id, _) => id
      }
    assert(test.isInstanceOf[GroupedDataSet[_]],
      "sortGroupWith on tuples should produce a GroupedDataSet")
  }

  @Test
  def testSortGroupWithOnCaseClass(): Unit = {
    val test =
      groupedCaseObjects.sortGroupWith(Order.ASCENDING) {
        case KeyValuePair(id, _) => id
      }
    assert(test.isInstanceOf[GroupedDataSet[_]],
      "sortGroupWith on case objects should produce a GroupedDataSet")
  }

  @Test
  def testReduceWithOnTuple(): Unit = {
    val test =
      groupedTuples.reduceWith {
        case ((_, v1), (_, v2)) => (0, s"$v1 $v2")
      }

    assert(test.javaSet.isInstanceOf[ReduceOperator[_]],
      "reduceWith on tuples should produce a ReduceOperator")
  }

  @Test
  def testReduceWithOnCaseClass(): Unit = {
    val test =
      groupedCaseObjects.reduceWith {
        case (KeyValuePair(_, v1), KeyValuePair(_, v2)) => KeyValuePair(0, s"$v1 $v2")
      }

    assert(test.javaSet.isInstanceOf[ReduceOperator[_]],
      "reduceWith on case objects should produce a ReduceOperator")
  }

  @Test
  def testReduceGroupWithOnTuple(): Unit = {
    val accumulator: StringBuffer = new StringBuffer()
    val test =
      groupedTuples.reduceGroupWith {
        case (_, value) #:: _ => accumulator.append(value).append('\n')
      }

    assert(test.javaSet.isInstanceOf[GroupReduceOperator[_, _]],
      "reduceGroupWith on tuples should produce a GroupReduceOperator")
  }

  @Test
  def testReduceGroupWithOnCaseClass(): Unit = {
    val accumulator: StringBuffer = new StringBuffer()
    val test =
      groupedCaseObjects.reduceGroupWith {
        case KeyValuePair(_, value) #:: _ => accumulator.append(value).append('\n')
      }

    assert(test.javaSet.isInstanceOf[GroupReduceOperator[_, _]],
      "reduceGroupWith on case objects should produce a GroupReduceOperator")
  }

  @Test
  def testCombineGroupWithOnTuple(): Unit = {
    val accumulator: StringBuffer = new StringBuffer()
    val test =
      groupedTuples.combineGroupWith {
        case (_, value) #:: _ => accumulator.append(value).append('\n')
      }

    assert(test.javaSet.isInstanceOf[GroupCombineOperator[_, _]],
      "combineGroupWith on tuples should produce a GroupCombineOperator")
  }

  @Test
  def testCombineGroupWithOnCaseClass(): Unit = {
    val accumulator: StringBuffer = new StringBuffer()
    val test =
      groupedCaseObjects.combineGroupWith {
        case KeyValuePair(_, value) #:: _ => accumulator.append(value).append('\n')
      }

    assert(test.javaSet.isInstanceOf[GroupCombineOperator[_, _]],
      "combineGroupWith on case objects should produce a GroupCombineOperator")
  }

}
