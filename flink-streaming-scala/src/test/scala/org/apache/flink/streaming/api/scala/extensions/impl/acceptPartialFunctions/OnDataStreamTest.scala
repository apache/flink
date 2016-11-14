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
package org.apache.flink.streaming.api.scala.extensions.impl.acceptPartialFunctions

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.{KeyedStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.streaming.api.scala.extensions.base.AcceptPFTestBase
import org.apache.flink.streaming.api.scala.extensions.data.KeyValuePair
import org.junit.Test

class OnDataStreamTest extends AcceptPFTestBase {

  @Test
  def testMapWithOnTuple(): Unit = {
    val test =
      tuples.mapWith {
        case (id, value) => s"$id $value"
      }
    assert(test.javaStream.isInstanceOf[SingleOutputStreamOperator[_]],
      "mapWith should produce a SingleOutputStreamOperator")
  }

  @Test
  def testMapWithOnCaseClass(): Unit = {
    val test =
      caseObjects.mapWith {
        case KeyValuePair(id, value) => s"$id $value"
      }
    assert(test.javaStream.isInstanceOf[SingleOutputStreamOperator[_]],
      "mapWith should produce a SingleOutputStreamOperator")
  }

  @Test
  def testFlatMapWithOnTuple(): Unit = {
    val test =
      tuples.flatMapWith {
        case (id, value) => List(id.toString, value)
      }
    assert(test.javaStream.isInstanceOf[SingleOutputStreamOperator[_]],
      "flatMapWith should produce a SingleOutputStreamOperator")
  }

  @Test
  def testFlatMapWithOnCaseClass(): Unit = {
    val test =
      caseObjects.flatMapWith {
        case KeyValuePair(id, value) => List(id.toString, value)
      }
    assert(test.javaStream.isInstanceOf[SingleOutputStreamOperator[_]],
      "flatMapWith should produce a SingleOutputStreamOperator")
  }

  @Test
  def testFilterWithOnTuple(): Unit = {
    val test =
      tuples.filterWith {
        case (id, value) => id == 1
      }
    assert(test.javaStream.isInstanceOf[SingleOutputStreamOperator[_]],
      "filterWith should produce a SingleOutputStreamOperator")
  }

  @Test
  def testFilterWithOnCaseClass(): Unit = {
    val test =
      caseObjects.filterWith {
        case KeyValuePair(id, value) => id == 1
      }
    assert(test.javaStream.isInstanceOf[SingleOutputStreamOperator[_]],
      "filterWith should produce a SingleOutputStreamOperator")
  }

  @Test
  def testKeyingByOnTuple(): Unit = {
    val test =
      tuples.keyingBy {
        case (id, _) => id
      }
    assert(test.javaStream.isInstanceOf[KeyedStream[_, _]],
      "keyingBy should produce a KeyedStream")
  }

  @Test
  def testKeyingByOnCaseClass(): Unit = {
    val test =
      caseObjects.keyingBy {
        case KeyValuePair(id, _) => id
      }
    assert(test.javaStream.isInstanceOf[KeyedStream[_, _]],
      "keyingBy should produce a KeyedStream")
  }

}
