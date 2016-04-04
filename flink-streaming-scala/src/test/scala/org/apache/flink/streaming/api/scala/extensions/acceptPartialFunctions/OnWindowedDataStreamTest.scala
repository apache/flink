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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.streaming.api.scala.extensions.base.AcceptPFTestBase
import org.apache.flink.streaming.api.scala.extensions.data.KeyValuePair
import org.junit.Test

class OnWindowedDataStreamTest extends AcceptPFTestBase {

  @Test
  def testReduceWithOnTuple(): Unit = {
    val test =
      windowedTuples.reduceWith {
        case ((_, v1), (_, v2)) => 0 -> s"$v1 $v2"
      }
    assert(test.javaStream.isInstanceOf[SingleOutputStreamOperator[_]],
      "reduceWith should produce a SingleOutputStreamOperator")
  }

  @Test
  def testReduceWithOnCaseClass(): Unit = {
    val test =
      windowedCaseObjects.reduceWith {
        case (KeyValuePair(_, v1), KeyValuePair(_, v2)) => KeyValuePair(0, s"$v1 $v2")
      }
    assert(test.javaStream.isInstanceOf[SingleOutputStreamOperator[_]],
      "reduceWith should produce a SingleOutputStreamOperator")
  }

  @Test
  def testFoldWithOnTuple(): Unit = {
    val test =
      windowedTuples.foldWith("") {
        case (folding, (_, value)) => s"$folding $value"
      }
    assert(test.javaStream.isInstanceOf[SingleOutputStreamOperator[_]],
      "foldWith should produce a SingleOutputStreamOperator")
  }

  @Test
  def testFoldWithOnCaseClass(): Unit = {
    val test =
      windowedCaseObjects.foldWith("") {
        case (folding, KeyValuePair(_, value)) => s"$folding $value"
      }
    assert(test.javaStream.isInstanceOf[SingleOutputStreamOperator[_]],
      "foldWith should produce a SingleOutputStreamOperator")
  }

  @Test
  def testApplyWithOnTuple(): Unit = {
    val test =
      windowedTuples.applyWith("")(
        foldFunction = {
          case (folding, (_, value)) => s"$folding $value"
        },
        windowFunction = {
          case (n, w, head #:: neck #:: _) => Seq(n.toString, w.maxTimestamp().toString, head, neck)
        })
    assert(test.javaStream.isInstanceOf[SingleOutputStreamOperator[_]],
      "applyWith should produce a SingleOutputStreamOperator")
  }

  @Test
  def testApplyWithOnCaseClass(): Unit = {
    val test =
      windowedCaseObjects.applyWith("")(
        foldFunction = {
          case (folding, KeyValuePair(_, value)) => s"$folding $value"
        },
        windowFunction = {
          case (n, w, head #:: neck #:: _) => Seq(n.toString, w.maxTimestamp().toString, head, neck)
        })
    assert(test.javaStream.isInstanceOf[SingleOutputStreamOperator[_]],
      "applyWith should produce a SingleOutputStreamOperator")
  }

}
