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

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.streaming.api.scala.extensions.base.AcceptPFTestBase
import org.apache.flink.streaming.api.scala.extensions.data.KeyValuePair
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.Test

class OnJoinedStreamTest extends AcceptPFTestBase {

  @Test
  def testProjectingOnTuple(): Unit = {
    val test =
      tuples.join(tuples).
        where {
          case (id, _) => id
        }.equalTo {
          case (id, _) => id
        }.window {
          TumblingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS))
        }.projecting {
          case ((_, v1), (_, v2)) => s"$v1 $v2"
        }
    assert(test.javaStream.isInstanceOf[SingleOutputStreamOperator[_]],
      "projecting should produce a SingleOutputStreamOperator")
  }

  @Test
  def testProjectingOnCaseClass(): Unit = {
    val test =
      caseObjects.join(caseObjects).
      where {
        case KeyValuePair(id, _) => id
      }.equalTo {
        case KeyValuePair(id, _) => id
      }.window {
        TumblingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS))
      }.projecting {
        case (KeyValuePair(_, v1), KeyValuePair(_, v2)) => s"$v1 $v2"
      }
   assert(test.javaStream.isInstanceOf[SingleOutputStreamOperator[_]],
     "projecting should produce a SingleOutputStreamOperator")
  }

}
