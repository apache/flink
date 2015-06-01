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
package org.apache.flink.streaming.api.scala

import java.lang.reflect.Method

import org.apache.flink.api.scala.completeness.ScalaAPICompletenessTestBase
import org.apache.flink.streaming.api.datastream.{DataStream => JavaStream}

import scala.language.existentials

import org.junit.Test

/**
 * This checks whether the streaming Scala API is up to feature parity with the Java API.
 * Implements the {@link ScalaAPICompletenessTest} for streaming.
 */
class StreamingScalaAPICompletenessTest extends ScalaAPICompletenessTestBase {

  override def isExcludedByName(method: Method): Boolean = {
    val name = method.getDeclaringClass.getName + "." + method.getName
    val excludedNames = Seq(
      // These are only used internally. Should be internal API but Java doesn't have
      // private[flink].
      "org.apache.flink.streaming.api.datastream.DataStream.getExecutionEnvironment",
      "org.apache.flink.streaming.api.datastream.DataStream.getType",
      "org.apache.flink.streaming.api.datastream.DataStream.copy",
      "org.apache.flink.streaming.api.datastream.DataStream.transform",
      "org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator.copy",
      "org.apache.flink.streaming.api.datastream.ConnectedDataStream.getExecutionEnvironment",
      "org.apache.flink.streaming.api.datastream.ConnectedDataStream.getType1",
      "org.apache.flink.streaming.api.datastream.ConnectedDataStream.getType2",
      "org.apache.flink.streaming.api.datastream.ConnectedDataStream.addGeneralWindowCombine",
      "org.apache.flink.streaming.api.datastream.ConnectedDataStream.transform",
      "org.apache.flink.streaming.api.datastream.WindowedDataStream.getType",
      "org.apache.flink.streaming.api.datastream.WindowedDataStream.getExecutionConfig",

      // TypeHints are only needed for Java API, Scala API doesn't need them
      "org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator.returns"
    )
    val excludedPatterns = Seq(
      // We don't have project on tuples in the Scala API
      """^org\.apache\.flink\.streaming.api.*project""",

      // Cleaning is easier in the Scala API
      """^org\.apache\.flink\.streaming.api.*clean""",

      // Object methods
      """^.*notify""",
      """^.*wait""",
      """^.*notifyAll""",
      """^.*equals""",
      """^.*toString""",
      """^.*getClass""",
      """^.*hashCode"""
    ).map(_.r)
    lazy val excludedByPattern =
      excludedPatterns.map(_.findFirstIn(name)).filter(_.isDefined).nonEmpty
    name.contains("$") || excludedNames.contains(name) || excludedByPattern
  }

  @Test
  override def testCompleteness(): Unit = {
    checkMethods("DataStream", "DataStream", classOf[JavaStream[_]], classOf[DataStream[_]])

    checkMethods(
      "StreamExecutionEnvironment", "StreamExecutionEnvironment",
      classOf[org.apache.flink.streaming.api.environment.StreamExecutionEnvironment],
      classOf[StreamExecutionEnvironment])

    checkMethods(
      "SingleOutputStreamOperator", "DataStream",
      classOf[org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator[_,_]],
      classOf[DataStream[_]])

    checkMethods(
      "ConnectedDataStream", "ConnectedDataStream",
      classOf[org.apache.flink.streaming.api.datastream.ConnectedDataStream[_,_]],
      classOf[ConnectedDataStream[_,_]])

    checkMethods(
      "SplitDataStream", "SplitDataStream",
      classOf[org.apache.flink.streaming.api.datastream.SplitDataStream[_]],
      classOf[SplitDataStream[_]])

    checkMethods(
      "StreamCrossOperator", "StreamCrossOperator",
      classOf[org.apache.flink.streaming.api.datastream.temporal.StreamCrossOperator[_,_]],
      classOf[StreamCrossOperator[_,_]])

    checkMethods(
      "StreamJoinOperator", "StreamJoinOperator",
      classOf[org.apache.flink.streaming.api.datastream.temporal.StreamJoinOperator[_,_]],
      classOf[StreamJoinOperator[_,_]])

    checkMethods(
      "TemporalOperator", "TemporalOperator",
      classOf[org.apache.flink.streaming.api.datastream.temporal.TemporalOperator[_,_,_]],
      classOf[TemporalOperator[_,_,_]])

    checkMethods(
      "WindowedDataStream", "WindowedDataStream",
      classOf[org.apache.flink.streaming.api.datastream.WindowedDataStream[_]],
      classOf[WindowedDataStream[_]])
  }
}
