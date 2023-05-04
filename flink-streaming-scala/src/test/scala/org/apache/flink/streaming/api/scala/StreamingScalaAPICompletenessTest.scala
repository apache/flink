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

import org.apache.flink.api.scala.completeness.ScalaAPICompletenessTestBase
import org.apache.flink.streaming.api.datastream.{DataStream => JavaStream}

import org.junit.jupiter.api.Test

import java.lang.reflect.Method

import scala.language.existentials

/** This checks whether the streaming Scala API is up to feature parity with the Java API. */
class StreamingScalaAPICompletenessTest extends ScalaAPICompletenessTestBase {

  override def isExcludedByName(method: Method): Boolean = {
    val name = method.getDeclaringClass.getName + "." + method.getName
    val excludedNames = Seq(
      // These are only used internally. Should be internal API but Java doesn't have
      // private[flink].
      "org.apache.flink.streaming.api.datastream.DataStream.getType",
      "org.apache.flink.streaming.api.datastream.DataStream.copy",
      "org.apache.flink.streaming.api.datastream.DataStream.getTransformation",
      "org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator.forceNonParallel",
      "org.apache.flink.streaming.api.datastream.ConnectedStreams.getExecutionEnvironment",
      "org.apache.flink.streaming.api.datastream.ConnectedStreams.getExecutionEnvironment",
      "org.apache.flink.streaming.api.datastream.ConnectedStreams.getFirstInput",
      "org.apache.flink.streaming.api.datastream.ConnectedStreams.getSecondInput",
      "org.apache.flink.streaming.api.datastream.ConnectedStreams.getType1",
      "org.apache.flink.streaming.api.datastream.ConnectedStreams.getType2",
      "org.apache.flink.streaming.api.datastream.ConnectedStreams.addGeneralWindowCombine",
      "org.apache.flink.streaming.api.datastream.WindowedStream.getType",
      "org.apache.flink.streaming.api.datastream.WindowedStream.getExecutionConfig",
      "org.apache.flink.streaming.api.datastream.WindowedStream.getExecutionEnvironment",
      "org.apache.flink.streaming.api.datastream.WindowedStream.getInputType",
      "org.apache.flink.streaming.api.datastream.AllWindowedStream.getExecutionEnvironment",
      "org.apache.flink.streaming.api.datastream.AllWindowedStream.getInputType",
      "org.apache.flink.streaming.api.datastream.KeyedStream.getKeySelector",
      "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.isChainingEnabled",
      "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment." +
        "isChainingOfOperatorsWithDifferentMaxParallelismEnabled",
      "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment." +
        "getStateHandleProvider",
      "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getCheckpointInterval",
      "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.addOperator",
      "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getCheckpointingMode",
      "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.isForceCheckpointing",
      "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.generateStreamGraph",
      "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getTransformations",
      "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment" +
        ".areExplicitEnvironmentsAllowed",
      "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment" +
        ".registerCollectIterator",
      "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment" +
        ".invalidateClusterDataset",
      "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment" +
        ".listCompletedClusterDatasets",
      "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment" +
        ".registerCacheTransformation",

      // TypeHints are only needed for Java API, Scala API doesn't need them
      "org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator.returns",

      // Deactivated until Scala API has new windowing API
      "org.apache.flink.streaming.api.datastream.DataStream.timeWindowAll",
      "org.apache.flink.streaming.api.datastream.DataStream.windowAll"
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
      excludedPatterns.map(_.findFirstIn(name)).exists(_.isDefined)
    name.contains("$") || excludedNames.contains(name) || excludedByPattern
  }

  @Test
  override def testCompleteness(): Unit = {
    checkMethods("DataStream", "DataStream", classOf[JavaStream[_]], classOf[DataStream[_]])

    checkMethods(
      "StreamExecutionEnvironment",
      "StreamExecutionEnvironment",
      classOf[org.apache.flink.streaming.api.environment.StreamExecutionEnvironment],
      classOf[StreamExecutionEnvironment]
    )

    checkMethods(
      "SingleOutputStreamOperator",
      "DataStream",
      classOf[org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator[_]],
      classOf[DataStream[_]])

    checkMethods(
      "ConnectedStreams",
      "ConnectedStreams",
      classOf[org.apache.flink.streaming.api.datastream.ConnectedStreams[_, _]],
      classOf[ConnectedStreams[_, _]])

    checkMethods(
      "WindowedStream",
      "WindowedStream",
      classOf[org.apache.flink.streaming.api.datastream.WindowedStream[_, _, _]],
      classOf[WindowedStream[_, _, _]])

    checkMethods(
      "AllWindowedStream",
      "AllWindowedStream",
      classOf[org.apache.flink.streaming.api.datastream.AllWindowedStream[_, _]],
      classOf[AllWindowedStream[_, _]])

    checkMethods(
      "KeyedStream",
      "KeyedStream",
      classOf[org.apache.flink.streaming.api.datastream.KeyedStream[_, _]],
      classOf[KeyedStream[_, _]])

    checkMethods(
      "JoinedStreams.WithWindow",
      "JoinedStreams.WithWindow",
      classOf[org.apache.flink.streaming.api.datastream.JoinedStreams.WithWindow[_, _, _, _]],
      classOf[JoinedStreams[_, _]#Where[_]#EqualTo#WithWindow[_]]
    )

    checkMethods(
      "CoGroupedStreams.WithWindow",
      "CoGroupedStreams.WithWindow",
      classOf[org.apache.flink.streaming.api.datastream.CoGroupedStreams.WithWindow[_, _, _, _]],
      classOf[CoGroupedStreams[_, _]#Where[_]#EqualTo#WithWindow[_]]
    )
  }
}
