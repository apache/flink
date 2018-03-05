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

package org.apache.flink.streaming.api.scala.testutils

import org.apache.flink.api.common.functions.Function
import org.apache.flink.streaming.api.graph.{StreamGraph}
import org.apache.flink.streaming.api.operators.{AbstractUdfStreamOperator, StreamOperator}
import org.apache.flink.streaming.api.scala.{DataStream}

/**
  * Test utils for DataStream and KeyedStream.
  */
object StreamTestUtils {

  def getFunctionForDataStream(dataStream: DataStream[_]): Function = {
    dataStream.print()
    val operator = getOperatorForDataStream(dataStream)
      .asInstanceOf[AbstractUdfStreamOperator[_, _]]
    operator.getUserFunction.asInstanceOf[Function]
  }

  def getOperatorForDataStream(dataStream: DataStream[_]): StreamOperator[_] = {
    dataStream.print()
    val env = dataStream.javaStream.getExecutionEnvironment
    val streamGraph: StreamGraph = env.getStreamGraph
    streamGraph.getStreamNode(dataStream.getId).getOperator
  }
}
