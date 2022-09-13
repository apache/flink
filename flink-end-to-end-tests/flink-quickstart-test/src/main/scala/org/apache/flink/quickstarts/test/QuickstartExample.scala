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
package org.apache.flink.quickstarts.test

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.collection.JavaConversions.mapAsJavaMap

object QuickstartExample {
  def main(args: Array[String]) {

    val parameterTool = ParameterTool.fromArgs(args)

    if (parameterTool.getNumberOfParameters < 1) {
      println("Missing parameters!\nUsage: --numRecords <numRecords>")
      return
    }
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)

    val source: DataStream[(String)] = env
      .fromSequence(0, parameterTool.getInt("numRecords") - 1)
      .map(v => Utils.prefix(v))

    source.print()

    env.execute("Elasticsearch7.x end to end sink test example")
  }
}
