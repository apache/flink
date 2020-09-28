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

import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.junit.Test

/**
  * Integration test for [[DataStreamUtils.reinterpretAsKeyedStream()]].
  */
class ReinterpretDataStreamAsKeyedStreamITCase {

  @Test
  def testReinterpretAsKeyedStream(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val source = env.fromElements(1, 2, 3, 1, 2, 3, 4, 4, 1, 1, 3)
    new DataStreamUtils(source).reinterpretAsKeyedStream((in) => in)
      .countWindow(2)
      .reduce((a, b) => a + b)
      .addSink(new PrintSinkFunction[Int])
    env.execute()
  }
}
