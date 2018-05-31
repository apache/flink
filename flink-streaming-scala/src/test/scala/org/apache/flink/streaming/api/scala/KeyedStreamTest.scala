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

import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.operators.{KeyedProcessOperator, LegacyKeyedProcessOperator}
import org.apache.flink.streaming.api.scala.testutils.StreamTestUtils
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.util.Collector
import org.junit.Test

/**
  * Tests for [[KeyedStream]]
  */
class KeyedStreamTest extends AbstractTestBase {

  /**
    * Verify that a [[KeyedStream.process(ProcessFunction)]] call is correctly
    * translated to an operator.
    */
  @Test
  @deprecated
  def testKeyedStreamProcessTranslation(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val src = env.generateSequence(0, 0)

    val processFunction = new ProcessFunction[Long, Int] {
      override def processElement(
                                   value: Long,
                                   ctx: ProcessFunction[Long, Int]#Context,
                                   out: Collector[Int]): Unit = ???
    }

    val flatMapped = src.keyBy(x => x).process(processFunction)

    assert(processFunction == StreamTestUtils.getFunctionForDataStream(flatMapped))
    assert(StreamTestUtils.getOperatorForDataStream(flatMapped)
      .isInstanceOf[LegacyKeyedProcessOperator[_, _, _]])
  }

  /**
    * Verify that a [[KeyedStream.process(KeyedProcessFunction)]] call is correctly
    * translated to an operator.
    */
  @Test
  def testKeyedStreamKeyedProcessTranslation(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val src = env.generateSequence(0, 0)

    val keyedProcessFunction = new KeyedProcessFunction[Long, Long, Int] {
      override def processElement(
                                   value: Long,
                                   ctx: KeyedProcessFunction[Long, Long, Int]#Context,
                                   out: Collector[Int]): Unit = ???
    }

    val flatMapped = src.keyBy(x => x).process(keyedProcessFunction)

    assert(keyedProcessFunction == StreamTestUtils.getFunctionForDataStream(flatMapped))
    assert(StreamTestUtils.getOperatorForDataStream(flatMapped)
      .isInstanceOf[KeyedProcessOperator[_, _, _]])
  }
}
