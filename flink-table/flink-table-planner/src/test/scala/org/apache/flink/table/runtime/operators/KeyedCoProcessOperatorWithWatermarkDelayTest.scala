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
package org.apache.flink.table.runtime.operators

import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.{KeyedTwoInputStreamOperatorTestHarness, TestHarnessUtil}
import org.apache.flink.util.{Collector, TestLogger}
import org.junit.Test

/**
  * Tests [[KeyedCoProcessOperatorWithWatermarkDelay]].
  */
class KeyedCoProcessOperatorWithWatermarkDelayTest extends TestLogger {

  @Test
  def testHoldingBackWatermarks(): Unit = {
    val operator = new KeyedCoProcessOperatorWithWatermarkDelay[String, Integer, String, String](
      new EmptyCoProcessFunction, 100)
    val testHarness = new KeyedTwoInputStreamOperatorTestHarness[String, Integer, String, String](
      operator,
      new IntToStringKeySelector, new CoIdentityKeySelector[String],
      BasicTypeInfo.STRING_TYPE_INFO)

    testHarness.setup()
    testHarness.open()
    testHarness.processWatermark1(new Watermark(101))
    testHarness.processWatermark2(new Watermark(202))
    testHarness.processWatermark1(new Watermark(103))
    testHarness.processWatermark2(new Watermark(204))

    val expectedOutput = new ConcurrentLinkedQueue[AnyRef]
    expectedOutput.add(new Watermark(1))
    expectedOutput.add(new Watermark(3))

    TestHarnessUtil.assertOutputEquals(
      "Output was not correct.",
      expectedOutput,
      testHarness.getOutput)

    testHarness.close()
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testDelayParameter(): Unit = {
    new KeyedCoProcessOperatorWithWatermarkDelay[AnyRef, Integer, String, String](
      new EmptyCoProcessFunction, -1)
  }
}

private class EmptyCoProcessFunction extends CoProcessFunction[Integer, String, String] {
  override def processElement1(
    value: Integer,
    ctx: CoProcessFunction[Integer, String, String]#Context,
    out: Collector[String]): Unit = {
    // do nothing
  }

  override def processElement2(
    value: String,
    ctx: CoProcessFunction[Integer, String, String]#Context,
    out: Collector[String]): Unit = {
    //do nothing
  }
}


private class IntToStringKeySelector extends KeySelector[Integer, String] {
  override def getKey(value: Integer): String = String.valueOf(value)
}

private class CoIdentityKeySelector[T] extends KeySelector[T, T] {
  override def getKey(value: T): T = value
}
