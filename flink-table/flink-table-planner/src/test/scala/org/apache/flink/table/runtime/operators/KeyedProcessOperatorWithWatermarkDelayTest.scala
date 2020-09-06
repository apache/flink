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
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.{KeyedOneInputStreamOperatorTestHarness, TestHarnessUtil}
import org.apache.flink.util.{Collector, TestLogger}
import org.junit.Test

/**
  * Tests [[KeyedProcessOperatorWithWatermarkDelay]].
  */
class KeyedProcessOperatorWithWatermarkDelayTest extends TestLogger {

  @Test
  def testHoldingBackWatermarks(): Unit = {
    val operator = new KeyedProcessOperatorWithWatermarkDelay[Integer, Integer, String](
      new EmptyProcessFunction, 100)
    val testHarness = new KeyedOneInputStreamOperatorTestHarness[Integer, Integer, String](
      operator, new IdentityKeySelector[Integer], BasicTypeInfo.INT_TYPE_INFO)

    testHarness.setup()
    testHarness.open()
    testHarness.processWatermark(new Watermark(101))
    testHarness.processWatermark(new Watermark(103))

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
    new KeyedProcessOperatorWithWatermarkDelay[Integer, Integer, String](
      new EmptyProcessFunction, -1)
  }
}

private class EmptyProcessFunction extends ProcessFunction[Integer, String] {
  override def processElement(
    value: Integer,
    ctx: ProcessFunction[Integer, String]#Context,
    out: Collector[String]): Unit = {
    // do nothing
  }
}

private class IdentityKeySelector[T] extends KeySelector[T, T] {
  override def getKey(value: T): T = value
}
