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

import java.lang.{Boolean => JBool}
import scala.collection.JavaConversions._
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.runtime.aggregate.KeyedProcessFunctionWithCleanupState
import org.apache.flink.table.runtime.harness.HarnessTestBase
import org.apache.flink.util.Collector
import org.junit.Test
import org.junit.Assert.assertArrayEquals

class KeyedProcessFunctionWithCleanupStateTest extends HarnessTestBase {
  @Test
  def testNeedToCleanup(): Unit = {
    val queryConfig = new StreamQueryConfig()
      .withIdleStateRetentionTime(Time.milliseconds(5), Time.milliseconds(10))

    val func = new MockedKeyedProcessFunction[String, String](queryConfig)
    val operator = new KeyedProcessOperator(func)

    val testHarness = createHarnessTester(operator,
      new IdentityKeySelector[String],
      TypeInformation.of(classOf[String]))

    testHarness.open()

    testHarness.setProcessingTime(2)
    testHarness.processElement("a", 2)
    testHarness.processElement("a", 8)
    testHarness.setProcessingTime(18)
    testHarness.processElement("a", 10)

    val output: Array[Boolean] = testHarness.getOutput
      .map(_.asInstanceOf[StreamRecord[JBool]].getValue.asInstanceOf[Boolean])
      .toArray
    val expected = Array(false, false, true)

    assertArrayEquals(expected, output)

    testHarness.close()
  }
}

private class MockedKeyedProcessFunction[K, I](queryConfig: StreamQueryConfig)
  extends KeyedProcessFunctionWithCleanupState[K, I, Boolean](queryConfig) {
  override def open(parameters: Configuration): Unit = {
    initCleanupTimeState("CleanUpState")
  }


  override def processElement(
    value: I,
    ctx: KeyedProcessFunction[K, I, Boolean]#Context,
    out: Collector[Boolean]): Unit = {
    val recordTime = ctx.timestamp()
    registerProcessingCleanupTimer(ctx, recordTime)

    val curTime = ctx.timerService().currentProcessingTime()
    out.collect(needToCleanupState(curTime))
  }
}

