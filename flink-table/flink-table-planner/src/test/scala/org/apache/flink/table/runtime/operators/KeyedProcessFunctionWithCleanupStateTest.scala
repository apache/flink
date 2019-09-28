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

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.runtime.aggregate.KeyedProcessFunctionWithCleanupState
import org.apache.flink.table.runtime.harness.HarnessTestBase
import org.apache.flink.table.runtime.harness.HarnessTestBase.TestStreamQueryConfig
import org.apache.flink.util.Collector
import org.junit.Assert.assertEquals
import org.junit.Test

class KeyedProcessFunctionWithCleanupStateTest extends HarnessTestBase {

  @Test
  def testStateCleaning(): Unit = {
    val queryConfig = new TestStreamQueryConfig(Time.milliseconds(5), Time.milliseconds(10))

    val func = new MockedKeyedProcessFunction(queryConfig)
    val operator = new KeyedProcessOperator(func)

    val testHarness = createHarnessTester(operator,
      new FirstFieldSelector,
      TypeInformation.of(classOf[String]))

    testHarness.open()

    testHarness.setProcessingTime(1)
    // add state for key "a"
    testHarness.processElement(("a", "payload"), 1)
    // add state for key "b"
    testHarness.processElement(("b", "payload"), 1)

    // check that we have two states (a, b)
    // we check for the double number of states, because KeyedProcessFunctionWithCleanupState
    //   adds one more state per key to hold the cleanup timestamp.
    assertEquals(4, testHarness.numKeyedStateEntries())

    // advance time and add state for key "c"
    testHarness.setProcessingTime(5)
    testHarness.processElement(("c", "payload"), 1)
    // add state for key "a". Timer is not reset, because it is still within minRetentionTime
    testHarness.processElement(("a", "payload"), 1)

    // check that we have three states (a, b, c)
    assertEquals(6, testHarness.numKeyedStateEntries())

    // advance time and update key "b". Timer for "b" is reset to 18
    testHarness.setProcessingTime(8)
    testHarness.processElement(("b", "payload"), 1)
    // check that we have three states (a, b, c)
    assertEquals(6, testHarness.numKeyedStateEntries())

    // advance time to clear state for key "a"
    testHarness.setProcessingTime(11)
    // check that we have two states (b, c)
    assertEquals(4, testHarness.numKeyedStateEntries())

    // advance time to clear state for key "c"
    testHarness.setProcessingTime(15)
    // check that we have one state (b)
    assertEquals(2, testHarness.numKeyedStateEntries())

    // advance time to clear state for key "c"
    testHarness.setProcessingTime(18)
    // check that we have no states
    assertEquals(0, testHarness.numKeyedStateEntries())

    testHarness.close()
  }
}

private class MockedKeyedProcessFunction(queryConfig: StreamQueryConfig)
    extends KeyedProcessFunctionWithCleanupState[String, (String, String), String](queryConfig) {

  var state: ValueState[String] = _

  override def open(parameters: Configuration): Unit = {
    initCleanupTimeState("CleanUpState")
    val stateDesc = new ValueStateDescriptor[String]("testState", classOf[String])
    state = getRuntimeContext.getState(stateDesc)
  }

  override def processElement(
      value: (String, String),
      ctx: KeyedProcessFunction[String, (String, String), String]#Context,
      out: Collector[String]): Unit = {

    val curTime = ctx.timerService().currentProcessingTime()
    processCleanupTimer(ctx, curTime)
    state.update(value._2)
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[String, (String, String), String]#OnTimerContext,
      out: Collector[String]): Unit = {

    if (stateCleaningEnabled) {
      val cleanupTime = cleanupTimeState.value()
      if (null != cleanupTime && timestamp == cleanupTime) {
        // clean up
        cleanupState(state)
      }
    }
  }
}
