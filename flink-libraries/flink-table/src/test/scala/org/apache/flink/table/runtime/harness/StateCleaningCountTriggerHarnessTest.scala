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
package org.apache.flink.table.runtime.harness

import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.runtime.operators.windowing.TriggerTestHarness
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.runtime.harness.HarnessTestBase.TestStreamQueryConfig
import org.apache.flink.table.runtime.triggers.StateCleaningCountTrigger
import org.junit.Assert.assertEquals
import org.junit.Test

class StateCleaningCountTriggerHarnessTest {
  protected var queryConfig =
    new TestStreamQueryConfig(Time.seconds(2), Time.seconds(3))

  @Test
  def testFiringAndFiringWithPurging(): Unit = {
    val testHarness = new TriggerTestHarness[Any, GlobalWindow](
      StateCleaningCountTrigger.of(queryConfig, 10), new GlobalWindow.Serializer)

    // try to trigger onProcessingTime method via 1, but there is non timer is triggered
    assertEquals(0, testHarness.advanceProcessingTime(1).size())

    // register cleanup timer with 3001
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord(1), GlobalWindow.get))

    // try to trigger onProcessingTime method via 1000, but there is non timer is triggered
    assertEquals(0, testHarness.advanceProcessingTime(1000).size())

    // 1000 + 2000 <= 3001 reuse timer 3001
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord(1), GlobalWindow.get))

    // there are two state entries, one is timer(3001) another is counter(2)
    assertEquals(2, testHarness.numStateEntries)

    // try to trigger onProcessingTime method via 3001, and timer(3001) is triggered
    assertEquals(
      TriggerResult.FIRE_AND_PURGE,
      testHarness.advanceProcessingTime(3001).iterator().next().f1)

    assertEquals(0, testHarness.numStateEntries)

    // 3001 + 2000 >= 3001 register cleanup timer with 6001
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord(1), GlobalWindow.get))

    // try to trigger onProcessingTime method via 4002, but there is non timer is triggered
    assertEquals(0, testHarness.advanceProcessingTime(4002).size())

    // 4002 + 2000 >= 6001 register cleanup timer via 7002
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord(1), GlobalWindow.get))

    // 4002 + 2000 <= 7002 reuse timer 7002
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord(1), GlobalWindow.get))

    // have one timer 7002
    assertEquals(1, testHarness.numProcessingTimeTimers)
    assertEquals(0, testHarness.numEventTimeTimers)
    assertEquals(2, testHarness.numStateEntries)
    assertEquals(2, testHarness.numStateEntries(GlobalWindow.get))

    // 4002 + 2000 <= 7002 reuse timer 7002
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord(1), GlobalWindow.get))

    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord(1), GlobalWindow.get))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord(1), GlobalWindow.get))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord(1), GlobalWindow.get))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord(1), GlobalWindow.get))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord(1), GlobalWindow.get))
    assertEquals(
      TriggerResult.FIRE,
      testHarness.processElement(new StreamRecord(1), GlobalWindow.get))

    // counter of window() is cleared
    assertEquals(1, testHarness.numStateEntries)
    assertEquals(1, testHarness.numStateEntries(GlobalWindow.get))

    // try to trigger onProcessingTime method via 7002, and all states are cleared
    val timesIt = testHarness.advanceProcessingTime(7002).iterator()

    assertEquals(
      TriggerResult.FIRE_AND_PURGE,
      timesIt.next().f1)

    assertEquals(0, testHarness.numStateEntries)
  }

  /**
    * Verify that clear() does not leak across windows.
    */
  @Test
  def testClear() {
    val testHarness = new TriggerTestHarness[Any, GlobalWindow](
      StateCleaningCountTrigger.of(queryConfig, 3),
      new GlobalWindow.Serializer)
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord(1), GlobalWindow.get))
    // have 1 timers
    assertEquals(1, testHarness.numProcessingTimeTimers)
    assertEquals(0, testHarness.numEventTimeTimers)
    assertEquals(2, testHarness.numStateEntries)
    assertEquals(2, testHarness.numStateEntries(GlobalWindow.get))

    testHarness.clearTriggerState(GlobalWindow.get)

    assertEquals(0, testHarness.numStateEntries)
    assertEquals(0, testHarness.numStateEntries(GlobalWindow.get))
  }
}
