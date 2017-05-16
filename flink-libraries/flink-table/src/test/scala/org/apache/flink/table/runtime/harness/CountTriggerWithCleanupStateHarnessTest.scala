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

import com.google.common.collect.Lists
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.windowing.TriggerTestHarness
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.runtime.triggers.CountTriggerWithCleanupState
import org.junit.Assert.assertEquals
import org.junit.Test

class CountTriggerWithCleanupStateHarnessTest {
  protected var queryConfig =
    new StreamQueryConfig().withIdleStateRetentionTime(Time.seconds(2), Time.seconds(3))

  @Test
  def testFiringAndFireingWithPurging(): Unit = {
    val testHarness = new TriggerTestHarness[Any, TimeWindow](
      CountTriggerWithCleanupState.of[TimeWindow](queryConfig, 10), new TimeWindow.Serializer)

    // try to trigger onProcessingTime method via 1, but there is non timer is triggered
    assertEquals(0, testHarness.advanceProcessingTime(1).size())

    // register cleanup timer with 3001
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(0, 9)))

    // try to trigger onProcessingTime method via 1000, but there is non timer is triggered
    assertEquals(0, testHarness.advanceProcessingTime(1000).size())

    // 1000 + 2000 <= 3001 reuse timer 3001
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(0, 9)))

    // there are two state entries, one is timer(3001) another is counter(2)
    assertEquals(2, testHarness.numStateEntries)

    // try to trigger onProcessingTime method via 3001, and timer(3001) is triggered
    assertEquals(
      TriggerResult.FIRE_AND_PURGE,
      testHarness.advanceProcessingTime(3001).iterator().next().f1)

    assertEquals(0, testHarness.numStateEntries)

    // 3001 + 2000 >= 3001 register cleanup timer with 6001, and remove timer 3001
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(0, 9)))

    // try to trigger onProcessingTime method via 4002, but there is non timer is triggered
    assertEquals(0, testHarness.advanceProcessingTime(4002).size())

    // 4002 + 2000 >= 6001 register cleanup timer via 7002, and remove timer 6001
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(0, 9)))

    // 4002 + 2000 <= 7002 reuse timer 7002
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(0, 9)))

    // have one timer 7002
    assertEquals(1, testHarness.numProcessingTimeTimers)
    assertEquals(0, testHarness.numEventTimeTimers)
    assertEquals(2, testHarness.numStateEntries)
    assertEquals(2, testHarness.numStateEntries(new TimeWindow(0, 9)))
    assertEquals(0, testHarness.numStateEntries(new TimeWindow(9, 18)))

    // 4002 + 2000 <= 7002 reuse timer 7002
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(0, 9)))

    // register cleanup timer via 7002 for window (9, 18)
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(9, 18)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(9, 18)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(9, 18)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(9, 18)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(9, 18)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(9, 18)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(9, 18)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(9, 18)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(9, 18)))

    // there are four state entries
    assertEquals(4, testHarness.numStateEntries)
    assertEquals(2, testHarness.numStateEntries(new TimeWindow(0, 9)))
    assertEquals(2, testHarness.numStateEntries(new TimeWindow(9, 18)))

    // the window counter triggered, count >= 10
    assertEquals(
      TriggerResult.FIRE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(9, 18)))

    // counter of window(9, 18) is cleared
    assertEquals(3, testHarness.numStateEntries)

    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(0, 9)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(0, 9)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(0, 9)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(0, 9)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(0, 9)))
    assertEquals(
      TriggerResult.FIRE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(0, 9)))

    // counter of window(0, 9) is cleared
    assertEquals(2, testHarness.numStateEntries)
    assertEquals(1, testHarness.numStateEntries(new TimeWindow(0, 9)))
    assertEquals(1, testHarness.numStateEntries(new TimeWindow(9, 18)))

    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(18, 27)))

    assertEquals(4, testHarness.numStateEntries)

    // try to trigger onProcessingTime method via 7002, and all states are cleared
    assertEquals(
      TriggerResult.FIRE_AND_PURGE,
      testHarness.advanceProcessingTime(7002).iterator().next().f1)

    assertEquals(0, testHarness.numStateEntries)
  }

  /**
    * Verify that clear() does not leak across windows.
    */
  @Test
  def testClear() {
    val testHarness = new TriggerTestHarness[Any, TimeWindow](
      CountTriggerWithCleanupState.of[TimeWindow](queryConfig, 3),
      new TimeWindow.Serializer)
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(0, 2)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(2, 4)))
    // have 2 timers
    assertEquals(2, testHarness.numProcessingTimeTimers)
    assertEquals(0, testHarness.numEventTimeTimers)
    assertEquals(4, testHarness.numStateEntries)
    assertEquals(2, testHarness.numStateEntries(new TimeWindow(0, 2)))
    assertEquals(2, testHarness.numStateEntries(new TimeWindow(2, 4)))
    testHarness.clearTriggerState(new TimeWindow(2, 4))
    assertEquals(2, testHarness.numStateEntries)
    assertEquals(2, testHarness.numStateEntries(new TimeWindow(0, 2)))
    assertEquals(0, testHarness.numStateEntries(new TimeWindow(2, 4)))
    testHarness.clearTriggerState(new TimeWindow(0, 2))
    assertEquals(0, testHarness.numStateEntries)
    assertEquals(0, testHarness.numStateEntries(new TimeWindow(0, 2)))
    assertEquals(0, testHarness.numStateEntries(new TimeWindow(2, 4)))
  }

  @Test
  def testMergingWindows() {
    val testHarness = new TriggerTestHarness[Any, TimeWindow](
      CountTriggerWithCleanupState.of[TimeWindow](queryConfig, 3),
      new TimeWindow.Serializer)

    // first trigger onProcessingTime method via 1
    assertEquals(0, testHarness.advanceProcessingTime(1).size())
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(0, 2)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(2, 4)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(4, 6)))
    // have 3 timers
    assertEquals(3, testHarness.numProcessingTimeTimers)
    assertEquals(0, testHarness.numEventTimeTimers)
    assertEquals(6, testHarness.numStateEntries)
    assertEquals(2, testHarness.numStateEntries(new TimeWindow(0, 2)))
    assertEquals(2, testHarness.numStateEntries(new TimeWindow(2, 4)))
    assertEquals(2, testHarness.numStateEntries(new TimeWindow(4, 6)))
    testHarness.mergeWindows(
      new TimeWindow(0, 4),
      Lists.newArrayList(new TimeWindow(0, 2), new TimeWindow(2, 4)))

    assertEquals(3, testHarness.numStateEntries)
    assertEquals(0, testHarness.numStateEntries(new TimeWindow(0, 2)))
    assertEquals(0, testHarness.numStateEntries(new TimeWindow(2, 4)))
    assertEquals(1, testHarness.numStateEntries(new TimeWindow(0, 4)))
    assertEquals(2, testHarness.numStateEntries(new TimeWindow(4, 6)))
    assertEquals(
      TriggerResult.FIRE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(0, 4)))
    assertEquals(3, testHarness.numStateEntries)
    assertEquals(1, testHarness.numStateEntries(new TimeWindow(0, 4)))
    assertEquals(2, testHarness.numStateEntries(new TimeWindow(4, 6)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(4, 6)))
    assertEquals(
      TriggerResult.FIRE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(4, 6)))
    assertEquals(2, testHarness.numStateEntries)

    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(0, 2)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(2, 4)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(4, 6)))

    // try to trigger onProcessingTime method via 3001 and FIRE_AND_PURGE
    val it = testHarness.advanceProcessingTime(3001).iterator()
    while (it.hasNext) {
      assertEquals(
        TriggerResult.FIRE_AND_PURGE, it.next().f1)
    }

    assertEquals(0, testHarness.numStateEntries)
  }

  @Test
  def testMergeSubsumingWindow() {
    val testHarness = new TriggerTestHarness[Any, TimeWindow](
      CountTriggerWithCleanupState.of[TimeWindow](queryConfig, 3),
      new TimeWindow.Serializer)
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(2, 4)))
    assertEquals(
      TriggerResult.CONTINUE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(4, 6)))
    // have 2 timers
    assertEquals(2, testHarness.numProcessingTimeTimers)
    assertEquals(0, testHarness.numEventTimeTimers)
    assertEquals(4, testHarness.numStateEntries)
    assertEquals(2, testHarness.numStateEntries(new TimeWindow(2, 4)))
    assertEquals(2, testHarness.numStateEntries(new TimeWindow(4, 6)))
    testHarness.mergeWindows(
      new TimeWindow(0, 8),
      Lists.newArrayList(new TimeWindow(2, 4), new TimeWindow(4, 6)))
    assertEquals(1, testHarness.numStateEntries)
    assertEquals(0, testHarness.numStateEntries(new TimeWindow(2, 4)))
    assertEquals(0, testHarness.numStateEntries(new TimeWindow(4, 6)))
    assertEquals(1, testHarness.numStateEntries(new TimeWindow(0, 8)))
    assertEquals(
      TriggerResult.FIRE,
      testHarness.processElement(new StreamRecord[Any](1), new TimeWindow(0, 8)))
    assertEquals(1, testHarness.numStateEntries)

    testHarness.clearTriggerState(new TimeWindow(0, 8))
    assertEquals(0, testHarness.numStateEntries(new TimeWindow(0, 8)))
  }
}
