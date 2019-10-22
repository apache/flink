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

package org.apache.flink.table.planner.runtime.harness

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.runtime.state.VoidNamespace
import org.apache.flink.streaming.api.operators.InternalTimer
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.planner.runtime.harness.HarnessTestBase.TestingBaseRowKeySelector
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.HEAP_BACKEND
import org.apache.flink.table.runtime.operators.join.temporal.BaseTwoInputStreamOperatorWithStateRetention
import org.apache.flink.table.runtime.util.StreamRecordUtils.record

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.{Description, TypeSafeMatcher}
import org.junit.{After, Before, Test}

import java.lang.{Long => JLong}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Tests for the [[BaseTwoInputStreamOperatorWithStateRetention]].
  */
class AbstractTwoInputStreamOperatorWithTTLTest
  extends HarnessTestBase(HEAP_BACKEND) {

  @transient
  private var recordAForFirstKey: StreamRecord[BaseRow] = _
  @transient
  private var recordBForFirstKey: StreamRecord[BaseRow] = _

  private val minRetentionTime = Time.milliseconds(2)
  private val maxRetentionTime = Time.milliseconds(4)

  private var operatorUnderTest: StubOperatorWithStateTTL = _

  private var testHarness
  : KeyedTwoInputStreamOperatorTestHarness[JLong, BaseRow, BaseRow, BaseRow] = _

  @Before
  def createTestHarness(): Unit = {
    operatorUnderTest = new StubOperatorWithStateTTL(minRetentionTime, maxRetentionTime)
    testHarness = createTestHarness(operatorUnderTest)
    testHarness.open()
    recordAForFirstKey = record(1L: JLong, "hello")
    recordBForFirstKey = record(1L: JLong, "world")
  }

  @After
  def closeTestHarness(): Unit = {
    testHarness.close()
  }

  @Test
  def normalScenarioWorks(): Unit = {
    testHarness.setProcessingTime(1L)
    testHarness.processElement1(recordAForFirstKey)

    testHarness.setProcessingTime(10L)

    assertThat(operatorUnderTest, hasFiredCleanUpTimersForTimestamps(5L))
  }

  @Test
  def whenCurrentTimePlusMinRetentionSmallerThanCurrentCleanupTimeNoNewTimerRegistered(): Unit = {
    testHarness.setProcessingTime(1L)
    testHarness.processElement1(recordAForFirstKey)

    testHarness.setProcessingTime(2L)
    testHarness.processElement1(recordBForFirstKey)

    testHarness.setProcessingTime(20L)

    assertThat(operatorUnderTest, hasFiredCleanUpTimersForTimestamps(5L))
  }

  @Test
  def whenCurrentTimePlusMinRetentionLargerThanCurrentCleanupTimeTimerIsUpdated(): Unit = {
    testHarness.setProcessingTime(1L)
    testHarness.processElement1(recordAForFirstKey)

    testHarness.setProcessingTime(4L)
    testHarness.processElement1(recordBForFirstKey)

    testHarness.setProcessingTime(20L)

    assertThat(operatorUnderTest, hasFiredCleanUpTimersForTimestamps(8L))
  }

  @Test
  def otherSideToSameKeyStateAlsoUpdatesCleanupTimer(): Unit = {
    testHarness.setProcessingTime(1L)
    testHarness.processElement1(recordAForFirstKey)

    testHarness.setProcessingTime(4L)
    testHarness.processElement2(recordBForFirstKey)

    testHarness.setProcessingTime(20L)

    assertThat(operatorUnderTest, hasFiredCleanUpTimersForTimestamps(8L))
  }

  // -------------------------------- Test Utilities --------------------------------

  private def createTestHarness(operator: BaseTwoInputStreamOperatorWithStateRetention) = {
    new KeyedTwoInputStreamOperatorTestHarness[JLong, BaseRow, BaseRow, BaseRow](
      operator,
      new TestingBaseRowKeySelector(0),
      new TestingBaseRowKeySelector(0),
      BasicTypeInfo.LONG_TYPE_INFO,
      1,
      1,
      0)
  }

  // -------------------------------- Matchers --------------------------------

  private def hasFiredCleanUpTimersForTimestamps(timers: JLong*) =
    new TypeSafeMatcher[StubOperatorWithStateTTL]() {

      override protected def matchesSafely(operator: StubOperatorWithStateTTL): Boolean = {
        operator.firedCleanUpTimers.toArray.deep == timers.toArray.deep
      }

      def describeTo(description: Description): Unit = {
        description
          .appendText("a list of timers with timestamps=")
          .appendValue(timers.mkString(","))
      }
    }

  // -------------------------------- Test Classes --------------------------------

  /**
    * A mock [[BaseTwoInputStreamOperatorWithStateRetention]] which registers
    * the timestamps of the clean-up timers that fired (not the registered
    * ones, which can be deleted without firing).
    */
  class StubOperatorWithStateTTL(
      minRetentionTime: Time,
      maxRetentionTime: Time)
    extends BaseTwoInputStreamOperatorWithStateRetention(
      minRetentionTime.toMilliseconds, maxRetentionTime.toMilliseconds) {

    val firedCleanUpTimers: mutable.Buffer[JLong] = ArrayBuffer.empty

    override def cleanupState(time: Long): Unit = {
      firedCleanUpTimers.append(time)
    }

    override def processElement1(element: StreamRecord[BaseRow]): Unit = {
      registerProcessingCleanupTimer()
    }

    override def processElement2(element: StreamRecord[BaseRow]): Unit = {
      registerProcessingCleanupTimer()
    }

    override def onEventTime(timer: InternalTimer[Object, VoidNamespace]): Unit = ()
  }
}
