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

import java.lang.{Long => JLong}

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.runtime.state.VoidNamespace
import org.apache.flink.streaming.api.operators.InternalTimer
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.runtime.harness.HarnessTestBase.{TestTableConfig, TupleRowKeySelector}
import org.apache.flink.table.runtime.join.BaseTwoInputStreamOperatorWithStateRetention
import org.apache.flink.table.runtime.types.CRow
import org.hamcrest.{Description, TypeSafeMatcher}
import org.junit.{After, Before, Test}
import org.hamcrest.MatcherAssert.assertThat

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Tests for the [[BaseTwoInputStreamOperatorWithStateRetention]].
  */
class BaseTwoInputStreamOperatorWithStateRetentionTest extends HarnessTestBase {

  private val recordAForFirstKey = new StreamRecord(CRow(1L: JLong, "hello"))
  private val recordBForFirstKey = new StreamRecord(CRow(1L: JLong, "world"))

  private val config = new TestTableConfig
  config.setIdleStateRetentionTime(Time.milliseconds(2), Time.milliseconds(4))

  private var operatorUnderTest: StubOperatorWithStateTTL = _

  private var testHarness: KeyedTwoInputStreamOperatorTestHarness[JLong, CRow, CRow, CRow] = _

  @Before
  def createTestHarness(): Unit = {
    operatorUnderTest = new StubOperatorWithStateTTL(
      config.getMinIdleStateRetentionTime,
      config.getMaxIdleStateRetentionTime)
    testHarness = createTestHarness(operatorUnderTest)
    testHarness.open()
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
    new KeyedTwoInputStreamOperatorTestHarness[JLong, CRow, CRow, CRow](
      operator,
      new TupleRowKeySelector[JLong](0),
      new TupleRowKeySelector[JLong](0),
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
  class StubOperatorWithStateTTL(minRetentionTime: Long, maxRetentionTime: Long)
    extends BaseTwoInputStreamOperatorWithStateRetention(minRetentionTime, maxRetentionTime) {

    val firedCleanUpTimers: mutable.Buffer[JLong] = ArrayBuffer.empty

    override def cleanUpState(time: Long): Unit = {
      firedCleanUpTimers.append(time)
    }

    override def processElement1(element: StreamRecord[CRow]): Unit = {
      registerProcessingCleanUpTimer()
    }

    override def processElement2(element: StreamRecord[CRow]): Unit = {
      registerProcessingCleanUpTimer()
    }

    override def onEventTime(timer: InternalTimer[Any, VoidNamespace]): Unit = ()
  }
}
