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

<<<<<<< 04fe3fb4543fd075a94184b8c5a6976e3137ba96
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
=======
import java.lang.{Boolean => JBool}

import scala.collection.JavaConversions._
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeDomain
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
>>>>>>> Use keyed process function.
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.runtime.aggregate.KeyedProcessFunctionWithCleanupState
import org.apache.flink.table.runtime.harness.HarnessTestBase
import org.apache.flink.table.runtime.harness.HarnessTestBase.TestStreamQueryConfig
import org.apache.flink.util.Collector
<<<<<<< 04fe3fb4543fd075a94184b8c5a6976e3137ba96

import org.junit.Test
import org.junit.Assert.assertEquals

class KeyedProcessFunctionWithCleanupStateTest extends HarnessTestBase {

  @Test
  def testStateCleaning(): Unit = {
    val queryConfig = new TestStreamQueryConfig(Time.milliseconds(5), Time.milliseconds(10))

    val func = new MockedKeyedProcessFunction(queryConfig)
    val operator = new KeyedProcessOperator(func)

    val testHarness = createHarnessTester(operator,
      new FirstFieldSelector,
=======
import org.junit.Test
import org.junit.Assert.assertArrayEquals

class KeyedProcessFunctionWithCleanupStateTest extends HarnessTestBase {
  @Test
  def testProcessingTimeNeedToCleanup(): Unit = {
    val queryConfig = new StreamQueryConfig()
      .withIdleStateRetentionTime(Time.milliseconds(5), Time.milliseconds(10))

    val func = new MockedKeyedProcessFunction[String, String](
        queryConfig,
        TimeDomain.PROCESSING_TIME)
    val operator = new KeyedProcessOperator(func)

    val testHarness = createHarnessTester(operator,
      new IdentityKeySelector[String],
>>>>>>> Use keyed process function.
      TypeInformation.of(classOf[String]))

    testHarness.open()

<<<<<<< 04fe3fb4543fd075a94184b8c5a6976e3137ba96
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
    registerProcessingCleanupTimer(ctx, curTime)
    state.update(value._2)
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[String, (String, String), String]#OnTimerContext,
      out: Collector[String]): Unit = {

    if (needToCleanupState(timestamp)) {
      cleanupState(state)
    }
  }
}
=======
    testHarness.setProcessingTime(2)
    testHarness.processElement("a", 2)
    testHarness.processElement("a", 8)
    testHarness.setProcessingTime(12)
    testHarness.processElement("a", 10)

    val output: Array[Boolean] = testHarness.getOutput
      .map(_.asInstanceOf[StreamRecord[JBool]].getValue.asInstanceOf[Boolean])
      .toArray
    val expected = Array(false, false, true)

    assertArrayEquals(expected, output)

    testHarness.close()
  }

  @Test
  def testEventTimeNeedToCleanup(): Unit = {
    val queryConfig = new StreamQueryConfig()
      .withIdleStateRetentionTime(Time.milliseconds(5), Time.milliseconds(10))

    val func = new MockedKeyedProcessFunction[String, String](
      queryConfig,
      TimeDomain.EVENT_TIME)
    val operator = new KeyedProcessOperator(func)

    val testHarness = createHarnessTester(operator,
      new IdentityKeySelector[String],
      TypeInformation.of(classOf[String]))

    testHarness.open()

    testHarness.processElement("a", 2)
    testHarness.processElement("a", 8)
    testHarness.processElement("a", 18)

    val output: Array[Boolean] = testHarness.getOutput
      .map(_.asInstanceOf[StreamRecord[JBool]].getValue.asInstanceOf[Boolean])
      .toArray
    val expected = Array(false, false, true)

    assertArrayEquals(expected, output)

    testHarness.close()
  }
}

private class MockedKeyedProcessFunction[K, I](
    private val queryConfig: StreamQueryConfig,
    private val timeDomain: TimeDomain)
  extends KeyedProcessFunctionWithCleanupState[K, I, Boolean](queryConfig) {
  override def open(parameters: Configuration): Unit = {
    initCleanupTimeState("CleanUpState")
  }


  override def processElement(
    value: I,
    ctx: KeyedProcessFunction[K, I, Boolean]#Context,
    out: Collector[Boolean]): Unit = {
    val curTime = getTime(ctx)
    out.collect(needToCleanupState(curTime))

    registerCleanupTimer(ctx, curTime, timeDomain)
  }

  private def getTime(ctx: KeyedProcessFunction[K, I, Boolean]#Context): Long = {
    timeDomain match {
      case TimeDomain.EVENT_TIME => ctx.timestamp()
      case TimeDomain.PROCESSING_TIME => ctx.timerService().currentProcessingTime()
    }
  }
}

>>>>>>> Use keyed process function.
