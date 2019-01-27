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
package org.apache.flink.table.plan.util

import org.apache.flink.table.api.TableException
import org.apache.flink.table.runtime.window.triggers.{EventTime, ProcessingTime, Trigger}
import org.junit.{Rule, Test}
import org.junit.Assert._
import org.junit.rules.ExpectedException

class EmitStrategyTest {

  // used for accurate exception information checking.
  val expectedException: ExpectedException = ExpectedException.none()

  @Rule
  def thrown: ExpectedException = expectedException

  @Test
  def testInvalid(): Unit = {
    var emit: EmitStrategy = null

    emit = new EmitStrategy(true, false, Long.MinValue, 0L, 0L)
    thrown.expect(classOf[TableException])
    thrown.expectMessage(
      "'AFTER WATERMARK' emit strategy requires " +
        "'sql.exec.state.ttl.ms' config in job config")
    emit.checkValidation()
  }

  @Test
  def testEventTime(): Unit = {
    var emit: EmitStrategy = null
    var trigger: Trigger[_] = null
    var expectedTrigger: String = null
    var expectedString: String = null

    emit = new EmitStrategy(true, false, 0L, 1000L, 10000L)
    trigger = emit.getTrigger
    expectedTrigger =
      "EventTime.afterEndOfWindow()" +
      ".withEarlyFirings(Element.every())" +
      ".withLateFirings(ProcessingTime.every(1000))"
    expectedString = "early no delay, late delay 1000 millisecond"
    // pass
    emit.checkValidation()
    assertEquals(expectedTrigger, trigger.toString)
    assertEquals(expectedString, emit.toString)
    assertTrue(trigger.isInstanceOf[EventTime.AfterEndOfWindowEarlyAndLate[_]])
    assertTrue(emit.produceUpdates)


    emit = new EmitStrategy(true, false, 1000L, 0L, 10000L)
    trigger = emit.getTrigger
    expectedTrigger = "EventTime.afterEndOfWindow().withEarlyFirings(ProcessingTime.every(1000))"
    expectedString = "early delay 1000 millisecond, late no delay"
    // pass
    emit.checkValidation()
    assertEquals(expectedTrigger, trigger.toString)
    assertEquals(expectedString, emit.toString)
    assertTrue(trigger.isInstanceOf[EventTime.AfterEndOfWindowNoLate[_]])
    assertTrue(emit.produceUpdates)


    emit = new EmitStrategy(true, false, 1000L, Long.MinValue, 10000L)
    trigger = emit.getTrigger
    expectedTrigger = "EventTime.afterEndOfWindow().withEarlyFirings(ProcessingTime.every(1000))"
    expectedString = "early delay 1000 millisecond"
    // pass
    emit.checkValidation()
    assertEquals(expectedTrigger, trigger.toString)
    assertEquals(expectedString, emit.toString)
    assertTrue(trigger.isInstanceOf[EventTime.AfterEndOfWindowNoLate[_]])
    assertTrue(emit.produceUpdates)


    emit = new EmitStrategy(true, false, 0L, Long.MinValue, 10000L)
    trigger = emit.getTrigger
    expectedTrigger = "EventTime.afterEndOfWindow().withEarlyFirings(Element.every())"
    expectedString = "early no delay"
    // pass
    emit.checkValidation()
    assertEquals(expectedTrigger, trigger.toString)
    assertEquals(expectedString, emit.toString)
    assertTrue(trigger.isInstanceOf[EventTime.AfterEndOfWindowNoLate[_]])
    assertTrue(emit.produceUpdates)


    emit = new EmitStrategy(true, false, Long.MinValue, 1000L, 10000L)
    trigger = emit.getTrigger
    expectedTrigger = "EventTime.afterEndOfWindow().withLateFirings(ProcessingTime.every(1000))"
    expectedString = "late delay 1000 millisecond"
    // pass
    emit.checkValidation()
    assertEquals(expectedTrigger, trigger.toString)
    assertEquals(expectedString, emit.toString)
    assertTrue(trigger.isInstanceOf[EventTime.AfterEndOfWindowEarlyAndLate[_]])
    assertTrue(emit.produceUpdates)


    emit = new EmitStrategy(true, false, Long.MinValue, 0L, 1000L)
    trigger = emit.getTrigger
    expectedTrigger = "EventTime.afterEndOfWindow()"
    expectedString = "late no delay"
    // pass
    emit.checkValidation()
    assertEquals(expectedTrigger, trigger.toString)
    assertEquals(expectedString, emit.toString)
    assertTrue(trigger.isInstanceOf[EventTime.AfterEndOfWindow[_]])
    assertTrue(emit.produceUpdates)


    emit = new EmitStrategy(true, false, Long.MinValue, Long.MinValue, 0L)
    trigger = emit.getTrigger
    expectedTrigger = "EventTime.afterEndOfWindow()"
    expectedString = ""
    // pass
    emit.checkValidation()
    assertEquals(expectedTrigger, trigger.toString)
    assertEquals(expectedString, emit.toString)
    assertTrue(trigger.isInstanceOf[EventTime.AfterEndOfWindow[_]])
    assertFalse(emit.produceUpdates)
  }

  @Test
  def testProcessingTime(): Unit = {
    var emit: EmitStrategy = null
    var trigger: Trigger[_] = null
    var expectedTrigger: String = null
    var expectedString: String = null

    emit = new EmitStrategy(false, false, Long.MinValue, Long.MinValue, 0L)
    trigger = emit.getTrigger
    expectedTrigger = "ProcessingTime.afterEndOfWindow()"
    expectedString = ""
    // pass
    emit.checkValidation()
    assertEquals(expectedTrigger, trigger.toString)
    assertEquals(expectedString, emit.toString)
    assertTrue(trigger.isInstanceOf[ProcessingTime.AfterEndOfWindow[_]])
    assertFalse(emit.produceUpdates)

    emit = new EmitStrategy(false, false, 0L, Long.MinValue, 0L)
    trigger = emit.getTrigger
    expectedTrigger = "ProcessingTime.afterEndOfWindow().withEarlyFirings(Element.every())"
    expectedString = "early no delay"
    // pass
    emit.checkValidation()
    assertEquals(expectedTrigger, trigger.toString)
    assertEquals(expectedString, emit.toString)
    assertTrue(trigger.isInstanceOf[ProcessingTime.AfterEndOfWindowNoLate[_]])
    assertTrue(emit.produceUpdates)


    emit = new EmitStrategy(false, false, 1000L, Long.MinValue, 0L)
    trigger = emit.getTrigger
    expectedTrigger =
      "ProcessingTime" +
      ".afterEndOfWindow()" +
      ".withEarlyFirings(ProcessingTime.every(1000))"
    expectedString = "early delay 1000 millisecond"
    // pass
    emit.checkValidation()
    assertEquals(expectedTrigger, trigger.toString)
    assertEquals(expectedString, emit.toString)
    assertTrue(trigger.isInstanceOf[ProcessingTime.AfterEndOfWindowNoLate[_]])
    assertTrue(emit.produceUpdates)
  }
}
