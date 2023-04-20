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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.annotation.Experimental
import org.apache.flink.configuration.{ConfigOption, ReadableConfig}
import org.apache.flink.configuration.ConfigOptions.key
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.config.ExecutionConfigOptions.IDLE_STATE_RETENTION
import org.apache.flink.table.planner.{JBoolean, JLong}
import org.apache.flink.table.planner.plan.logical.{LogicalWindow, SessionGroupWindow}
import org.apache.flink.table.planner.plan.utils.AggregateUtil.isRowtimeAttribute
import org.apache.flink.table.runtime.operators.window.TimeWindow
import org.apache.flink.table.runtime.operators.window.triggers._

import java.time.Duration

class WindowEmitStrategy(
    isEventTime: JBoolean,
    isSessionWindow: JBoolean,
    earlyFireDelay: Duration,
    earlyFireDelayEnabled: JBoolean,
    lateFireDelay: Duration,
    lateFireDelayEnabled: JBoolean,
    allowLateness: JLong) {

  checkValidation()

  def getAllowLateness: JLong = allowLateness

  private def checkValidation(): Unit = {
    if (isSessionWindow && (earlyFireDelayEnabled || lateFireDelayEnabled)) {
      throw new TableException("Session window doesn't support EMIT strategy currently.")
    }
    if (isEventTime && lateFireDelayEnabled && allowLateness <= 0L) {
      throw new TableException(
        "The 'AFTER WATERMARK' emit strategy requires set " +
          "'allow-lateness' or 'minIdleStateRetentionTime' in table config.")
    }
    if (earlyFireDelayEnabled && (earlyFireDelay == null || earlyFireDelay.toMillis < 0)) {
      throw new TableException(
        "Early-fire delay should not be null or negative value when" +
          "enable early-fire emit strategy.")
    }
    if (lateFireDelayEnabled && (lateFireDelay == null || lateFireDelay.toMillis < 0)) {
      throw new TableException(
        "Late-fire delay should not be null or negative value when" +
          "enable late-fire emit strategy.")
    }
    if (lateFireDelayEnabled && (lateFireDelay.toMillis > allowLateness)) {
      throw new TableException(
        s"Allow-lateness [${allowLateness}ms] should not be smaller than " +
          s"Late-fire delay [${lateFireDelay.toMillis}ms] when enable late-fire emit strategy.")
    }
  }

  def produceUpdates: JBoolean = {
    if (isEventTime) {
      earlyFireDelayEnabled || lateFireDelayEnabled
    } else {
      earlyFireDelayEnabled
    }
  }

  def getTrigger: Trigger[TimeWindow] = {
    val earlyTrigger = createTriggerFromFireDelay(earlyFireDelayEnabled, earlyFireDelay)
    val lateTrigger = createTriggerFromFireDelay(lateFireDelayEnabled, lateFireDelay)

    if (isEventTime) {
      val trigger = EventTimeTriggers.afterEndOfWindow[TimeWindow]()

      (earlyTrigger, lateTrigger) match {
        case (Some(early), Some(late)) => trigger.withEarlyFirings(early).withLateFirings(late)
        case (Some(early), None) => trigger.withEarlyFirings(early)
        case (None, Some(late)) => trigger.withLateFirings(late)
        case (None, None) => trigger
      }
    } else {
      val trigger = ProcessingTimeTriggers.afterEndOfWindow[TimeWindow]()

      // late trigger is ignored, as no late element in processing time
      earlyTrigger match {
        case Some(early) => trigger.withEarlyFirings(early)
        case None => trigger
      }
    }
  }

  override def toString: String = {
    val builder = new StringBuilder
    val earlyString = fireDelayToString(earlyFireDelayEnabled, earlyFireDelay)
    val lateString = fireDelayToString(lateFireDelayEnabled, lateFireDelay)
    if (earlyString != null) {
      builder.append("early ").append(earlyString)
    }
    if (lateString != null) {
      if (earlyString != null) {
        builder.append(", ")
      }
      builder.append("late ").append(lateString)
    }
    builder.toString
  }

  private def createTriggerFromFireDelay(
      enableDelayEmit: JBoolean,
      fireDelay: Duration): Option[Trigger[TimeWindow]] = {
    if (!enableDelayEmit) {
      None
    } else {
      if (fireDelay.toMillis > 0) {
        Some(ProcessingTimeTriggers.every(fireDelay))
      } else {
        Some(ElementTriggers.every())
      }
    }
  }

  private def fireDelayToString(enableDelayEmit: JBoolean, fireDelay: Duration): String = {
    if (!enableDelayEmit) {
      null
    } else {
      if (fireDelay.toMillis > 0) {
        s"delay ${fireDelay.toMillis} millisecond"
      } else {
        "no delay"
      }
    }
  }
}

object WindowEmitStrategy {
  def apply(config: ReadableConfig, window: LogicalWindow): WindowEmitStrategy = {
    val isEventTime = isRowtimeAttribute(window.timeAttribute)
    val isSessionWindow = window.isInstanceOf[SessionGroupWindow]

    val allowLateness = parseAllowLateness(isSessionWindow, config)
    val enableEarlyFireDelay = config.get(TABLE_EXEC_EMIT_EARLY_FIRE_ENABLED)
    val earlyFireDelay: Duration = config
      .getOptional(TABLE_EXEC_EMIT_EARLY_FIRE_DELAY)
      .orElse(null)
    val enableLateFireDelay = config.get(TABLE_EXEC_EMIT_LATE_FIRE_ENABLED)
    val lateFireDelay: Duration = config
      .getOptional(TABLE_EXEC_EMIT_LATE_FIRE_DELAY)
      .orElse(null)
    new WindowEmitStrategy(
      isEventTime,
      isSessionWindow,
      earlyFireDelay,
      enableEarlyFireDelay,
      lateFireDelay,
      enableLateFireDelay,
      allowLateness)
  }

  private def parseAllowLateness(isSessionWindow: Boolean, config: ReadableConfig): Long = {
    val enableLateFireDelay = config.get(TABLE_EXEC_EMIT_LATE_FIRE_ENABLED)
    val emitAllowLateness: Duration = config
      .getOptional(TABLE_EXEC_EMIT_ALLOW_LATENESS)
      .orElse(null)
    if (isSessionWindow) {
      // ignore allow lateness in session window because retraction is not supported
      0L
    } else if (!enableLateFireDelay) {
      // ignore allow lateness if disable late-fire delay
      0L
    } else if (emitAllowLateness != null) {
      // return emit allow-lateness if it is set
      emitAllowLateness.toMillis
    } else if (config.get(IDLE_STATE_RETENTION).toMillis < 0) {
      // min idle state retention time is not set, use 0L as default which means not allow lateness
      0L
    } else {
      // use min idle state retention time as allow lateness
      config.get(IDLE_STATE_RETENTION).toMillis
    }
  }

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_EXEC_EMIT_EARLY_FIRE_ENABLED: ConfigOption[JBoolean] =
    key("table.exec.emit.early-fire.enabled")
      .booleanType()
      .defaultValue(Boolean.box(false))
      .withDescription("Specifies whether to enable early-fire emit." +
        "Early-fire is an emit strategy before watermark advanced to end of window.")

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_EXEC_EMIT_EARLY_FIRE_DELAY: ConfigOption[Duration] =
    key("table.exec.emit.early-fire.delay")
      .durationType()
      .noDefaultValue()
      .withDescription(
        "The early firing delay in milli second, early fire is " +
          "the emit strategy before watermark advanced to end of window. " +
          "< 0 is illegal configuration. " +
          "0 means no delay (fire on every element). " +
          "> 0 means the fire interval. ")

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_EXEC_EMIT_LATE_FIRE_ENABLED: ConfigOption[JBoolean] =
    key("table.exec.emit.late-fire.enabled")
      .booleanType()
      .defaultValue(Boolean.box(false))
      .withDescription("Specifies whether to enable late-fire emit. " +
        "Late-fire is an emit strategy after watermark advanced to end of window.")

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_EXEC_EMIT_LATE_FIRE_DELAY: ConfigOption[Duration] =
    key("table.exec.emit.late-fire.delay")
      .durationType()
      .noDefaultValue()
      .withDescription(
        "The late firing delay in milli second, late fire is " +
          "the emit strategy after watermark advanced to end of window. " +
          "< 0 is illegal configuration. " +
          "0 means no delay (fire on every element). " +
          "> 0 means the fire interval.")

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_EXEC_EMIT_ALLOW_LATENESS: ConfigOption[Duration] =
    key("table.exec.emit.allow-lateness")
      .durationType()
      .noDefaultValue()
      .withDescription("Sets the time by which elements are allowed to be late. " +
        "Elements that arrive behind the watermark by more than the specified time " +
        "will be dropped. " +
        "Note: use the value if it is set, else use 'minIdleStateRetentionTime' in table config." +
        "< 0 is illegal configuration. " +
        "0 means disable allow lateness. " +
        "> 0 means allow-lateness.")
}
