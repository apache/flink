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
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.configuration.ConfigOptions.key
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.planner.plan.logical.{LogicalWindow, SessionGroupWindow}
import org.apache.flink.table.planner.plan.utils.AggregateUtil.isRowtimeAttribute
import org.apache.flink.table.planner.utils.TableConfigUtils.getMillisecondFromConfigDuration
import org.apache.flink.table.runtime.operators.window.TimeWindow
import org.apache.flink.table.runtime.operators.window.triggers._

import java.lang.{Boolean, Long}
import java.time.Duration

class WindowEmitStrategy(
    isEventTime: Boolean,
    isSessionWindow: Boolean,
    earlyFireDelay: Long,
    earlyFireDelayEnabled: Boolean,
    lateFireDelay: Long,
    lateFireDelayEnabled: Boolean,
    allowLateness: Long) {

  checkValidation()

  def getAllowLateness: Long = allowLateness

  private def checkValidation(): Unit = {
    if (isSessionWindow && (earlyFireDelayEnabled || lateFireDelayEnabled)) {
      throw new TableException("Session window doesn't support EMIT strategy currently.")
    }
    if (isEventTime && lateFireDelayEnabled && allowLateness <= 0L) {
      throw new TableException("The 'AFTER WATERMARK' emit strategy requires set " +
        "'minIdleStateRetentionTime' in table config.")
    }
    if (earlyFireDelayEnabled && (earlyFireDelay == null || earlyFireDelay < 0)) {
      throw new TableException("Early-fire delay should not be null or negative value when" +
        "enable early-fire emit strategy.")
    }
    if (lateFireDelayEnabled && (lateFireDelay == null || lateFireDelay < 0)) {
      throw new TableException("Late-fire delay should not be null or negative value when" +
        "enable late-fire emit strategy.")
    }
  }

  def produceUpdates: Boolean = {
    if (isEventTime) {
      allowLateness > 0 || earlyFireDelayEnabled || lateFireDelayEnabled
    } else {
      earlyFireDelayEnabled
    }
  }

  def getTrigger: Trigger[TimeWindow] = {
    val earlyTrigger = createTriggerFromInterval(earlyFireDelayEnabled, earlyFireDelay)
    val lateTrigger = createTriggerFromInterval(lateFireDelayEnabled, lateFireDelay)

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
    val earlyString = intervalToString(earlyFireDelayEnabled, earlyFireDelay)
    val lateString = intervalToString(lateFireDelayEnabled, lateFireDelay)
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

  private def createTriggerFromInterval(
      enableDelayEmit: Boolean,
      interval: Long): Option[Trigger[TimeWindow]] = {
    if (!enableDelayEmit) {
      None
    } else {
      if (interval > 0) {
        Some(ProcessingTimeTriggers.every(Duration.ofMillis(interval)))
      } else {
        Some(ElementTriggers.every())
      }
    }
  }

  private def intervalToString(enableDelayEmit: Boolean, interval: Long): String = {
    if (!enableDelayEmit) {
      null
    } else {
      if (interval > 0) {
        s"delay $interval millisecond"
      } else {
        "no delay"
      }
    }
  }
}

object WindowEmitStrategy {
  def apply(tableConfig: TableConfig, window: LogicalWindow): WindowEmitStrategy = {
    val isEventTime = isRowtimeAttribute(window.timeAttribute)
    val isSessionWindow = window.isInstanceOf[SessionGroupWindow]

    val allowLateness = if (isSessionWindow) {
      // ignore allow lateness in session window because retraction is not supported
      0L
    } else if (tableConfig.getMinIdleStateRetentionTime < 0) {
      // min idle state retention time is not set, use 0L as default which means not allow lateness
      0L
    } else {
      // use min idle state retention time as allow lateness
      tableConfig.getMinIdleStateRetentionTime
    }
    val enableEarlyFireDelay = tableConfig.getConfiguration.getBoolean(
      TABLE_EXEC_EMIT_EARLY_FIRE_ENABLED)
    val earlyFireDelay = getMillisecondFromConfigDuration(
      tableConfig, TABLE_EXEC_EMIT_EARLY_FIRE_DELAY)
    val enableLateFireDelay = tableConfig.getConfiguration.getBoolean(
      TABLE_EXEC_EMIT_LATE_FIRE_ENABLED)
    val lateFireDelay = getMillisecondFromConfigDuration(
      tableConfig, TABLE_EXEC_EMIT_LATE_FIRE_DELAY)
    new WindowEmitStrategy(
      isEventTime,
      isSessionWindow,
      earlyFireDelay,
      enableEarlyFireDelay,
      lateFireDelay,
      enableLateFireDelay,
      allowLateness)
  }

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_EXEC_EMIT_EARLY_FIRE_ENABLED: ConfigOption[Boolean] =
  key("table.exec.emit.early-fire.enabled")
      .defaultValue(Boolean.valueOf(false))
      .withDescription("Specifies whether to enable early-fire emit." +
          "Early-fire is an emit strategy before watermark advanced to end of window.")

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_EXEC_EMIT_EARLY_FIRE_DELAY: ConfigOption[String] =
  key("table.exec.emit.early-fire.delay")
      .noDefaultValue
      .withDescription("The early firing delay in milli second, early fire is " +
          "the emit strategy before watermark advanced to end of window. " +
          "< 0 is illegal configuration. " +
          "0 means no delay (fire on every element). " +
          "> 0 means the fire interval. ")

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_EXEC_EMIT_LATE_FIRE_ENABLED: ConfigOption[Boolean] =
  key("table.exec.emit.late-fire.enabled")
      .defaultValue(Boolean.valueOf(false))
      .withDescription("Specifies whether to enable late-fire emit. " +
          "Late-fire is an emit strategy after watermark advanced to end of window.")

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_EXEC_EMIT_LATE_FIRE_DELAY: ConfigOption[String] =
  key("table.exec.emit.late-fire.delay")
      .noDefaultValue
      .withDescription("The late firing delay in milli second, late fire is " +
          "the emit strategy after watermark advanced to end of window. " +
          "< 0 is illegal configuration. " +
          "0 means no delay (fire on every element). " +
          "> 0 means the fire interval.")

}
