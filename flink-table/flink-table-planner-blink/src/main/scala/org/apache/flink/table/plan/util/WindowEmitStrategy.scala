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

import org.apache.flink.table.api.window.TimeWindow
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.plan.logical.{LogicalWindow, SessionGroupWindow}
import org.apache.flink.table.plan.util.AggregateUtil.isRowtimeIndicatorType
import org.apache.flink.table.runtime.window.triggers._

import java.time.Duration

class WindowEmitStrategy(
    isEventTime: Boolean,
    isSessionWindow: Boolean,
    val earlyFireInterval: Long,
    val lateFireInterval: Long,
    allowLateness: Long) {

  def getAllowLateness: Long = allowLateness

  def checkValidation(): Unit = {
    if (isSessionWindow && (earlyFireInterval >= 0 || lateFireInterval >= 0)) {
      throw new TableException("Session window doesn't support EMIT strategy currently.")
    }
    if (isEventTime && lateFireInterval >= 0L && allowLateness <= 0L) {
      throw new TableException("The 'AFTER WATERMARK' emit strategy requires " +
        "'sql.exec.state.ttl.ms' config in job config.")
    }
  }

  def produceUpdates: Boolean = {
    if (isEventTime) {
      allowLateness > 0 || earlyFireInterval >= 0 || lateFireInterval >= 0
    } else {
      earlyFireInterval >= 0
    }
  }

  def getTrigger: Trigger[TimeWindow] = {
    val earlyTrigger = createTriggerFromInterval(earlyFireInterval)
    val lateTrigger = createTriggerFromInterval(lateFireInterval)

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
    val earlyString = intervalToString(earlyFireInterval)
    val lateString = intervalToString(lateFireInterval)
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

  private def createTriggerFromInterval(interval: Long): Option[Trigger[TimeWindow]] = {
    if (interval > 0) {
      Some(ProcessingTimeTriggers.every(Duration.ofMillis(interval)))
    } else if (interval == 0) {
      Some(ElementTriggers.every())
    } else {
      None
    }
  }

  private def intervalToString(interval: Long): String = {
    if (interval > 0) {
      s"delay $interval millisecond"
    } else if (interval == 0) {
      "no delay"
    } else {
      null
    }
  }
}

object WindowEmitStrategy {
  def apply(tableConfig: TableConfig, window: LogicalWindow): WindowEmitStrategy = {
    val isEventTime = isRowtimeIndicatorType(window.timeAttribute.getResultType)
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

    new WindowEmitStrategy(
      isEventTime,
      isSessionWindow,
      tableConfig.getEarlyFireInterval,
      tableConfig.getLateFireInterval,
      allowLateness)
  }
}
