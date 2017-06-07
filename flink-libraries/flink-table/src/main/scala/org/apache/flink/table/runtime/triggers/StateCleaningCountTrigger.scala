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
package org.apache.flink.table.runtime.triggers

import java.lang.{Long => JLong}

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state._
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.table.api.{StreamQueryConfig, Types}
import org.apache.flink.table.runtime.triggers.StateCleaningCountTrigger.Sum

/**
  * A [[Trigger]] that fires once the count of elements in a pane reaches the given count
  * or the cleanup timer is triggered.
  */
class StateCleaningCountTrigger(queryConfig: StreamQueryConfig, maxCount: Long)
  extends Trigger[Any, GlobalWindow] {

  protected val minRetentionTime: Long = queryConfig.getMinIdleStateRetentionTime
  protected val maxRetentionTime: Long = queryConfig.getMaxIdleStateRetentionTime
  protected val stateCleaningEnabled: Boolean = minRetentionTime > 1

  private val stateDesc =
    new ReducingStateDescriptor[JLong]("count", new Sum, Types.LONG)

  private val cleanupStateDesc =
    new ValueStateDescriptor[JLong]("countCleanup", Types.LONG)

  override def canMerge = false

  override def toString: String = "CountTriggerGlobalWindowithCleanupState(" +
    "minIdleStateRetentionTime=" + queryConfig.getMinIdleStateRetentionTime + ", " +
    "maxIdleStateRetentionTime=" + queryConfig.getMaxIdleStateRetentionTime + ", " +
    "maxCount=" + maxCount + ")"

  override def onElement(
      element: Any,
      timestamp: Long,
      window: GlobalWindow,
      ctx: TriggerContext): TriggerResult = {

    val currentTime = ctx.getCurrentProcessingTime

    // register cleanup timer
    if (stateCleaningEnabled) {
      // last registered timer
      val curCleanupTime = ctx.getPartitionedState(cleanupStateDesc).value()

      // check if a cleanup timer is registered and
      // that the current cleanup timer won't delete state we need to keep
      if (curCleanupTime == null || (currentTime + minRetentionTime) > curCleanupTime) {
        // we need to register a new (later) timer
        val cleanupTime = currentTime + maxRetentionTime
        // register timer and remember clean-up time
        ctx.registerProcessingTimeTimer(cleanupTime)

        ctx.getPartitionedState(cleanupStateDesc).update(cleanupTime)
      }
    }

    val count = ctx.getPartitionedState(stateDesc)
    count.add(1L)

    if (count.get >= maxCount) {
      count.clear()
      TriggerResult.FIRE
    } else {
      TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(
      time: Long,
      window: GlobalWindow,
      ctx: TriggerContext): TriggerResult = {

    if (stateCleaningEnabled) {
      val cleanupTime = ctx.getPartitionedState(cleanupStateDesc).value()
      // check that the triggered timer is the last registered processing time timer.
      if (null != cleanupTime && time == cleanupTime) {
        clear(window, ctx)
        return TriggerResult.FIRE_AND_PURGE
      }
    }
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long, window: GlobalWindow, ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: GlobalWindow, ctx: TriggerContext): Unit = {
    ctx.getPartitionedState(stateDesc).clear()
    ctx.getPartitionedState(cleanupStateDesc).clear()
  }

}

object StateCleaningCountTrigger {

  /**
    * Create a [[StateCleaningCountTrigger]] instance.
    *
    * @param queryConfig query configuration.
    * @param maxCount The count of elements at which to fire.
    */
  def of(queryConfig: StreamQueryConfig, maxCount: Long): StateCleaningCountTrigger =
    new StateCleaningCountTrigger(queryConfig, maxCount)

  class Sum extends ReduceFunction[JLong] {
    override def reduce(value1: JLong, value2: JLong): JLong = value1 + value2
  }

}
