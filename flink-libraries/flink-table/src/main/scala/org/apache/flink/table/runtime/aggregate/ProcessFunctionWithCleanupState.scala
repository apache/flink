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
package org.apache.flink.table.runtime.aggregate

import java.lang.{Long => JLong}

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.state.State
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.{StreamQueryConfig, Types}

abstract class ProcessFunctionWithCleanupState[IN,OUT](qConfig: StreamQueryConfig)
  extends ProcessFunction[IN, OUT]{

  protected val minRetentionTime = qConfig.getMinIdleStateRetentionTime
  protected val maxRetentionTime = qConfig.getMaxIdleStateRetentionTime
  protected val stateCleaningEnabled = minRetentionTime > 1 && maxRetentionTime > 1
  // interval in which clean-up timers are registered
  protected val cleanupTimerInterval = maxRetentionTime - minRetentionTime

  // holds the latest registered cleanup timer
  private var cleanupTimeState: ValueState[JLong] = _

  protected def initCleanupTimeState(stateName: String) {
    if (stateCleaningEnabled) {
      val inputCntDescriptor: ValueStateDescriptor[JLong] =
        new ValueStateDescriptor[JLong](stateName, Types.LONG)
      cleanupTimeState = getRuntimeContext.getState(inputCntDescriptor)
    }
  }

  protected def registerProcessingCleanupTimer(
    ctx: ProcessFunction[IN, OUT]#Context,
    currentTime: Long): Unit = {
    if (stateCleaningEnabled) {

      val earliestCleanup = currentTime + minRetentionTime

      // last registered timer
      val lastCleanupTime = cleanupTimeState.value()

      if (lastCleanupTime == null || earliestCleanup >= lastCleanupTime + cleanupTimerInterval) {
        // we need to register a new timer
        val cleanupTime = earliestCleanup + cleanupTimerInterval
        // register timer and remember clean-up time
        ctx.timerService().registerProcessingTimeTimer(cleanupTime)
        cleanupTimeState.update(cleanupTime)
      }
    }
  }
  protected def registerEventCleanupTimer(
    ctx: ProcessFunction[IN, OUT]#Context,
    currentTime: Long): Unit = {
    if (stateCleaningEnabled) {

      val earliestCleanup = currentTime + minRetentionTime

      // last registered timer
      val lastCleanupTime = cleanupTimeState.value()

      if (lastCleanupTime == null || earliestCleanup >= lastCleanupTime + cleanupTimerInterval) {
        // we need to register a new timer
        val cleanupTime = earliestCleanup + cleanupTimerInterval
        // register timer and remember clean-up time
        ctx.timerService().registerEventTimeTimer(cleanupTime)
        cleanupTimeState.update(cleanupTime)
      }
    }
  }

  protected def cleanupStateOnTimer(timestamp: Long, states: State*): Boolean = {
    var result: Boolean = false
    if (stateCleaningEnabled) {
      val cleanupTime = cleanupTimeState.value()
      if (null != cleanupTime && timestamp == cleanupTime) {
        // clear all state
        states.foreach(_.clear())
        this.cleanupTimeState.clear()
        result = true
      }
    }
    result
  }
}
