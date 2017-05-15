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
import org.apache.flink.streaming.api.TimeDomain
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.{StreamQueryConfig, Types}

abstract class ProcessFunctionWithCleanupState[IN,OUT](queryConfig: StreamQueryConfig)
  extends ProcessFunction[IN, OUT]{

  protected val minRetentionTime: Long = queryConfig.getMinIdleStateRetentionTime
  protected val maxRetentionTime: Long = queryConfig.getMaxIdleStateRetentionTime
  protected val stateCleaningEnabled: Boolean = minRetentionTime > 1

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

      // last registered timer
      val curCleanupTime = cleanupTimeState.value()

      // check if a cleanup timer is registered and
      // that the current cleanup timer won't delete state we need to keep
      if (curCleanupTime == null || (currentTime + minRetentionTime) > curCleanupTime) {
        // we need to register a new (later) timer
        val cleanupTime = currentTime + maxRetentionTime
        // register timer and remember clean-up time
        ctx.timerService().registerProcessingTimeTimer(cleanupTime)
        cleanupTimeState.update(cleanupTime)
      }
    }
  }

  protected def isProcessingTimeTimer(ctx: OnTimerContext): Boolean = {
    ctx.timeDomain() == TimeDomain.PROCESSING_TIME
  }

  protected def needToCleanupState(timestamp: Long): Boolean = {
    if (stateCleaningEnabled) {
      val cleanupTime = cleanupTimeState.value()
      // check that the triggered timer is the last registered processing time timer.
      null != cleanupTime && timestamp == cleanupTime
    } else {
      false
    }
  }

  protected def cleanupState(states: State*): Unit = {
    // clear all state
    states.foreach(_.clear())
    this.cleanupTimeState.clear()
  }
}
