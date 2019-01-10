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

package org.apache.flink.table.runtime.join

import java.lang.{Long => JLong}
import java.util.Optional

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.{VoidNamespace, VoidNamespaceSerializer}
import org.apache.flink.streaming.api.SimpleTimerService
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, InternalTimer, Triggerable, TwoInputStreamOperator}
import org.apache.flink.table.api.{StreamQueryConfig, Types}
import org.apache.flink.table.runtime.types.CRow

/**
  * An abstract [[TwoInputStreamOperator]] that allows its subclasses to clean
  * up their state based on a TTL. This TTL should be specified in the provided
  * [[StreamQueryConfig]].
  *
  * For each known key, this operator registers a timer (in processing time) to
  * fire after the TTL expires. When the timer fires, the subclass can decide which
  * state to cleanup and what further action to take.
  *
  * This class takes care of maintaining at most one timer per key.
  *
  * <p><b>IMPORTANT NOTE TO USERS:</b> When extending this class, do not use processing time
  * timers in your business logic. The reason is that:
  *
  * 1) if your timers collide with clean up timers and you delete them, then state
  * clean-up will not be performed, and
  *
  * 2) (this one is the reason why this class does not allow to override the onProcessingTime())
  * the onProcessingTime with your logic would be also executed on each clean up timer.
  */
@Internal
abstract class BaseTwoInputStreamOperatorWithStateRetention(
    queryConfig: StreamQueryConfig)
  extends AbstractStreamOperator[CRow]
  with TwoInputStreamOperator[CRow, CRow, CRow]
  with Triggerable[Any, VoidNamespace] {

  private val minRetentionTime: Long = queryConfig.getMinIdleStateRetentionTime
  private val maxRetentionTime: Long = queryConfig.getMaxIdleStateRetentionTime

  private val CLEANUP_TIMESTAMP = "cleanup-timestamp"
  private val TIMERS_STATE_NAME = "timers"

  private var latestRegisteredCleanUpTimer: ValueState[JLong] = _

  protected val stateCleaningEnabled: Boolean = minRetentionTime > 1

  protected var timerService: SimpleTimerService = _

  override def open(): Unit = {

    initializeTimerService()

    if (stateCleaningEnabled) {
      val cleanUpStateDescriptor: ValueStateDescriptor[JLong] =
        new ValueStateDescriptor[JLong](CLEANUP_TIMESTAMP, Types.LONG)
      latestRegisteredCleanUpTimer = getRuntimeContext.getState(cleanUpStateDescriptor)
    }
  }

  private def initializeTimerService(): Unit = {
    val internalTimerService = getInternalTimerService(
      TIMERS_STATE_NAME,
      VoidNamespaceSerializer.INSTANCE,
      this)

    timerService = new SimpleTimerService(internalTimerService)
  }

  /**
    * If the user has specified a `minRetentionTime` and `maxRetentionTime`, this
    * method registers a cleanup timer for `currentProcessingTime + minRetentionTime`.
    *
    * <p>When this timer fires, the [[BaseTwoInputStreamOperatorWithStateRetention.cleanUpState()]]
    * method is called.
    */
  protected def registerProcessingCleanUpTimer(): Unit = {
    if (stateCleaningEnabled) {
      val currentProcessingTime = timerService.currentProcessingTime()
      val currentCleanUpTime = Optional.ofNullable[JLong](latestRegisteredCleanUpTimer.value())

      if (!currentCleanUpTime.isPresent
        || (currentProcessingTime + minRetentionTime) > currentCleanUpTime.get) {

        updateCleanUpTimer(currentProcessingTime, currentCleanUpTime)
      }
    }
  }

  private def updateCleanUpTimer(
      currentProcessingTime: JLong,
      currentCleanUpTime: Optional[JLong]): Unit = {

    if (currentCleanUpTime.isPresent) {
      timerService.deleteProcessingTimeTimer(currentCleanUpTime.get())
    }

    val newCleanUpTime: JLong = currentProcessingTime + maxRetentionTime
    timerService.registerProcessingTimeTimer(newCleanUpTime)
    latestRegisteredCleanUpTimer.update(newCleanUpTime)
  }

  protected def cleanUpLastTimer(): Unit = {
    if (stateCleaningEnabled) {
      val currentCleanUpTime = Optional.ofNullable[JLong](latestRegisteredCleanUpTimer.value())
      if (currentCleanUpTime.isPresent) {
        latestRegisteredCleanUpTimer.clear()
        timerService.deleteProcessingTimeTimer(currentCleanUpTime.get())
      }
    }
  }

  /**
    * The users of this class are not allowed to use processing time timers.
    * See class javadoc.
    */
  override final def onProcessingTime(timer: InternalTimer[Any, VoidNamespace]): Unit = {
    if (stateCleaningEnabled) {
      val timerTime = timer.getTimestamp
      val cleanupTime = latestRegisteredCleanUpTimer.value()

      if (cleanupTime != null && cleanupTime == timerTime) {
        cleanUpState(cleanupTime)
        latestRegisteredCleanUpTimer.clear()
      }
    }
  }

  // ----------------- Abstract Methods -----------------

  /**
    * The method to be called when a cleanup timer fires.
    * @param time The timestamp of the fired timer.
    */
  def cleanUpState(time: Long): Unit
}
