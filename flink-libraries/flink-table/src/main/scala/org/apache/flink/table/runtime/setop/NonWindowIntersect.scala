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
package org.apache.flink.table.runtime.setop

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.table.api.{StreamQueryConfig, Types}
import org.apache.flink.table.runtime.join.CRowWrappingMultiOutputCollector
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.typeutils.TypeCheckUtils.validateEqualsHashCode
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * This function connect left stream and right stream, only used for non-window stream intersect.
  * @param resultType  the output type of intersect
  * @param queryConfig the configuration for the query to generate
  * @param all         indicates an intersect or intersect all
  */
class NonWindowIntersect(
  resultType: TypeInformation[Row],
  queryConfig: StreamQueryConfig,
  all: Boolean)
  extends CoProcessFunction[CRow, CRow, CRow]
  with Logging {

  validateEqualsHashCode("intersect", resultType)

  // state to keep track of the left row
  private var leftState: ValueState[JTuple2[Int, Long]] = _
  // state to keep track of the right row
  private var rightState: ValueState[JTuple2[Int, Long]] = _

  private val minRetentionTime: Long = queryConfig.getMinIdleStateRetentionTime
  private val maxRetentionTime: Long = queryConfig.getMaxIdleStateRetentionTime
  private val stateCleaningEnabled: Boolean = minRetentionTime > 1

  // state to record last timer of left stream, 0 means no timer
  private var leftTimer: ValueState[Long] = _
  // state to record last timer of right stream, 0 means no timer
  private var rightTimer: ValueState[Long] = _

  private var cRowWrapper: CRowWrappingMultiOutputCollector = _

  override def open(parameters: Configuration): Unit = {
    LOG.debug("Instantiating StreamIntersectCoProcessFunction.")
    // initialize left and right state, the first element of tuple2 indicates how many rows of
    // this row, while the second element represents the expired time of this row.
    val tupleTypeInfo = new TupleTypeInfo[JTuple2[Int, Long]](Types.INT, Types.LONG)
    val leftStateDescriptor = new ValueStateDescriptor[JTuple2[Int, Long]](
      "left", tupleTypeInfo)
    val rightStateDescriptor = new ValueStateDescriptor[JTuple2[Int, Long]](
      "right", tupleTypeInfo)
    leftState = getRuntimeContext.getState(leftStateDescriptor)
    rightState = getRuntimeContext.getState(rightStateDescriptor)

    // initialize timer state
    val valueStateDescriptor1 = new ValueStateDescriptor[Long]("leftTimer", classOf[Long])
    leftTimer = getRuntimeContext.getState(valueStateDescriptor1)
    val valueStateDescriptor2 = new ValueStateDescriptor[Long]("rightTimer", classOf[Long])
    rightTimer = getRuntimeContext.getState(valueStateDescriptor2)

    cRowWrapper = new CRowWrappingMultiOutputCollector()
    //we emit one record per process at most
    cRowWrapper.setTimes(1)
  }

  override def processElement1(
    value: CRow,
    ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
    out: Collector[CRow]): Unit = {

    processElement(value, ctx, out, leftState, leftTimer, rightState)
  }

  override def processElement2(
    value: CRow,
    ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
    out: Collector[CRow]): Unit = {

    processElement(value, ctx, out, rightState, rightTimer, leftState)
  }

  private def processElement(
    value: CRow,
    ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
    out: Collector[CRow],
    currentSideState: ValueState[JTuple2[Int, Long]],
    currentSideTimer: ValueState[Long],
    otherSideState: ValueState[JTuple2[Int, Long]]): Unit = {

    val inputRow = value.row
    cRowWrapper.setChange(value.change)
    cRowWrapper.setCollector(out)

    val cntAndExpiredTime = updateState(value, ctx, currentSideState, currentSideTimer)

    val otherSideValue = otherSideState.value()
    if (otherSideValue != null) {
      if (all) {
        if (value.change && cntAndExpiredTime.f0 <= otherSideValue.f0) {
          cRowWrapper.collect(inputRow)
        } else if (!value.change && cntAndExpiredTime.f0 < otherSideValue.f0) {
          cRowWrapper.collect(inputRow)
        }
      } else {
        if (value.change && cntAndExpiredTime.f0 == 1) {
          cRowWrapper.collect(inputRow)
        } else if (!value.change && cntAndExpiredTime.f0 == 0) {
          cRowWrapper.collect(inputRow)
        }
      }
    }
  }

  /**
    * update valueState and TimerState and return the current state
    * @param value
    * @param ctx
    * @param state
    * @param timerState
    * @return
    */
  private def updateState(
    value: CRow,
    ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
    state: ValueState[JTuple2[Int, Long]],
    timerState: ValueState[Long]): JTuple2[Int, Long] = {

    val curProcessTime = ctx.timerService.currentProcessingTime
    val oldCntAndExpiredTime = state.value()
    val cntAndExpiredTime = if (null == oldCntAndExpiredTime) {
      JTuple2.of(0, -1L)
    } else {
      oldCntAndExpiredTime
    }

    cntAndExpiredTime.f1 = getNewExpiredTime(curProcessTime, cntAndExpiredTime.f1)
    if (stateCleaningEnabled && timerState.value() == 0) {
      timerState.update(cntAndExpiredTime.f1)
      ctx.timerService().registerProcessingTimeTimer(cntAndExpiredTime.f1)
    }

    if (!value.change) {
      cntAndExpiredTime.f0 = cntAndExpiredTime.f0 - 1
      if (cntAndExpiredTime.f0 <= 0) {
        state.clear()
      } else {
        state.update(cntAndExpiredTime)
      }
    } else {
      cntAndExpiredTime.f0 = cntAndExpiredTime.f0 + 1
      state.update(cntAndExpiredTime)
    }
    cntAndExpiredTime

  }

  def getNewExpiredTime(
   curProcessTime: Long,
   oldExpiredTime: Long): Long = {
    if (stateCleaningEnabled && curProcessTime + minRetentionTime > oldExpiredTime) {
      curProcessTime + maxRetentionTime
    } else {
      oldExpiredTime
    }
  }

  override def onTimer(
    timestamp: Long,
    ctx: CoProcessFunction[CRow, CRow, CRow]#OnTimerContext,
    out: Collector[CRow]): Unit = {

    if (stateCleaningEnabled && leftTimer.value == timestamp) {
      expireOutTimeRow(
        timestamp,
        leftState,
        leftTimer,
        ctx
      )
    }

    if (stateCleaningEnabled && rightTimer.value == timestamp) {
      expireOutTimeRow(
        timestamp,
        rightState,
        rightTimer,
        ctx
      )
    }
  }

  private def expireOutTimeRow(
    curTime: Long,
    rowState: ValueState[JTuple2[Int, Long]],
    timerState: ValueState[Long],
    ctx: CoProcessFunction[CRow, CRow, CRow]#OnTimerContext): Unit = {

    var validTimestamp: Boolean = false
    val rowValue = rowState.value()
    if (rowValue != null) {
      val recordExpiredTime = rowValue.f1
      if (recordExpiredTime <= curTime) {
        rowState.clear()
      } else {
        // we found a timestamp that is still valid
        validTimestamp = true
      }
    }

    // If the state has non-expired timestamps, register a new timer.
    // Otherwise clean the complete state for this input.
    if (validTimestamp) {
      val cleanupTime = curTime + maxRetentionTime
      ctx.timerService.registerProcessingTimeTimer(cleanupTime)
      timerState.update(cleanupTime)
    } else {
      timerState.clear()
      rowState.clear()
    }
  }
}
