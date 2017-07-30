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

import org.apache.flink.api.common.functions.RichFlatJoinFunction
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{ResultTypeQueryable, TupleTypeInfo}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.table.api.{StreamQueryConfig, Types}
import org.apache.flink.table.runtime.CRowWrappingMultiOuputCollector
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}

/**
  * Connect data for left stream and right stream. Only use for innerJoin.
  *
  * @param joiner           join function
  * @param leftType         the input type of left stream
  * @param rightType        the input type of right stream
  * @param resultType       the output type of join
  * @param queryConfig
  */
class ProcTimeNonWindowInnerJoin(
    joiner: RichFlatJoinFunction[Row, Row, Row],
    leftType: TypeInformation[Row],
    rightType: TypeInformation[Row],
    resultType: TypeInformation[CRow],
    queryConfig: StreamQueryConfig) extends
  CoProcessFunction[CRow, CRow, CRow] with ResultTypeQueryable[CRow] {


  // state to hold left stream element
  private var leftState: MapState[Row, JTuple2[Int, Long]] = null
  // state to hold right stream element
  private var rightState: MapState[Row, JTuple2[Int, Long]] = null
  private var cRowWrapper: CRowWrappingMultiOuputCollector = null

  private val minRetentionTime: Long = queryConfig.getMinIdleStateRetentionTime
  private val maxRetentionTime: Long = queryConfig.getMaxIdleStateRetentionTime
  private val stateCleaningEnabled: Boolean = minRetentionTime > 1

  // state to record last timer of left stream, 0 means no timer
  private var timerState1: ValueState[Long] = _
  // state to record last timer of right stream, 0 means no timer
  private var timerState2: ValueState[Long] = _


  override def open(parameters: Configuration): Unit = {
    // initialize left and right state
    val tupleTypeInfo = new TupleTypeInfo[JTuple2[Int, Long]](Types.INT, Types.LONG)
    val leftStateDescriptor = new MapStateDescriptor[Row, JTuple2[Int, Long]](
      "left", leftType, tupleTypeInfo)
    val rightStateDescriptor = new MapStateDescriptor[Row, JTuple2[Int, Long]](
      "right", rightType, tupleTypeInfo)
    leftState = getRuntimeContext.getMapState(leftStateDescriptor)
    rightState = getRuntimeContext.getMapState(rightStateDescriptor)

    // initialize timer state
    val valueStateDescriptor1 = new ValueStateDescriptor[Long]("timervaluestate1", classOf[Long])
    timerState1 = getRuntimeContext.getState(valueStateDescriptor1)
    val valueStateDescriptor2 = new ValueStateDescriptor[Long]("timervaluestate2", classOf[Long])
    timerState2 = getRuntimeContext.getState(valueStateDescriptor2)

    cRowWrapper = new CRowWrappingMultiOuputCollector()
    joiner.setRuntimeContext(getRuntimeContext)
    joiner.open(parameters)
  }

  /**
    * Process left stream records
    *
    * @param valueC The input value.
    * @param ctx   The ctx to register timer or get current time
    * @param out   The collector for returning result values.
    *
    */
  override def processElement1(
      valueC: CRow,
      ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
      out: Collector[CRow]): Unit = {

    processElement(valueC, ctx, out, timerState1, leftState, rightState, true)
  }

  /**
    * Process right stream records
    *
    * @param valueC The input value.
    * @param ctx   The ctx to register timer or get current time
    * @param out   The collector for returning result values.
    *
    */
  override def processElement2(
      valueC: CRow,
      ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
      out: Collector[CRow]): Unit = {

    processElement(valueC, ctx, out, timerState2, rightState, leftState, false)
  }


  /**
    * Called when a processing timer trigger.
    * Expire left/right records which are expired in left and right state.
    *
    * @param timestamp The timestamp of the firing timer.
    * @param ctx       The ctx to register timer or get current time
    * @param out       The collector for returning result values.
    */
  override def onTimer(
      timestamp: Long,
      ctx: CoProcessFunction[CRow, CRow, CRow]#OnTimerContext,
      out: Collector[CRow]): Unit = {

    if (stateCleaningEnabled && timerState1.value == timestamp) {
      expireOutTimeRow(
        timestamp,
        leftState,
        timerState1,
        ctx
      )
    }

    if (stateCleaningEnabled && timerState2.value == timestamp) {
      expireOutTimeRow(
        timestamp,
        rightState,
        timerState2,
        ctx
      )
    }
  }


  def getNewExpiredTime(
      curProcessTime: Long,
      oldExpiredTime: Long): Long = {

    var newExpiredTime = oldExpiredTime
    if (stateCleaningEnabled) {
      if (-1 == oldExpiredTime || (curProcessTime + minRetentionTime) > oldExpiredTime) {
        newExpiredTime = curProcessTime + maxRetentionTime
      }
    }
    newExpiredTime
  }

  /**
    * Puts or Retract an element from the input stream into state and search the other state to
    * output records meet the condition. Records will be expired in state if state retention time
    * has been specified.
    */
  def processElement(
      value: CRow,
      ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
      out: Collector[CRow],
      timerState: ValueState[Long],
      currentSideState: MapState[Row, JTuple2[Int, Long]],
      otherSideState: MapState[Row, JTuple2[Int, Long]],
      isLeft: Boolean): Unit = {

    cRowWrapper.out = out
    cRowWrapper.setChange(value.change)

    val curProcessTime = ctx.timerService.currentProcessingTime
    var oldCnt = 0
    var oldExpiredTime: Long = -1

    val currentRowCntAndExpiredTime = currentSideState.get(value.row)
    if (currentRowCntAndExpiredTime != null) {
      oldCnt = currentRowCntAndExpiredTime.f0
      oldExpiredTime = currentRowCntAndExpiredTime.f1
    }

    val newExpiredTime = getNewExpiredTime(curProcessTime, oldExpiredTime)
    if (stateCleaningEnabled && timerState.value() == 0) {
      timerState.update(newExpiredTime)
      ctx.timerService().registerProcessingTimeTimer(newExpiredTime)
    }

    // update current side stream state
    if (!value.asInstanceOf[CRow].change) {
      oldCnt = oldCnt - 1
      if (oldCnt <= 0) {
        currentSideState.remove(value.row)
      } else {
        currentSideState.put(value.row, JTuple2.of(oldCnt, newExpiredTime))
      }
    } else {
      oldCnt = oldCnt + 1
      currentSideState.put(value.row, JTuple2.of(oldCnt, newExpiredTime))
    }

    val otherSideRowsIterator = otherSideState.keys().iterator()
    // join other side data
    while (otherSideRowsIterator.hasNext) {
      val otherSideRow = otherSideRowsIterator.next()
      val cntAndExpiredTime = otherSideState.get(otherSideRow)
      // if other side record is expired
      if (stateCleaningEnabled && curProcessTime >= cntAndExpiredTime.f1) {
        otherSideRowsIterator.remove()
      }
      // if other side record is valid
      else {
        cRowWrapper.setTimes(cntAndExpiredTime.f0)
        if (isLeft) {
          joiner.join(value.row, otherSideRow, cRowWrapper)
        } else {
          joiner.join(otherSideRow, value.row, cRowWrapper)
        }
      }
    }
  }


  /**
    * Removes records which are expired from the state. Registers a new timer if the state still
    * holds records after the clean-up.
    */
  private def expireOutTimeRow(
      curTime: Long,
      rowMapState: MapState[Row, JTuple2[Int, Long]],
      timerState: ValueState[Long],
      ctx: CoProcessFunction[CRow, CRow, CRow]#OnTimerContext): Unit = {

    val keyIter = rowMapState.keys().iterator()
    var validTimestamp: Boolean = false

    while (keyIter.hasNext) {
      val record = keyIter.next
      val recordExpiredTime = rowMapState.get(record).f1
      if (recordExpiredTime <= curTime) {
        keyIter.remove()
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
      rowMapState.clear()
    }
  }

  override def getProducedType: TypeInformation[CRow] = resultType
}

