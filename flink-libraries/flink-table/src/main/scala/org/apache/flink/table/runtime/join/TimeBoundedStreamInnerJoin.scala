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

import java.util.{ArrayList, List => JList}

import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.table.api.Types
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.runtime.CRowWrappingCollector
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * A CoProcessFunction to execute time-bounded stream inner-join.
  * Two kinds of time criteria:
  * "L.time between R.time + X and R.time + Y" or "R.time between L.time - Y and L.time - X".
  *
  * @param leftLowerBound  the lower bound for the left stream (X in the criteria)
  * @param leftUpperBound  the upper bound for the left stream (Y in the criteria)
  * @param allowedLateness the lateness allowed for the two streams
  * @param leftType        the input type of left stream
  * @param rightType       the input type of right stream
  * @param genJoinFuncName the function code of other non-equi conditions
  * @param genJoinFuncCode the function name of other non-equi conditions
  *
  */
abstract class TimeBoundedStreamInnerJoin(
    private val leftLowerBound: Long,
    private val leftUpperBound: Long,
    private val allowedLateness: Long,
    private val leftType: TypeInformation[Row],
    private val rightType: TypeInformation[Row],
    private val genJoinFuncName: String,
    private val genJoinFuncCode: String,
    private val leftTimeIdx: Int,
    private val rightTimeIdx: Int)
    extends CoProcessFunction[CRow, CRow, CRow]
    with Compiler[FlatJoinFunction[Row, Row, Row]]
    with Logging {

  private var cRowWrapper: CRowWrappingCollector = _

  // the join function for other conditions
  private var joinFunction: FlatJoinFunction[Row, Row, Row] = _

  // cache to store rows from the left stream
  private var leftCache: MapState[Long, JList[Row]] = _
  // cache to store rows from the right stream
  private var rightCache: MapState[Long, JList[Row]] = _

  // state to record the timer on the left stream. 0 means no timer set
  private var leftTimerState: ValueState[Long] = _
  // state to record the timer on the right stream. 0 means no timer set
  private var rightTimerState: ValueState[Long] = _

  private val leftRelativeSize: Long = -leftLowerBound
  private val rightRelativeSize: Long = leftUpperBound

  protected var leftOperatorTime: Long = 0L
  protected var rightOperatorTime: Long = 0L

  // for delayed cleanup
  private val cleanupDelay = (leftRelativeSize + rightRelativeSize) / 2

  if (allowedLateness < 0) {
    throw new IllegalArgumentException("The allowed lateness must be non-negative.")
  }

  /**
    * Get the maximum interval between receiving a row and emitting it (as part of a joined result).
    * Only reasonable for row time join.
    *
    * @return the maximum delay for the outputs
    */
  def getMaxOutputDelay: Long = Math.max(leftRelativeSize, rightRelativeSize) + allowedLateness

  override def open(config: Configuration) {
    LOG.debug(s"Compiling JoinFunction: $genJoinFuncName \n\n " +
      s"Code:\n$genJoinFuncCode")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genJoinFuncName,
      genJoinFuncCode)
    LOG.debug("Instantiating JoinFunction.")
    joinFunction = clazz.newInstance()

    cRowWrapper = new CRowWrappingCollector()
    cRowWrapper.setChange(true)

    // Initialize the data caches.
    val leftListTypeInfo: TypeInformation[JList[Row]] = new ListTypeInfo[Row](leftType)
    val leftStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]](
        "InnerJoinLeftCache",
        Types.LONG.asInstanceOf[TypeInformation[Long]],
        leftListTypeInfo)
    leftCache = getRuntimeContext.getMapState(leftStateDescriptor)

    val rightListTypeInfo: TypeInformation[JList[Row]] = new ListTypeInfo[Row](rightType)
    val rightStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]](
        "InnerJoinRightCache",
        Types.LONG.asInstanceOf[TypeInformation[Long]],
        rightListTypeInfo)
    rightCache = getRuntimeContext.getMapState(rightStateDescriptor)

    // Initialize the timer states.
    val leftTimerStateDesc: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("InnerJoinLeftTimerState", classOf[Long])
    leftTimerState = getRuntimeContext.getState(leftTimerStateDesc)

    val rightTimerStateDesc: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("InnerJoinRightTimerState", classOf[Long])
    rightTimerState = getRuntimeContext.getState(rightTimerStateDesc)
  }

  /**
    * Process rows from the left stream.
    */
  override def processElement1(
      cRowValue: CRow,
      ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
      out: Collector[CRow]): Unit = {
    updateOperatorTime(ctx)
    val leftRow = cRowValue.row
    val timeForLeftRow: Long = getTimeForLeftStream(ctx, leftRow)
    val rightQualifiedLowerBound: Long = timeForLeftRow - rightRelativeSize
    val rightQualifiedUpperBound: Long = timeForLeftRow + leftRelativeSize
    val rightExpirationTime = calExpirationTime(leftOperatorTime, rightRelativeSize)
    cRowWrapper.out = out

    if (!isRowTooLate(timeForLeftRow, leftOperatorTime)) {
      if (rightOperatorTime < rightQualifiedUpperBound) {
        // Put the leftRow into the cache for later use.
        var leftRowList = leftCache.get(timeForLeftRow)
        if (null == leftRowList) {
          leftRowList = new ArrayList[Row](1)
        }
        leftRowList.add(leftRow)
        leftCache.put(timeForLeftRow, leftRowList)
        if (rightTimerState.value == 0) {
          // Register a timer on the RIGHT stream to remove rows.
          registerCleanUpTimer(ctx, timeForLeftRow, rightTimerState, leftRow = true)
        }
      }

      // Join the leftRow with rows from the right cache.
      val rightIterator = rightCache.iterator()
      while (rightIterator.hasNext) {
        val rightEntry = rightIterator.next
        val rightTime = rightEntry.getKey
        if (rightTime >= rightQualifiedLowerBound && rightTime <= rightQualifiedUpperBound) {
          val rightRows = rightEntry.getValue
          var i = 0
          while (i < rightRows.size) {
            joinFunction.join(leftRow, rightRows.get(i), cRowWrapper)
            i += 1
          }
        } else if (rightTime <= rightExpirationTime) {
          // eager remove
          rightIterator.remove()
        }// We could do the short-cutting optimization here once we get a state with ordered keys.
      }
    }
  }

  /**
    * Process rows from the right stream.
    */
  override def processElement2(
      cRowValue: CRow,
      ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
      out: Collector[CRow]): Unit = {
    updateOperatorTime(ctx)
    val rightRow = cRowValue.row
    val timeForRightRow: Long = getTimeForRightStream(ctx, rightRow)
    val leftQualifiedLowerBound: Long = timeForRightRow - leftRelativeSize
    val leftQualifiedUpperBound: Long =  timeForRightRow + rightRelativeSize
    val leftExpirationTime = calExpirationTime(rightOperatorTime, leftRelativeSize)
    cRowWrapper.out = out

    if (!isRowTooLate(timeForRightRow, rightOperatorTime)) {
      if (leftOperatorTime < leftQualifiedUpperBound) {
        // Put the rightRow into the cache for later use.
        var rightRowList = rightCache.get(timeForRightRow)
        if (null == rightRowList) {
          rightRowList = new ArrayList[Row](1)
        }
        rightRowList.add(rightRow)
        rightCache.put(timeForRightRow, rightRowList)
        if (leftTimerState.value == 0) {
          // Register a timer on the LEFT stream to remove rows.
          registerCleanUpTimer(ctx, timeForRightRow, leftTimerState, leftRow = false)
        }
      }

      // Join the rightRow with rows from the left cache.
      val leftIterator = leftCache.iterator()
      while (leftIterator.hasNext) {
        val leftEntry = leftIterator.next
        val leftTime = leftEntry.getKey
        if (leftTime >= leftQualifiedLowerBound && leftTime <= leftQualifiedUpperBound) {
          val leftRows = leftEntry.getValue
          var i = 0
          while (i < leftRows.size) {
            joinFunction.join(leftRows.get(i), rightRow, cRowWrapper)
            i += 1
          }
        } else if (leftTime <= leftExpirationTime) {
          // eager remove
          leftIterator.remove()
        } // We could do the short-cutting optimization here once we get a state with ordered keys.
      }
    }
  }

  /**
    * Called when a registered timer is fired.
    * Remove rows whose timestamps are earlier than the expiration time,
    * and register a new timer for the remaining rows.
    *
    * @param timestamp the timestamp of the timer
    * @param ctx       the context to register timer or get current time
    * @param out       the collector for returning result values
    */
  override def onTimer(
      timestamp: Long,
      ctx: CoProcessFunction[CRow, CRow, CRow]#OnTimerContext,
      out: Collector[CRow]): Unit = {
    updateOperatorTime(ctx)
    // In the future, we should separate the left and right watermarks. Otherwise, the
    // registered timer of the faster stream will be delayed, even if the watermarks have
    // already been emitted by the source.
    if (leftTimerState.value == timestamp) {
      val rightExpirationTime = calExpirationTime(leftOperatorTime, rightRelativeSize)
      removeExpiredRows(
        rightExpirationTime,
        rightCache,
        leftTimerState,
        ctx,
        removeLeft = false
      )
    }

    if (rightTimerState.value == timestamp) {
      val leftExpirationTime = calExpirationTime(rightOperatorTime, leftRelativeSize)
      removeExpiredRows(
        leftExpirationTime,
        leftCache,
        rightTimerState,
        ctx,
        removeLeft = true
      )
    }
  }

  /**
    * Calculate the expiration time with the given operator time and relative window size.
    *
    * @param operatorTime the operator time
    * @param relativeSize the relative window size
    * @return the expiration time for cached rows
    */
  private def calExpirationTime(operatorTime: Long, relativeSize: Long): Long = {
    if (operatorTime < Long.MaxValue) {
      operatorTime - relativeSize - allowedLateness - 1
    } else {
      Long.MaxValue
    }
  }

  /**
    * Register a timer for cleaning up rows in a specified time.
    *
    * @param ctx        the context to register timer
    * @param rowTime    time for the input row
    * @param timerState stores the timestamp for the next timer
    * @param leftRow    whether this row comes from the left stream
    */
  private def registerCleanUpTimer(
      ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
      rowTime: Long,
      timerState: ValueState[Long],
      leftRow: Boolean): Unit = {
    val cleanupTime = if (leftRow) {
      rowTime + leftRelativeSize + cleanupDelay + allowedLateness + 1
    } else {
      rowTime + rightRelativeSize + cleanupDelay + allowedLateness + 1
    }
    registerTimer(ctx, cleanupTime)
    timerState.update(cleanupTime)
  }

  /**
    * Remove the expired rows. Register a new timer if the cache still holds valid rows
    * after the cleaning up.
    *
    * @param expirationTime the expiration time for this cache
    * @param rowCache       the row cache
    * @param timerState     timer state for the opposite stream
    * @param ctx            the context to register the cleanup timer
    * @param removeLeft     whether to remove the left rows
    */
  private def removeExpiredRows(
      expirationTime: Long,
      rowCache: MapState[Long, JList[Row]],
      timerState: ValueState[Long],
      ctx: CoProcessFunction[CRow, CRow, CRow]#OnTimerContext,
      removeLeft: Boolean): Unit = {

    val keysIterator = rowCache.keys().iterator()

    // Search for expired timestamps.
    // If we find a non-expired timestamp, remember the timestamp and leave the loop.
    // This way we find all expired timestamps if they are sorted without doing a full pass.
    var earliestTimestamp: Long = -1L
    var rowTime: Long = 0L
    while (keysIterator.hasNext) {
      rowTime = keysIterator.next
      if (rowTime <= expirationTime) {
        keysIterator.remove()
      } else {
        // We find the earliest timestamp that is still valid.
        if (rowTime < earliestTimestamp || earliestTimestamp < 0) {
          earliestTimestamp = rowTime
        }
      }
    }
    // If the cache contains non-expired timestamps, register a new timer.
    // Otherwise clear the states.
    if (earliestTimestamp > 0) {
      registerCleanUpTimer(
        ctx,
        earliestTimestamp,
        timerState,
        removeLeft)
    } else {
      // The timerState will be 0.
      timerState.clear()
      rowCache.clear()
    }
  }

  /**
    * Check if the row is too late to be processed.
    *
    * @param timeForRow time of the row
    * @param operatorTime  operator time for current stream
    * @return true if the row is out of date; false otherwise
    */
  def isRowTooLate(timeForRow: Long, operatorTime: Long): Boolean

  /**
    * Update the operator time of the two streams.
    * Must be the first call in all processing methods (i.e., processElement(), onTimer()).
    *
    * @param ctx the context to acquire watermarks
    */
  def updateOperatorTime(ctx: CoProcessFunction[CRow, CRow, CRow]#Context): Unit

  /**
    * Return the time for the target row from the left stream.
    *
    * @param context the runtime context
    * @param row     the target row
    * @return time for the target row
    */
  def getTimeForLeftStream(context: CoProcessFunction[CRow, CRow, CRow]#Context, row: Row): Long

  /**
    * Return the time for the target row from the right stream.
    *
    * @param context the runtime context
    * @param row     the target row
    * @return time for the target row
    */
  def getTimeForRightStream(context: CoProcessFunction[CRow, CRow, CRow]#Context, row: Row): Long

  /**
    * Register a proctime or rowtime timer.
    *
    * @param ctx         the context to register the timer
    * @param cleanupTime timestamp for the timer
    */
  def registerTimer(
      ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
      cleanupTime: Long): Unit
}
