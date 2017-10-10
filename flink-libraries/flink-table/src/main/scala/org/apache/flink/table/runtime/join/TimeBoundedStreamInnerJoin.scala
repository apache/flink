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

import java.util
import java.util.{List => JList}

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
  * "L.time between R.time + X and R.time + Y" or "R.time between L.time - Y and L.time - X" where
  * X and Y might be negative or positive and X <= Y.
  *
  * @param leftLowerBound  the lower bound for the left stream (X in the criteria)
  * @param leftUpperBound  the upper bound for the left stream (Y in the criteria)
  * @param allowedLateness the lateness allowed for the two streams
  * @param leftType        the input type of left stream
  * @param rightType       the input type of right stream
  * @param genJoinFuncName the name of the generated function
  * @param genJoinFuncCode the code of function to evaluate the non-window join conditions
  *
  */
abstract class TimeBoundedStreamInnerJoin(
    private val leftLowerBound: Long,
    private val leftUpperBound: Long,
    private val allowedLateness: Long,
    private val leftType: TypeInformation[Row],
    private val rightType: TypeInformation[Row],
    private val genJoinFuncName: String,
    private val genJoinFuncCode: String)
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

  protected val leftRelativeSize: Long = -leftLowerBound
  protected val rightRelativeSize: Long = leftUpperBound

  // Points in time until which the respective cache has been cleaned.
  private var leftExpirationTime: Long = 0L
  private var rightExpirationTime: Long = 0L

  // Current time on the respective input stream.
  protected var leftOperatorTime: Long = 0L
  protected var rightOperatorTime: Long = 0L

  // Minimum interval by which state is cleaned up
  private val minCleanUpInterval = (leftRelativeSize + rightRelativeSize) / 2

  if (allowedLateness < 0) {
    throw new IllegalArgumentException("The allowed lateness must be non-negative.")
  }

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
    cRowWrapper.out = out

    // Check if we need to cache the current row.
    if (rightOperatorTime < rightQualifiedUpperBound) {
      // Operator time of right stream has not exceeded the upper window bound of the current
      // row. Put it into the left cache, since later coming records from the right stream are
      // expected to be joined with it.
      var leftRowList = leftCache.get(timeForLeftRow)
      if (null == leftRowList) {
        leftRowList = new util.ArrayList[Row](1)
      }
      leftRowList.add(leftRow)
      leftCache.put(timeForLeftRow, leftRowList)
      if (rightTimerState.value == 0) {
        // Register a timer on the RIGHT stream to remove rows.
        registerCleanUpTimer(ctx, timeForLeftRow, leftRow = true)
      }
    }
    // Check if we need to join the current row against cached rows of the right input.
    // The condition here should be rightMinimumTime < rightQualifiedUpperBound.
    // We use rightExpirationTime as an approximation of the rightMinimumTime here,
    // since rightExpirationTime <= rightMinimumTime is always true.
    if (rightExpirationTime < rightQualifiedUpperBound) {
      // Upper bound of current join window has not passed the cache expiration time yet.
      // There might be qualifying rows in the cache that the current row needs to be joined with.
      rightExpirationTime = calExpirationTime(leftOperatorTime, rightRelativeSize)
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
        }

        if (rightTime <= rightExpirationTime) {
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
    cRowWrapper.out = out

    // Check if we need to cache the current row.
    if (leftOperatorTime < leftQualifiedUpperBound) {
      // Operator time of left stream has not exceeded the upper window bound of the current
      // row. Put it into the right cache, since later coming records from the left stream are
      // expected to be joined with it.
      var rightRowList = rightCache.get(timeForRightRow)
      if (null == rightRowList) {
        rightRowList = new util.ArrayList[Row](1)
      }
      rightRowList.add(rightRow)
      rightCache.put(timeForRightRow, rightRowList)
      if (leftTimerState.value == 0) {
        // Register a timer on the LEFT stream to remove rows.
        registerCleanUpTimer(ctx, timeForRightRow, leftRow = false)
      }
    }
    // Check if we need to join the current row against cached rows of the left input.
    // The condition here should be leftMinimumTime < leftQualifiedUpperBound.
    // We use leftExpirationTime as an approximation of the leftMinimumTime here,
    // since leftExpirationTime <= leftMinimumTime is always true.
    if (leftExpirationTime < leftQualifiedUpperBound) {
      leftExpirationTime = calExpirationTime(rightOperatorTime, leftRelativeSize)
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
        }
        if (leftTime <= leftExpirationTime) {
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
      rightExpirationTime = calExpirationTime(leftOperatorTime, rightRelativeSize)
      removeExpiredRows(
        rightExpirationTime,
        rightCache,
        leftTimerState,
        ctx,
        removeLeft = false
      )
    }

    if (rightTimerState.value == timestamp) {
      leftExpirationTime = calExpirationTime(rightOperatorTime, leftRelativeSize)
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
      // When operatorTime = Long.MaxValue, it means the stream has reached the end.
      Long.MaxValue
    }
  }

  /**
    * Register a timer for cleaning up rows in a specified time.
    *
    * @param ctx        the context to register timer
    * @param rowTime    time for the input row
    * @param leftRow    whether this row comes from the left stream
    */
  private def registerCleanUpTimer(
      ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
      rowTime: Long,
      leftRow: Boolean): Unit = {
    if (leftRow) {
      val cleanupTime = rowTime + leftRelativeSize + minCleanUpInterval + allowedLateness + 1
      registerTimer(ctx, cleanupTime)
      rightTimerState.update(cleanupTime)
    } else {
      val cleanupTime = rowTime + rightRelativeSize + minCleanUpInterval + allowedLateness + 1
      registerTimer(ctx, cleanupTime)
      leftTimerState.update(cleanupTime)
    }
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

    var earliestTimestamp: Long = -1L
    var rowTime: Long = 0L

    // We remove all expired keys and do not leave the loop early.
    // Hence, we do a full pass over the state.
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

    if (earliestTimestamp > 0) {
      // There are rows left in the cache. Register a timer to expire them later.
      registerCleanUpTimer(
        ctx,
        earliestTimestamp,
        removeLeft)
    } else {
      // No rows left in the cache. Clear the states and the timerState will be 0.
      timerState.clear()
      rowCache.clear()
    }
  }

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
    * Requires that [[updateOperatorTime()]] has been called before.
    *
    * @param context the runtime context
    * @param row     the target row
    * @return time for the target row
    */
  def getTimeForLeftStream(context: CoProcessFunction[CRow, CRow, CRow]#Context, row: Row): Long

  /**
    * Return the time for the target row from the right stream.
    *
    * Requires that [[updateOperatorTime()]] has been called before.
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
