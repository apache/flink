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
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Collector

/**
  * A CoProcessFunction to execute time-bounded stream inner-join.
  * Two kinds of time criteria:
  * "L.time between R.time + X and R.time + Y" or "R.time between L.time - Y and L.time - X"
  * X and Y might be negative or positive and X <= Y.
  *
  * @param joinType        the join type (inner or left/right/full/ outer)
  * @param leftLowerBound  the lower bound for the left stream (X in the criteria)
  * @param leftUpperBound  the upper bound for the left stream (Y in the criteria)
  * @param allowLateness   the lateness allowed for the two streams
  * @param leftType        the input type of left stream
  * @param rightType       the input type of right stream
  * @param genJoinFuncName the name of the generated function
  * @param genJoinFuncCode the code of function to evaluate the non-window join conditions
  *
  **/
abstract class TimeBoundedStreamJoin(
    private val joinType: FlinkJoinRelType,
    private val leftLowerBound: Long,
    private val leftUpperBound: Long,
    private val allowLateness: Long,
    private val leftType: TypeInformation[BaseRow],
    private val rightType: TypeInformation[BaseRow],
    private val genJoinFuncName: String,
    private var genJoinFuncCode: String
) extends CoProcessFunction[BaseRow, BaseRow, BaseRow]
  with Compiler[FlatJoinFunction[BaseRow, BaseRow, BaseRow]]
  with Logging {

  private val paddingUtil: OuterJoinPaddingUtil =
    new OuterJoinPaddingUtil(leftType.getArity, rightType.getArity)

  private var joinCollector: EmitAwareCollector = _

  //the join function for other conditions
  private var joinFunction: FlatJoinFunction[BaseRow, BaseRow, BaseRow] = _

  //cache to store rows form the left stream
  private var leftCache: MapState[Long, JList[BaseRow]] = _
  // cache to store rows from the right stream
  private var rightCache: MapState[Long, JList[BaseRow]] = _

  //state to record the timer on the left stream. 0 means no timer set
  private var leftTimerState: ValueState[Long] = _
  //state to record the timer on the right stream. 0 means no timer set
  private var rightTimerState: ValueState[Long] = _

  protected val leftRelativeSize: Long = -leftLowerBound
  protected val rightRelativeSize: Long = leftUpperBound


  //points in time util which the respective cache has been cleaned
  private var leftExpirationTime: Long = 0L
  private var rightExpirationTime: Long = 0L

  //current time on the respective input stream
  protected var leftOperatorTime = 0L
  protected var rightOperatorTime = 0L

  //minimum interval by which state is cleaned up
  private val miCleanUpInterval = (leftRelativeSize + rightRelativeSize) / 2

  if (allowLateness < 0) {
    throw new IllegalArgumentException("The allowed lateness must be non-negative.")
  }

  override def open(parameters: Configuration): Unit = {
    LOG.debug(s"Compiling JoinFunction: $genJoinFuncName \n\n " +
      s"Code:\n $genJoinFuncCode")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genJoinFuncName,
      genJoinFuncCode
    )
    genJoinFuncCode = null
    LOG.debug("Instantiating JoinFunction.")
    joinFunction = clazz.newInstance()

    joinCollector = new EmitAwareCollector

    // Initialize the data caches.
    val leftRowListTypeInfo: ListTypeInfo[BaseRow] = new ListTypeInfo[BaseRow](leftType)
    val leftMapStateDescriptor: MapStateDescriptor[Long, JList[BaseRow]] =
      new MapStateDescriptor[Long, JList[BaseRow]]("leftRowMapState",
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]], leftRowListTypeInfo)
    leftCache = getRuntimeContext.getMapState(leftMapStateDescriptor)

    val rightRowListTypeInfo: ListTypeInfo[BaseRow] = new ListTypeInfo[BaseRow](rightType)
    val rightMapStateDescriptor: MapStateDescriptor[Long, JList[BaseRow]] =
      new MapStateDescriptor[Long, JList[BaseRow]]("rightMapState",
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]], rightRowListTypeInfo)
    rightCache = getRuntimeContext.getMapState(rightMapStateDescriptor)

    // Initialize the timer states.
    val leftValueStateDescriptor: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("leftTimerValueState", classOf[Long])
    leftTimerState = getRuntimeContext.getState(leftValueStateDescriptor)

    val rightValueStateDescriptor: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("rightTimerState", classOf[Long])
    rightTimerState = getRuntimeContext.getState(rightValueStateDescriptor)
  }

  /**
    * Process rows from the left stream.
    */
  override def processElement1(value: BaseRow,
    ctx: CoProcessFunction[BaseRow, BaseRow, BaseRow]#Context,
    out: Collector[BaseRow]): Unit = {

    joinCollector.innerCollector = out
    updateOperatorTime(ctx)
    val leftRow = value
    val timeForLeftRow = getTimeForLeftStream(ctx, leftRow)
    val rightQualifiedLowerBound = timeForLeftRow - rightRelativeSize
    val rightQualifiedUpperBound = timeForLeftRow + leftRelativeSize
    var emitted = false

    //check if we need to join the current row against cached rows of the right input.
    //the condition here should be rightMinimumTIme < rightQualifiedUpperBound.
    //we use rightExpirationTime as an approximation of the rightMinimumTime here.
    //since rightExpirationTime <= rightMinimumTime is always true.
    if (rightExpirationTime < rightQualifiedUpperBound) {
      //upper bound of current join windows has not passed the cache expiration time yet.
      //there might be qualifying rows in the cache that the current row needs to be joined with.
      rightExpirationTime = calExpirationTime(leftOperatorTime, rightRelativeSize)
      //join the leftRow with rows from the right cache.
      val rightIterator = rightCache.iterator()
      while (rightIterator.hasNext) {
        val rightEntry = rightIterator.next()
        val rightTime = rightEntry.getKey
        if (rightTime >= rightQualifiedLowerBound && rightTime <= rightQualifiedUpperBound) {
          val rightRows = rightEntry.getValue
          var i = 0
          var entryUpdated = false
          while (i < rightRows.size()) {
            joinCollector.reset()
            val row = rightRows.get(i)
            joinFunction.join(leftRow, row, joinCollector)
            emitted ||= joinCollector.emitted
            if (joinType == FlinkJoinRelType.RIGHT || joinType == FlinkJoinRelType.FULL) {
              if (!EmitAwareCollector.isJoined(row) && joinCollector.emitted) {
                //Mark the right row as being successfully joined and emitted.
                row.setHeader(EmitAwareCollector.JOINED)
                entryUpdated = true
              }
            }
            i += 1
          }

          if (entryUpdated) {
            //write back to the edited entry (since its header has been changed)
            // for the right cache.
            rightEntry.setValue(rightRows)
          }
        }
        //clean up the expired right cache row,clean the cache while join
        if (rightTime <= rightExpirationTime) {
          if (joinType == FlinkJoinRelType.RIGHT || joinType == FlinkJoinRelType.FULL) {
            val rightRows = rightEntry.getValue
            var i = 0
            while (i < rightRows.size()) {
              val row = rightRows.get(i)
              if (!EmitAwareCollector.isJoined(row)) {
                //emit a null padding result if the right row has never been successfully joined.
                joinCollector.collect(paddingUtil.padRight(row))
              }
              i += 1
            }
          }
          // eager remove
          rightIterator.remove()
        } // We could do the short-cutting optimization here once we get a state with ordered keys.
      }
    }
    //cache the leftRow if the watermark of right stream is smaller than right upper bound
    if (rightOperatorTime < rightQualifiedUpperBound) {
      // Operator time of right stream has not exceeded the upper window bound of the current
      // row. Put it into the left cache, since later coming records from the right stream are
      // expected to be joined with it.
      var leftRowList = leftCache.get(timeForLeftRow)
      if (leftRowList == null) {
        leftRowList = new util.ArrayList[BaseRow](1)
      }
      leftRow.setHeader(if (emitted) EmitAwareCollector.JOINED else EmitAwareCollector.NONJOINED)
      leftRowList.add(leftRow)
      leftCache.put(timeForLeftRow, leftRowList)
      if (rightTimerState.value() == 0) {
        // Register a timer on the RIGHT stream to remove rows.
        registerCleanUpTimer(ctx, timeForLeftRow, leftRow = true)
      }
    } else if (joinType == FlinkJoinRelType.LEFT || joinType == FlinkJoinRelType.FULL) {
      if (!emitted) {
        // Emit a null padding result if the left row is not cached and successfully joined.
        joinCollector.collect(paddingUtil.padLeft(leftRow))
      }
    }
  }

  /**
    * Process rows from the right stream.
    */
  override def processElement2(value: BaseRow,
    ctx: CoProcessFunction[BaseRow, BaseRow, BaseRow]#Context,
    out: Collector[BaseRow]): Unit = {

    joinCollector.innerCollector = out
    updateOperatorTime(ctx)
    val rightRow = value
    val timeForRightRow: Long = getTimeForRightStream(ctx, rightRow)
    val leftQualifiedLowerBound: Long = timeForRightRow - leftRelativeSize
    val leftQualifiedUpperBound: Long = timeForRightRow + rightRelativeSize
    var emitted: Boolean = false

    // Check if we need to join the current row against cached rows of the left input.
    // The condition here should be leftMinimumTime < leftQualifiedUpperBound.
    // We use leftExpirationTime as an approximation of the leftMinimumTime here,
    // since leftExpirationTime <= leftMinimumTime is always true.
    if (leftExpirationTime < leftQualifiedUpperBound) {
      leftExpirationTime = calExpirationTime(rightOperatorTime, leftRelativeSize)
      // Join the rightRow with rows from the left cache.
      val leftIterator = leftCache.iterator()
      while (leftIterator.hasNext) {
        val leftEntry = leftIterator.next()
        val leftTime = leftEntry.getKey
        if (leftTime >= leftQualifiedLowerBound && leftTime <= leftQualifiedUpperBound) {
          val leftRows = leftEntry.getValue
          var i = 0
          var entryUpdated = false
          while (i < leftRows.size()) {
            joinCollector.reset()
            val row = leftRows.get(i)
            joinFunction.join(row, rightRow, joinCollector)
            emitted ||= joinCollector.emitted
            if (joinType == FlinkJoinRelType.LEFT || joinType == FlinkJoinRelType.FULL) {
              if (!EmitAwareCollector.isJoined(row) && joinCollector.emitted) {
                // Mark the left row as being successfully joined and emitted.
                row.setHeader(EmitAwareCollector.JOINED)
                entryUpdated = true
              }
            }
            i += 1
          }
          if (entryUpdated) {
            // Write back the edited entry (mark emitted) for the right cache.
            leftEntry.setValue(leftRows)
          }
        }

        if (leftTime <= leftExpirationTime) {
          if (joinType == FlinkJoinRelType.LEFT || joinType == FlinkJoinRelType.FULL) {
            val leftRows = leftEntry.getValue
            var i = 0
            while (i < leftRows.size()) {
              val row = leftRows.get(i)
              if (!EmitAwareCollector.isJoined(row)) {
                // Emit a null padding result if the left row has never been successfully joined.
                joinCollector.collect(paddingUtil.padLeft(row))
              }
              i += 1
            }
          }
          // eager remove
          leftIterator.remove()
        } // We could do the short-cutting optimization here once we get a state with ordered keys.
      }
    }
    // Check if we need to cache the current row.
    if (leftOperatorTime < leftQualifiedUpperBound) {
      // Operator time of left stream has not exceeded the upper window bound of the current
      // row. Put it into the right cache, since later coming records from the left stream are
      // expected to be joined with it.
      var rightRowList = rightCache.get(timeForRightRow)
      if (null == rightRowList) {
        rightRowList = new util.ArrayList(1)
      }
      value.setHeader(if (emitted) EmitAwareCollector.JOINED else EmitAwareCollector.NONJOINED)
      rightRowList.add(value)
      rightCache.put(timeForRightRow, rightRowList)
      if (leftTimerState.value() == 0) {
        // Register a timer on the LEFT stream to remove rows.
        registerCleanUpTimer(ctx, timeForRightRow, leftRow = false)
      }
    } else if (joinType == FlinkJoinRelType.RIGHT || joinType == FlinkJoinRelType.FULL) {
      if (!emitted) {
        // Emit a null padding result if the right row is not cached and successfully joined.
        joinCollector.collect(paddingUtil.padRight(rightRow))
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
  override def onTimer(timestamp: Long,
    ctx: CoProcessFunction[BaseRow, BaseRow, BaseRow]#OnTimerContext,
    out: Collector[BaseRow]): Unit = {

    joinCollector.innerCollector = out
    updateOperatorTime(ctx)
    // In the future, we should separate the left and right watermarks. Otherwise, the
    // registered timer of the faster stream will be delayed, even if the watermarks have
    // already been emitted by the source.
    if (leftTimerState.value() == timestamp) {
      rightExpirationTime = calExpirationTime(leftOperatorTime, rightRelativeSize)
      removeExpiredRows(
        joinCollector,
        rightExpirationTime,
        rightCache,
        leftTimerState,
        ctx,
        removeLeft = false
      )
    }

    if (rightTimerState.value() == timestamp) {
      leftExpirationTime = calExpirationTime(rightOperatorTime, leftRelativeSize)
      removeExpiredRows(
        joinCollector,
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
      operatorTime - relativeSize - allowLateness - 1
    } else {
      // When operatorTime = Long.MaxValue, it means the stream has reached the end.
      Long.MaxValue
    }
  }

  /**
    * Register a timer for cleaning up rows in a specified time.
    *
    * @param ctx     the context to register timer
    * @param rowTime time for the input row
    * @param leftRow whether this row comes from the left stream
    */
  private def registerCleanUpTimer(
    ctx: CoProcessFunction[BaseRow, BaseRow, BaseRow]#Context,
    rowTime: Long,
    leftRow: Boolean): Unit = {
    if (leftRow) {
      val cleanUpTime = rowTime + leftRelativeSize + miCleanUpInterval + allowLateness + 1
      registerTimer(ctx, cleanUpTime)
      rightTimerState.update(cleanUpTime)
    } else {
      val cleanUpTime = rowTime + rightRelativeSize + miCleanUpInterval + allowLateness + 1
      registerTimer(ctx, cleanUpTime)
      leftTimerState.update(cleanUpTime)
    }
  }

  /**
    * Remove the expired rows. Register a new timer if the cache still holds valid rows
    * after the cleaning up.
    *
    * @param collector      the collector to emit results
    * @param expirationTime the expiration time for this cache
    * @param rowCache       the row cache
    * @param timerState     timer state for the opposite stream
    * @param ctx            the context to register the cleanup timer
    * @param removeLeft     whether to remove the left rows
    */
  private def removeExpiredRows(
    collector: Collector[BaseRow],
    expirationTime: Long,
    rowCache: MapState[Long, JList[BaseRow]],
    timerState: ValueState[Long],
    ctx: CoProcessFunction[BaseRow, BaseRow, BaseRow]#OnTimerContext,
    removeLeft: Boolean): Unit = {

    val iterator = rowCache.iterator()

    var earliestTimestamp: Long = -1L

    // We remove all expired keys and do not leave the loop early.
    // Hence, we do a full pass over the state.
    while (iterator.hasNext) {
      val entry = iterator.next()
      val rowTime = entry.getKey
      if (rowTime <= expirationTime) {
        if (removeLeft && (joinType == FlinkJoinRelType.LEFT ||
          joinType == FlinkJoinRelType.FULL)) {
          val rows = entry.getValue
          var i = 0
          while (i < rows.size()) {
            val row = rows.get(i)
            if (!EmitAwareCollector.isJoined(row)) {
              // Emit a null padding result if the row has never been successfully joined.
              collector.collect(paddingUtil.padLeft(row))
            }
            i += 1
          }
        } else if (!removeLeft && (joinType == FlinkJoinRelType.RIGHT ||
          joinType == FlinkJoinRelType.FULL)) {
          val rows = entry.getValue
          var i = 0
          while (i < rows.size()) {
            val row = rows.get(i)
            if (!EmitAwareCollector.isJoined(row)) {
              // Emit a null padding result if the row has never been successfully joined.
              collector.collect(paddingUtil.padRight(row))
            }
            i += 1
          }
        }
        iterator.remove()
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
        removeLeft
      )
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
  def updateOperatorTime(ctx: CoProcessFunction[BaseRow, BaseRow, BaseRow]#Context): Unit

  /**
    * Return the time for the target row from the left stream.
    *
    * Requires that [[updateOperatorTime()]] has been called before.
    *
    * @param ctx the runtime context
    * @param row the target row
    * @return time for the target row
    */
  def getTimeForLeftStream(
    ctx: CoProcessFunction[BaseRow, BaseRow, BaseRow]#Context,
    row: BaseRow): Long

  /**
    * Return the time for the target row from the right stream.
    *
    * Requires that [[updateOperatorTime()]] has been called before.
    *
    * @param ctx the runtime context
    * @param row the target row
    * @return time for the target row
    */
  def getTimeForRightStream(
    ctx: CoProcessFunction[BaseRow, BaseRow, BaseRow]#Context,
    row: BaseRow): Long

  /**
    * Register a proctime or rowtime timer.
    *
    * @param ctx         the context to register the timer
    * @param cleanupTime timestamp for the timer
    */
  def registerTimer(
    ctx: CoProcessFunction[BaseRow, BaseRow, BaseRow]#Context,
    cleanupTime: Long)
}
