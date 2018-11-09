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
import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.api.java.typeutils.{ListTypeInfo, TupleTypeInfo}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.table.api.Types
import org.apache.flink.table.codegen.Compiler
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
  * @param joinType        the join type (inner or left/right/full outer)
  * @param leftLowerBound  the lower bound for the left stream (X in the criteria)
  * @param leftUpperBound  the upper bound for the left stream (Y in the criteria)
  * @param allowedLateness the lateness allowed for the two streams
  * @param leftType        the input type of left stream
  * @param rightType       the input type of right stream
  * @param genJoinFuncName the name of the generated function
  * @param genJoinFuncCode the code of function to evaluate the non-window join conditions
  *
  */
abstract class TimeBoundedStreamJoin(
    private val joinType: JoinType,
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

  private val paddingUtil: OuterJoinPaddingUtil =
    new OuterJoinPaddingUtil(leftType.getArity, rightType.getArity)

  private var joinCollector: EmitAwareCollector = _

  // the join function for other conditions
  private var joinFunction: FlatJoinFunction[Row, Row, Row] = _

  // cache to store rows from the left stream
  private var leftCache: MapState[Long, JList[JTuple2[Row, Boolean]]] = _
  // cache to store rows from the right stream
  private var rightCache: MapState[Long, JList[JTuple2[Row, Boolean]]] = _

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
    FunctionUtils.setFunctionRuntimeContext(joinFunction, getRuntimeContext)
    FunctionUtils.openFunction(joinFunction, config)

    joinCollector = new EmitAwareCollector()
    joinCollector.setCRowChange(true)

    // Initialize the data caches.
    val leftListTypeInfo: TypeInformation[JList[JTuple2[Row, Boolean]]] =
      new ListTypeInfo[JTuple2[Row, Boolean]] (
        new TupleTypeInfo(leftType, BasicTypeInfo.BOOLEAN_TYPE_INFO)
          .asInstanceOf[TypeInformation[JTuple2[Row, Boolean]]])
    val leftStateDescriptor: MapStateDescriptor[Long, JList[JTuple2[Row, Boolean]]] =
      new MapStateDescriptor[Long, JList[JTuple2[Row, Boolean]]](
        "WindowJoinLeftCache",
        Types.LONG.asInstanceOf[TypeInformation[Long]],
        leftListTypeInfo)
    leftCache = getRuntimeContext.getMapState(leftStateDescriptor)

    val rightListTypeInfo: TypeInformation[JList[JTuple2[Row, Boolean]]] =
      new ListTypeInfo[JTuple2[Row, Boolean]] (
        new TupleTypeInfo(rightType, BasicTypeInfo.BOOLEAN_TYPE_INFO)
          .asInstanceOf[TypeInformation[JTuple2[Row, Boolean]]])
    val rightStateDescriptor: MapStateDescriptor[Long, JList[JTuple2[Row, Boolean]]] =
      new MapStateDescriptor[Long, JList[JTuple2[Row, Boolean]]](
        "WindowJoinRightCache",
        Types.LONG.asInstanceOf[TypeInformation[Long]],
        rightListTypeInfo)
    rightCache = getRuntimeContext.getMapState(rightStateDescriptor)

    // Initialize the timer states.
    val leftTimerStateDesc: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("WindowJoinLeftTimerState", classOf[Long])
    leftTimerState = getRuntimeContext.getState(leftTimerStateDesc)

    val rightTimerStateDesc: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("WindowJoinRightTimerState", classOf[Long])
    rightTimerState = getRuntimeContext.getState(rightTimerStateDesc)
  }

  /**
    * Process rows from the left stream.
    */
  override def processElement1(
      cRowValue: CRow,
      ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
      out: Collector[CRow]): Unit = {

    joinCollector.innerCollector = out
    updateOperatorTime(ctx)
    val leftRow = cRowValue.row
    val timeForLeftRow: Long = getTimeForLeftStream(ctx, leftRow)
    val rightQualifiedLowerBound: Long = timeForLeftRow - rightRelativeSize
    val rightQualifiedUpperBound: Long = timeForLeftRow + leftRelativeSize
    var emitted: Boolean = false

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
          var entryUpdated = false
          while (i < rightRows.size) {
            joinCollector.reset()
            val tuple = rightRows.get(i)
            joinFunction.join(leftRow, tuple.f0, joinCollector)
            emitted ||= joinCollector.emitted
            if (joinType == JoinType.RIGHT_OUTER || joinType == JoinType.FULL_OUTER) {
              if (!tuple.f1 && joinCollector.emitted) {
                // Mark the right row as being successfully joined and emitted.
                tuple.f1 = true
                entryUpdated = true
              }
            }
            i += 1
          }
          if (entryUpdated) {
            // Write back the edited entry (mark emitted) for the right cache.
            rightEntry.setValue(rightRows)
          }
        }

        if (rightTime <= rightExpirationTime) {
          if (joinType == JoinType.RIGHT_OUTER || joinType == JoinType.FULL_OUTER) {
            val rightRows = rightEntry.getValue
            var i = 0
            while (i < rightRows.size) {
              val tuple = rightRows.get(i)
              if (!tuple.f1) {
                // Emit a null padding result if the right row has never been successfully joined.
                joinCollector.collect(paddingUtil.padRight(tuple.f0))
              }
              i += 1
            }
          }
          // eager remove
          rightIterator.remove()
        } // We could do the short-cutting optimization here once we get a state with ordered keys.
      }
    }

    // Check if we need to cache the current row.
    if (rightOperatorTime < rightQualifiedUpperBound) {
      // Operator time of right stream has not exceeded the upper window bound of the current
      // row. Put it into the left cache, since later coming records from the right stream are
      // expected to be joined with it.
      var leftRowList = leftCache.get(timeForLeftRow)
      if (null == leftRowList) {
        leftRowList = new util.ArrayList[JTuple2[Row, Boolean]](1)
      }
      leftRowList.add(JTuple2.of(leftRow, emitted))
      leftCache.put(timeForLeftRow, leftRowList)
      if (rightTimerState.value == 0) {
        // Register a timer on the RIGHT stream to remove rows.
        registerCleanUpTimer(ctx, timeForLeftRow, leftRow = true)
      }
    } else if (joinType == JoinType.LEFT_OUTER || joinType == JoinType.FULL_OUTER) {
      if (!emitted) {
        // Emit a null padding result if the left row is not cached and successfully joined.
        joinCollector.collect(paddingUtil.padLeft(leftRow))
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

    joinCollector.innerCollector = out
    updateOperatorTime(ctx)
    val rightRow = cRowValue.row
    val timeForRightRow: Long = getTimeForRightStream(ctx, rightRow)
    val leftQualifiedLowerBound: Long = timeForRightRow - leftRelativeSize
    val leftQualifiedUpperBound: Long =  timeForRightRow + rightRelativeSize
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
        val leftEntry = leftIterator.next
        val leftTime = leftEntry.getKey
        if (leftTime >= leftQualifiedLowerBound && leftTime <= leftQualifiedUpperBound) {
          val leftRows = leftEntry.getValue
          var i = 0
          var entryUpdated = false
          while (i < leftRows.size) {
            joinCollector.reset()
            val tuple = leftRows.get(i)
            joinFunction.join(tuple.f0, rightRow, joinCollector)
            emitted ||= joinCollector.emitted
            if (joinType == JoinType.LEFT_OUTER || joinType == JoinType.FULL_OUTER) {
              if (!tuple.f1 && joinCollector.emitted) {
                // Mark the left row as being successfully joined and emitted.
                tuple.f1 = true
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
          if (joinType == JoinType.LEFT_OUTER || joinType == JoinType.FULL_OUTER) {
            val leftRows = leftEntry.getValue
            var i = 0
            while (i < leftRows.size) {
              val tuple = leftRows.get(i)
              if (!tuple.f1) {
                // Emit a null padding result if the left row has never been successfully joined.
                joinCollector.collect(paddingUtil.padLeft(tuple.f0))
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
        rightRowList = new util.ArrayList[JTuple2[Row, Boolean]](1)
      }
      rightRowList.add(new JTuple2(rightRow, emitted))
      rightCache.put(timeForRightRow, rightRowList)
      if (leftTimerState.value == 0) {
        // Register a timer on the LEFT stream to remove rows.
        registerCleanUpTimer(ctx, timeForRightRow, leftRow = false)
      }
    } else if (joinType == JoinType.RIGHT_OUTER || joinType == JoinType.FULL_OUTER) {
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
  override def onTimer(
      timestamp: Long,
      ctx: CoProcessFunction[CRow, CRow, CRow]#OnTimerContext,
      out: Collector[CRow]): Unit = {

    joinCollector.innerCollector = out
    updateOperatorTime(ctx)
    // In the future, we should separate the left and right watermarks. Otherwise, the
    // registered timer of the faster stream will be delayed, even if the watermarks have
    // already been emitted by the source.
    if (leftTimerState.value == timestamp) {
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

    if (rightTimerState.value == timestamp) {
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

  override def close(): Unit = {
    FunctionUtils.closeFunction(joinFunction)
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
    * @param collector      the collector to emit results
    * @param expirationTime the expiration time for this cache
    * @param rowCache       the row cache
    * @param timerState     timer state for the opposite stream
    * @param ctx            the context to register the cleanup timer
    * @param removeLeft     whether to remove the left rows
    */
  private def removeExpiredRows(
      collector: Collector[Row],
      expirationTime: Long,
      rowCache: MapState[Long, JList[JTuple2[Row, Boolean]]],
      timerState: ValueState[Long],
      ctx: CoProcessFunction[CRow, CRow, CRow]#OnTimerContext,
      removeLeft: Boolean): Unit = {

    val iterator = rowCache.iterator()

    var earliestTimestamp: Long = -1L

    // We remove all expired keys and do not leave the loop early.
    // Hence, we do a full pass over the state.
    while (iterator.hasNext) {
      val entry = iterator.next
      val rowTime = entry.getKey
      if (rowTime <= expirationTime) {
        if (removeLeft &&
          (joinType == JoinType.LEFT_OUTER || joinType == JoinType.FULL_OUTER)) {
          val rows = entry.getValue
          var i = 0
          while (i < rows.size) {
            val tuple = rows.get(i)
            if (!tuple.f1) {
              // Emit a null padding result if the row has never been successfully joined.
              collector.collect(paddingUtil.padLeft(tuple.f0))
            }
            i += 1
          }
        } else if (!removeLeft &&
          (joinType == JoinType.RIGHT_OUTER || joinType == JoinType.FULL_OUTER)) {
          val rows = entry.getValue
          var i = 0
          while (i < rows.size) {
            val tuple = rows.get(i)
            if (!tuple.f1) {
              // Emit a null padding result if the row has never been successfully joined.
              collector.collect(paddingUtil.padRight(tuple.f0))
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
