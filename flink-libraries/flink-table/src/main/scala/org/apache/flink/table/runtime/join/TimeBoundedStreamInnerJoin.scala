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

import java.text.SimpleDateFormat
import java.util
import java.util.Map.Entry
import java.util.{Date, List => JList}

import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.runtime.CRowWrappingCollector
import org.apache.flink.table.runtime.join.JoinTimeIndicator.JoinTimeIndicator
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * A CoProcessFunction to execute time-bounded stream inner-join.
  *
  * Sample criteria:
  *
  * L.time between R.time + X and R.time + Y
  * or AND R.time between L.time - Y and L.time - X
  *
  * @param leftLowerBound  X
  * @param leftUpperBound  Y
  * @param allowedLateness the lateness allowed for the two streams
  * @param leftType        the input type of left stream
  * @param rightType       the input type of right stream
  * @param genJoinFuncName the function code of other non-equi conditions
  * @param genJoinFuncCode the function name of other non-equi conditions
  * @param timeIndicator   indicate whether joining on proctime or rowtime
  *
  */
class TimeBoundedStreamInnerJoin(
  private val leftLowerBound: Long,
  private val leftUpperBound: Long,
  private val allowedLateness: Long,
  private val leftType: TypeInformation[Row],
  private val rightType: TypeInformation[Row],
  private val genJoinFuncName: String,
  private val genJoinFuncCode: String,
  private val leftTimeIdx: Int,
  private val rightTimeIdx: Int,
  private val timeIndicator: JoinTimeIndicator)
  extends CoProcessFunction[CRow, CRow, CRow]
    with Compiler[FlatJoinFunction[Row, Row, Row]]
    with Logging {

  private var cRowWrapper: CRowWrappingCollector = _

  // the join function for other conditions
  private var joinFunction: FlatJoinFunction[Row, Row, Row] = _

  // cache to store the left stream records
  private var leftCache: MapState[Long, JList[Row]] = _
  // cache to store right stream records
  private var rightCache: MapState[Long, JList[Row]] = _

  // state to record the timer on the left stream. 0 means no timer set
  private var leftTimerState: ValueState[Long] = _
  // state to record the timer on the right stream. 0 means no timer set
  private var rightTimerState: ValueState[Long] = _

  private val leftRelativeSize: Long = -leftLowerBound
  private val rightRelativeSize: Long = leftUpperBound

  private val relativeWindowSize = rightRelativeSize + leftRelativeSize

  private var leftOperatorTime: Long = 0L
  private var rightOperatorTime: Long = 0L

  private var backPressureSuggestion: Long = 0L

  if (relativeWindowSize <= 0) {
    LOG.warn("The relative window size is non-positive, please check the join conditions.")
  }

  if (allowedLateness < 0) {
    throw new IllegalArgumentException("The allowed lateness must be non-negative.")
  }


  /**
    * For holding back watermarks.
    *
    * @return the maximum delay for the outputs
    */
  def getMaxOutputDelay = Math.max(leftRelativeSize, rightRelativeSize) + allowedLateness;

  /**
    * For dynamic query optimization.
    *
    * @return the suggested offset time for back-pressure
    */
  def getBackPressureSuggestion = backPressureSuggestion

  override def open(config: Configuration) {
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genJoinFuncName,
      genJoinFuncCode)
    joinFunction = clazz.newInstance()

    cRowWrapper = new CRowWrappingCollector()
    cRowWrapper.setChange(true)

    // Initialize the data caches.
    val leftListTypeInfo: TypeInformation[JList[Row]] = new ListTypeInfo[Row](leftType)
    val leftStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]](timeIndicator + "InnerJoinLeftCache",
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]], leftListTypeInfo)
    leftCache = getRuntimeContext.getMapState(leftStateDescriptor)

    val rightListTypeInfo: TypeInformation[JList[Row]] = new ListTypeInfo[Row](rightType)
    val rightStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]](timeIndicator + "InnerJoinRightCache",
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]], rightListTypeInfo)
    rightCache = getRuntimeContext.getMapState(rightStateDescriptor)

    // Initialize the timer states.
    val leftTimerStateDesc: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long](timeIndicator + "InnerJoinLeftTimerState",
        classOf[Long])
    leftTimerState = getRuntimeContext.getState(leftTimerStateDesc)

    val rightTimerStateDesc: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long](timeIndicator + "InnerJoinRightTimerState",
        classOf[Long])
    rightTimerState = getRuntimeContext.getState(rightTimerStateDesc)
  }

  /**
    * Process records from the left stream.
    *
    * @param cRowValue the input record
    * @param ctx       the context to register timer or get current time
    * @param out       the collector for outputting results
    *
    */
  override def processElement1(
    cRowValue: CRow,
    ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
    out: Collector[CRow]): Unit = {
    val timeForRecord: Long = getTimeForRecord(ctx, cRowValue, true)
    getCurrentOperatorTime(ctx)
    processElement(
      cRowValue,
      timeForRecord,
      ctx,
      out,
      leftOperatorTime,
      rightOperatorTime,
      rightTimerState,
      leftCache,
      rightCache,
      true
    )
  }

  /**
    * Process records from the right stream.
    *
    * @param cRowValue the input record
    * @param ctx       the context to get current time
    * @param out       the collector for outputting results
    *
    */
  override def processElement2(
    cRowValue: CRow,
    ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
    out: Collector[CRow]): Unit = {
    val timeForRecord: Long = getTimeForRecord(ctx, cRowValue, false)
    getCurrentOperatorTime(ctx)
    processElement(
      cRowValue,
      timeForRecord,
      ctx,
      out,
      rightOperatorTime,
      leftOperatorTime,
      leftTimerState,
      rightCache,
      leftCache,
      false
    )
  }

  /**
    * Put a record from the input stream into the cache and iterate the opposite cache to
    * output records meeting the join conditions. If there is no timer set for the OPPOSITE
    * STREAM, register one.
    */
  private def processElement(
    cRowValue: CRow,
    timeForRecord: Long,
    ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
    out: Collector[CRow],
    myWatermark: Long,
    oppositeWatermark: Long,
    oppositeTimeState: ValueState[Long],
    recordListCache: MapState[Long, JList[Row]],
    oppositeCache: MapState[Long, JList[Row]],
    leftRecord: Boolean): Unit = {
    if (relativeWindowSize > 0) {
      //TODO Shall we consider adding a method for initialization with the context and collector?
      cRowWrapper.out = out

      val record = cRowValue.row

      //TODO Only if the time of the record is greater than the watermark, can we continue.
      if (timeForRecord >= myWatermark - allowedLateness) {
        val oppositeLowerBound: Long =
          if (leftRecord) timeForRecord - rightRelativeSize else timeForRecord - leftRelativeSize

        val oppositeUpperBound: Long =
          if (leftRecord) timeForRecord + leftRelativeSize else timeForRecord + rightRelativeSize

        // Put the record into the cache for later use.
        val recordList = if (recordListCache.contains(timeForRecord)) {
          recordListCache.get(timeForRecord)
        } else {
          new util.ArrayList[Row]()
        }
        recordList.add(record)
        recordListCache.put(timeForRecord, recordList)

        // Register a timer on THE OTHER STREAM to remove records from the cache once they are
        // expired.
        if (oppositeTimeState.value == 0) {
          registerCleanUpTimer(
            ctx, timeForRecord, oppositeWatermark, oppositeTimeState, leftRecord, true)
        }

        // Join the record with records from the opposite stream.
        val oppositeIterator = oppositeCache.iterator()
        var oppositeEntry: Entry[Long, util.List[Row]] = null
        var oppositeTime: Long = 0L;
        while (oppositeIterator.hasNext) {
          oppositeEntry = oppositeIterator.next
          oppositeTime = oppositeEntry.getKey
          if (oppositeTime < oppositeLowerBound - allowedLateness) {
            //TODO Considering the data out-of-order, we should not remove records here.
          } else if (oppositeTime >= oppositeLowerBound && oppositeTime <= oppositeUpperBound) {
            val oppositeRows = oppositeEntry.getValue
            var i = 0
            if (leftRecord) {
              while (i < oppositeRows.size) {
                joinFunction.join(record, oppositeRows.get(i), cRowWrapper)
                i += 1
              }
            } else {
              while (i < oppositeRows.size) {
                joinFunction.join(oppositeRows.get(i), record, cRowWrapper)
                i += 1
              }
            }
          } else if (oppositeTime > oppositeUpperBound) {
            //TODO If the keys are ordered, can we break here?
          }
        }
      } else {
        //TODO Need some extra logic here?
        LOG.warn(s"$record is out-of-date.")
      }
    }
  }

  /**
    * Register a timer for cleaning up records in a specified time.
    *
    * @param ctx               the context to register timer
    * @param timeForRecord     time for the input record
    * @param oppositeWatermark watermark of the opposite stream
    * @param timerState        stores the timestamp for the next timer
    * @param leftRecord        record from the left or the right stream
    * @param firstTimer        whether this is the first timer
    */
  private def registerCleanUpTimer(
    ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
    timeForRecord: Long,
    oppositeWatermark: Long,
    timerState: ValueState[Long],
    leftRecord: Boolean,
    firstTimer: Boolean): Unit = {
    val cleanUpTime = timeForRecord + (if (leftRecord) leftRelativeSize else rightRelativeSize) +
      allowedLateness + 1
    registerTimer(ctx, !leftRecord, cleanUpTime)
    LOG.debug(s"Register a clean up timer on the ${if (leftRecord) "RIGHT" else "LEFT"} state:"
      + s" timeForRecord = ${timeForRecord}, cleanUpTime = ${cleanUpTime}, oppositeWatermark = " +
      s"${oppositeWatermark}")
    timerState.update(cleanUpTime)
    if (cleanUpTime <= oppositeWatermark + allowedLateness && firstTimer) {
      backPressureSuggestion =
        if (leftRecord) (oppositeWatermark + allowedLateness - cleanUpTime)
        else -(oppositeWatermark + allowedLateness - cleanUpTime)
      LOG.warn("The clean timer for the " +
        s"${if (leftRecord) "left" else "right"}" +
        s" stream is lower than ${if (leftRecord) "right" else "left"} watermark." +
        s" requiredTime = ${formatTime(cleanUpTime)}, watermark = ${formatTime(oppositeWatermark)},"
        + s"backPressureSuggestion = " + s"${backPressureSuggestion}.")
    }
  }


  /**
    * Called when a registered timer is fired.
    * Remove records which are earlier than the expiration time,
    * and register a new timer for the earliest remaining records.
    *
    * @param timestamp the timestamp of the timer
    * @param ctx       the context to register timer or get current time
    * @param out       the collector for returning result values
    */
  override def onTimer(
    timestamp: Long,
    ctx: CoProcessFunction[CRow, CRow, CRow]#OnTimerContext,
    out: Collector[CRow]): Unit = {
    getCurrentOperatorTime(ctx)
    //TODO In the future, we should separate the left and right watermarks. Otherwise, the
    //TODO registered timer of the faster stream will be delayed, even if the watermarks have
    //TODO already been emitted by the source.
    if (leftTimerState.value == timestamp) {
      val rightExpirationTime = leftOperatorTime - rightRelativeSize - allowedLateness - 1
      removeExpiredRecords(
        timestamp,
        rightExpirationTime,
        leftOperatorTime,
        rightCache,
        leftTimerState,
        ctx,
        false
      )
    }

    if (rightTimerState.value == timestamp) {
      val leftExpirationTime = rightOperatorTime - leftRelativeSize - allowedLateness - 1
      removeExpiredRecords(
        timestamp,
        leftExpirationTime,
        rightOperatorTime,
        leftCache,
        rightTimerState,
        ctx,
        true
      )
    }
  }

  /**
    * Remove the expired records. Register a new timer if the cache still holds records
    * after the cleaning up.
    */
  private def removeExpiredRecords(
    timerFiringTime: Long,
    expirationTime: Long,
    oppositeWatermark: Long,
    recordCache: MapState[Long, JList[Row]],
    timerState: ValueState[Long],
    ctx: CoProcessFunction[CRow, CRow, CRow]#OnTimerContext,
    removeLeft: Boolean): Unit = {

    val keysIterator = recordCache.keys().iterator()

    // Search for expired timestamps.
    // If we find a non-expired timestamp, remember the timestamp and leave the loop.
    // This way we find all expired timestamps if they are sorted without doing a full pass.
    var earliestTimestamp: Long = -1L
    var recordTime: Long = 0L
    while (keysIterator.hasNext) {
      //TODO The "short-circuit" code was commented, because when using a StateMap with
      //TODO unordered keys, the cache will grow indefinitely!
      // && earliestTimestamp < 0) {
      recordTime = keysIterator.next
      if (recordTime <= expirationTime) {
        // TODO Not sure if we can remove records directly.
        keysIterator.remove()
      } else {
        // We find the earliest timestamp that is still valid.
        if (recordTime < earliestTimestamp || earliestTimestamp < 0) {
          earliestTimestamp = recordTime
        }
      }
    }
    // If the cache contains non-expired timestamps, register a new timer.
    // Otherwise clear the states.
    if (earliestTimestamp > 0) {
      registerCleanUpTimer(ctx, earliestTimestamp, oppositeWatermark, timerState, removeLeft, false)
    } else {
      // The timerState will be 0.
      timerState.clear()
      recordCache.clear()
    }
  }

  /**
    * Get the operator times of the two streams.
    *
    * @param ctx the context to acquire watermarks
    */
  protected def getCurrentOperatorTime(
    ctx: CoProcessFunction[CRow, CRow, CRow]#Context): Unit = {
    timeIndicator match {
      case JoinTimeIndicator.ROWTIME => {
        rightOperatorTime =
          if (ctx.timerService().currentWatermark() > 0) ctx.timerService().currentWatermark()
          else 0L;
        leftOperatorTime =
          if (ctx.timerService().currentWatermark() > 0) ctx.timerService().currentWatermark()
          else 0L;
      }
      case JoinTimeIndicator.PROCTIME => {
        rightOperatorTime = ctx.timerService().currentProcessingTime()
        leftOperatorTime = ctx.timerService().currentProcessingTime()
      }
    }
  }


  /**
    * Return the rowtime or proctime for the target record.
    *
    * @param context the runtime context
    * @param record  the target record
    * @param isLeft  whether the record is from the left stream
    * @return time for the target record
    */
  protected def getTimeForRecord(
    context: CoProcessFunction[CRow, CRow, CRow]#Context,
    record: CRow,
    isLeft: Boolean): Long = {
    timeIndicator match {
      case JoinTimeIndicator.ROWTIME => {
        return if (isLeft) {
          record.row.getField(leftTimeIdx).asInstanceOf[Long]
        } else {
          record.row.getField(rightTimeIdx).asInstanceOf[Long];
        }
      }
      case JoinTimeIndicator.PROCTIME => {
        return context.timerService().currentProcessingTime();
      }
    }
  }

  /**
    * Register a proctime or rowtime timer.
    *
    * @param ctx         the context to register the timer
    * @param isLeft      whether this timer should be registered on the left stream
    * @param cleanupTime timestamp for the timer
    */
  protected def registerTimer(
    ctx: CoProcessFunction[CRow, CRow, CRow]#Context, isLeft: Boolean, cleanupTime: Long): Unit = {
    // Maybe we can register timers for different streams in the future.
    timeIndicator match {
      case JoinTimeIndicator.ROWTIME => {
        ctx.timerService.registerEventTimeTimer(cleanupTime)
      }
      case JoinTimeIndicator.PROCTIME => {
        ctx.timerService.registerProcessingTimeTimer(cleanupTime)
      }
    }
  }

  //********* Functions for temporary test use. *****************//

  def formatTime(time: Long): String = {
    if (0 == time) {
      return "null"
    }
    val f: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss SSS")
    f.format(new Date(time))
  }

  def printCacheSize = {
    var leftSize = 0;
    var rightSize = 0;
    var iterator = leftCache.iterator();
    while (iterator.hasNext) {
      leftSize = leftSize + iterator.next().getValue.size()
    }
    iterator = rightCache.iterator();
    while (iterator.hasNext) {
      rightSize = rightSize + iterator.next().getValue.size()
    }

    println(s"leftSize = $leftSize, rightSize = $rightSize")
  }

  override def toString = s"RowTimeWindowInnerJoin(" +
    s"leftTimerState=${formatTime(leftTimerState.value())}, " +
    s"rightTimerState=${formatTime(rightTimerState.value())}, " +
    s"leftRelativeSize=$leftRelativeSize,  " +
    s"rightRelativeSize=$rightRelativeSize, relativeWindowSize=$relativeWindowSize,  " +
    s"leftOperatorTime=${formatTime(leftOperatorTime)}," +
    s" rightOperatorTime=${formatTime(rightOperatorTime)})"

}

//********* Will be removed before committing. ************//

/**
  * TODO Not sure if that can be replaced by [[org.apache.flink.streaming.api.TimeCharacteristic]]
  */
object JoinTimeIndicator extends Enumeration {
  type JoinTimeIndicator = Value
  val ROWTIME, PROCTIME = Value
}

