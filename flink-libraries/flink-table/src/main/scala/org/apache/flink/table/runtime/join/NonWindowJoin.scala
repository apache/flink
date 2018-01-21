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

import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.table.api.{StreamQueryConfig, Types}
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.typeutils.TypeCheckUtils._
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * Connect data for left stream and right stream. Base class for stream-stream non-window Join.
  *
  * @param leftType          the input type of left stream
  * @param rightType         the input type of right stream
  * @param resultType        the output type of join
  * @param genJoinFuncName   the function code of other non-equi condition
  * @param genJoinFuncCode   the function name of other non-equi condition
  * @param queryConfig       the configuration for the query to generate
  */
abstract class NonWindowJoin(
    leftType: TypeInformation[Row],
    rightType: TypeInformation[Row],
    resultType: TypeInformation[CRow],
    genJoinFuncName: String,
    genJoinFuncCode: String,
    queryConfig: StreamQueryConfig)
  extends CoProcessFunction[CRow, CRow, CRow]
  with Compiler[FlatJoinFunction[Row, Row, Row]]
  with Logging {

  // check if input types implement proper equals/hashCode
  validateEqualsHashCode("join", leftType)
  validateEqualsHashCode("join", rightType)

  // state to hold left stream element
  protected var leftState: MapState[Row, JTuple2[Int, Long]] = _
  // state to hold right stream element
  protected var rightState: MapState[Row, JTuple2[Int, Long]] = _
  protected var cRowWrapper: CRowWrappingMultiOutputCollector = _

  protected val minRetentionTime: Long = queryConfig.getMinIdleStateRetentionTime
  protected val maxRetentionTime: Long = queryConfig.getMaxIdleStateRetentionTime
  protected val stateCleaningEnabled: Boolean = minRetentionTime > 1

  // state to record last timer of left stream, 0 means no timer
  protected var leftTimer: ValueState[Long] = _
  // state to record last timer of right stream, 0 means no timer
  protected var rightTimer: ValueState[Long] = _

  // other condition function
  protected var joinFunction: FlatJoinFunction[Row, Row, Row] = _

  override def open(parameters: Configuration): Unit = {
    LOG.debug(s"Compiling JoinFunction: $genJoinFuncName \n\n " +
                s"Code:\n$genJoinFuncCode")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genJoinFuncName,
      genJoinFuncCode)
    LOG.debug("Instantiating JoinFunction.")
    joinFunction = clazz.newInstance()

    // initialize left and right state, the first element of tuple2 indicates how many rows of
    // this row, while the second element represents the expired time of this row.
    val tupleTypeInfo = new TupleTypeInfo[JTuple2[Int, Long]](Types.INT, Types.LONG)
    val leftStateDescriptor = new MapStateDescriptor[Row, JTuple2[Int, Long]](
      "left", leftType, tupleTypeInfo)
    val rightStateDescriptor = new MapStateDescriptor[Row, JTuple2[Int, Long]](
      "right", rightType, tupleTypeInfo)
    leftState = getRuntimeContext.getMapState(leftStateDescriptor)
    rightState = getRuntimeContext.getMapState(rightStateDescriptor)

    // initialize timer state
    val valueStateDescriptor1 = new ValueStateDescriptor[Long]("timervaluestate1", classOf[Long])
    leftTimer = getRuntimeContext.getState(valueStateDescriptor1)
    val valueStateDescriptor2 = new ValueStateDescriptor[Long]("timervaluestate2", classOf[Long])
    rightTimer = getRuntimeContext.getState(valueStateDescriptor2)

    cRowWrapper = new CRowWrappingMultiOutputCollector()
  }

  /**
    * Process left stream records
    *
    * @param valueC The input value.
    * @param ctx    The ctx to register timer or get current time
    * @param out    The collector for returning result values.
    *
    */
  override def processElement1(
      valueC: CRow,
      ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
      out: Collector[CRow]): Unit = {

    processElement(valueC, ctx, out, leftTimer, leftState, rightState, isLeft = true)
  }

  /**
    * Process right stream records
    *
    * @param valueC The input value.
    * @param ctx    The ctx to register timer or get current time
    * @param out    The collector for returning result values.
    *
    */
  override def processElement2(
      valueC: CRow,
      ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
      out: Collector[CRow]): Unit = {

    processElement(valueC, ctx, out, rightTimer, rightState, leftState, isLeft = false)
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

    if (stateCleaningEnabled && leftTimer.value == timestamp) {
      expireOutTimeRowForLeft(
        timestamp,
        leftState,
        leftTimer,
        ctx
      )
    }

    if (stateCleaningEnabled && rightTimer.value == timestamp) {
      expireOutTimeRowForRight(
        timestamp,
        rightState,
        rightTimer,
        ctx
      )
    }
  }

  /**
    * Removes records which are expired from left state.
    */
  def expireOutTimeRowForLeft(curTime: Long,
      rowMapState: MapState[Row, JTuple2[Int, Long]],
      timerState: ValueState[Long],
      ctx: CoProcessFunction[CRow, CRow, CRow]#OnTimerContext): Unit = {

    expireOutTimeRow(curTime, rowMapState, timerState, ctx)
  }

  /**
    * Removes records which are expired from right state.
    */
  def expireOutTimeRowForRight(curTime: Long,
      rowMapState: MapState[Row, JTuple2[Int, Long]],
      timerState: ValueState[Long],
      ctx: CoProcessFunction[CRow, CRow, CRow]#OnTimerContext): Unit = {

    expireOutTimeRow(curTime, rowMapState, timerState, ctx)
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

  /**
    * Removes records which are expired from the state. Registers a new timer if the state still
    * holds records after the clean-up.
    */
  def expireOutTimeRow(
      curTime: Long,
      rowMapState: MapState[Row, JTuple2[Int, Long]],
      timerState: ValueState[Long],
      ctx: CoProcessFunction[CRow, CRow, CRow]#OnTimerContext): Unit = {

    val rowMapIter = rowMapState.iterator()
    var validTimestamp: Boolean = false

    while (rowMapIter.hasNext) {
      val mapEntry = rowMapIter.next()
      val recordExpiredTime = mapEntry.getValue.f1
      if (recordExpiredTime <= curTime) {
        rowMapIter.remove()
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

  /**
    * Connect left row and null right row, then collect. The result is NULL from the right side,
    * if there is no match.
    *
    * @param leftRow    The row from left side.
    * @param defaultRow The result row used for output, right side fields will all be null.
    * @param out        The collector for returning result values.
    */
  def collectWithNullRight(leftRow: Row, defaultRow: Row, out: Collector[Row]): Unit = {
    var i = 0;
    while (i < leftRow.getArity) {
      defaultRow.setField(i, leftRow.getField(i))
      i += 1
    }
    out.collect(defaultRow)
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
      isLeft: Boolean): Unit
}
