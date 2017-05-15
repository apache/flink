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

import org.apache.flink.api.common.functions.{FlatJoinFunction, RichFilterFunction}
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.table.codegen.{Compiler, GeneratedFunction}
import org.apache.flink.table.runtime.CRowWrappingCollector
import org.apache.flink.table.runtime.aggregate.GeneratedAggregations
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
  * A CoProcessFunction to support stream join stream, currently just support inner-join
  *
  * @param leftStreamWindowSize    the left stream window size
  * @param rightStreamWindowSize    the right stream window size
  * @param element1Type  the input type of left stream
  * @param element2Type  the input type of right stream
  * @param genJoinFuncName    the function code of other non-equi condition
  * @param genJoinFuncCode    the function name of other non-equi condition
  *
  */
class ProcTimeInnerJoin(
  private val leftStreamWindowSize: Long,
  private val rightStreamWindowSize: Long,
  private val element1Type: TypeInformation[Row],
  private val element2Type: TypeInformation[Row],
  private val genJoinFuncName: String,
  private val genJoinFuncCode: String)
  extends CoProcessFunction[CRow, CRow, CRow]
    with Compiler[FlatJoinFunction[Row, Row, Row]]{

  private var cRowWrapper: CRowWrappingCollector = _

  /** other condition function **/
  private var joinFunction: FlatJoinFunction[Row, Row, Row] = _

  /** tmp list to store expired records **/
  private var listToRemove: JList[Long] = _

  /** state to hold left stream element **/
  private var row1MapState: MapState[Long, JList[Row]] = _
  /** state to hold right stream element **/
  private var row2MapState: MapState[Long, JList[Row]] = _

  /** state to record last timer of left stream, 0 means no timer **/
  private var timerState1: ValueState[Long] = _
  /** state to record last timer of right stream, 0 means no timer **/
  private var timerState2: ValueState[Long] = _

  val LOG = LoggerFactory.getLogger(this.getClass)
  override def open(config: Configuration) {
    LOG.debug(s"Compiling JoinFunction: $genJoinFuncName \n\n " +
      s"Code:\n$genJoinFuncCode")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genJoinFuncName,
      genJoinFuncCode)
    LOG.debug("Instantiating JoinFunction.")
    joinFunction = clazz.newInstance()

    listToRemove = new util.ArrayList[Long]()
    cRowWrapper = new CRowWrappingCollector()

    // initialize row state
    val rowListTypeInfo1: TypeInformation[JList[Row]] = new ListTypeInfo[Row](element1Type)
    val mapStateDescriptor1: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]]("row1mapstate",
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]], rowListTypeInfo1)
    row1MapState = getRuntimeContext.getMapState(mapStateDescriptor1)

    val rowListTypeInfo2: TypeInformation[JList[Row]] = new ListTypeInfo[Row](element2Type)
    val mapStateDescriptor2: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]]("row2mapstate",
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]], rowListTypeInfo2)
    row2MapState = getRuntimeContext.getMapState(mapStateDescriptor2)

    // initialize timer state
    val valueStateDescriptor1: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("timervaluestate1", classOf[Long])
    timerState1 = getRuntimeContext.getState(valueStateDescriptor1)

    val valueStateDescriptor2: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("timervaluestate2", classOf[Long])
    timerState2 = getRuntimeContext.getState(valueStateDescriptor2)
  }

  /**
    * Process leftstream records
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

    processElement(
      valueC,
      ctx,
      out,
      leftStreamWindowSize,
      timerState1,
      row1MapState,
      row2MapState,
      rightStreamWindowSize,
      true
    )
  }

  /**
    * Process rightstream records
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

    processElement(
      valueC,
      ctx,
      out,
      rightStreamWindowSize,
      timerState2,
      row2MapState,
      row1MapState,
      leftStreamWindowSize,
      false
    )
  }

  /**
    * Called when a processing timer trigger.
    * Expire left/right records which earlier than current time - windowsize.
    *
    * @param timestamp The timestamp of the firing timer.
    * @param ctx       The ctx to register timer or get current time
    * @param out       The collector for returning result values.
    */
  override def onTimer(
    timestamp: Long,
    ctx: CoProcessFunction[CRow, CRow, CRow]#OnTimerContext,
    out: Collector[CRow]): Unit = {

    if (timerState1.value == timestamp) {
      expireOutTimeRow(
        timestamp,
        leftStreamWindowSize,
        row1MapState,
        timerState1,
        ctx
      )
    }

    if (timerState2.value == timestamp) {
      expireOutTimeRow(
        timestamp,
        rightStreamWindowSize,
        row2MapState,
        timerState2,
        ctx
      )
    }
  }

  /**
    * Puts an element from the input stream into state and search the other state to
    * output records meet the condition, and registers a timer for the current record
    * if there is no timer at present.
    */
  private def processElement(
    valueC: CRow,
    ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
    out: Collector[CRow],
    winSize: Long,
    timerState: ValueState[Long],
    rowMapState: MapState[Long, JList[Row]],
    oppoRowMapState: MapState[Long, JList[Row]],
    oppoWinSize: Long,
    isLeft: Boolean): Unit = {

    cRowWrapper.out = out
    cRowWrapper.setChange(valueC.change)

    val value = valueC.row

    val curProcessTime = ctx.timerService.currentProcessingTime
    val oppoExpiredTime =
      if (oppoWinSize == 0) Long.MinValue else curProcessTime - oppoWinSize

    // only when windowsize != 0, we need to store the element
    if (winSize != 0) {
      // register a timer to expire the element
      if (timerState.value == 0 && winSize != -1) {
        ctx.timerService.registerProcessingTimeTimer(curProcessTime + winSize + 1)
        timerState.update(curProcessTime + winSize + 1)
      }

      var rowList = rowMapState.get(curProcessTime)
      if (rowList == null) {
        rowList = new util.ArrayList[Row]()
      }
      rowList.add(value)
      rowMapState.put(curProcessTime, rowList)

    }

    // loop the the other stream elments
    val oppositeKeyIter = oppoRowMapState.keys().iterator()
    while (oppositeKeyIter.hasNext) {
      val eleTime = oppositeKeyIter.next()
      if (eleTime < oppoExpiredTime) {
        listToRemove.add(eleTime)
      } else {
        val oppoRowList = oppoRowMapState.get(eleTime)
        var i = 0
        while (i < oppoRowList.size) {
          if (isLeft) {
            joinFunction.join(value, oppoRowList.get(i), cRowWrapper)
          } else {
            joinFunction.join(oppoRowList.get(i), value, cRowWrapper)
          }
          i += 1
        }
      }
    }

    // expire records out-of-time
    var i = listToRemove.size() - 1
    while (i >= 0) {
      oppoRowMapState.remove(listToRemove.get(i))
      listToRemove.remove(i)
      i -= 1
    }
  }

  /**
    * expire records which before curTime - windowSize,
    * and register a timer if still exist records.
    * Ensure that one key only has one timer, so register another
    * timer until last timer trigger.
    */
  private def expireOutTimeRow(
    curTime: Long,
    winSize: Long,
    rowMapState: MapState[Long, JList[Row]],
    timerState: ValueState[Long],
    ctx: CoProcessFunction[CRow, CRow, CRow]#OnTimerContext): Unit = {

    val expiredTime = curTime - winSize
    val keyIter = rowMapState.keys().iterator()
    var nextTimer: Long = 0
    // loop the timestamps to find out expired records, when meet one record
    // after the expried timestamp, break the loop. If the keys is ordered, thus
    // can reduce loop num, if the keys is unordered, also can expire at least one
    // element every time the timer trigger
    while (keyIter.hasNext && nextTimer == 0) {
      val curTime = keyIter.next
      if (curTime < expiredTime) {
        listToRemove.add(curTime)
      } else {
        nextTimer = curTime
      }
    }

    var i = listToRemove.size() - 1
    while (i >= 0) {
      rowMapState.remove(listToRemove.get(i))
      listToRemove.remove(i)
      i -= 1
    }

    // if exist records which later than the expire time,
    // register a timer for it, otherwise update the timerState to 0
    // to let processElement register timer when element come
    if (nextTimer != 0) {
      ctx.timerService.registerProcessingTimeTimer(nextTimer + winSize + 1)
      timerState.update(nextTimer + winSize + 1)
    } else {
      timerState.update(0)
    }
  }
}
