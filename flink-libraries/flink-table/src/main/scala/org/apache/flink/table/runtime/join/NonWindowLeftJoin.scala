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

import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * Connect data for left stream and right stream. Only use for LeftJoin without NonEquiPredicates.
  *
  * @param leftType        the input type of left stream
  * @param rightType       the input type of right stream
  * @param resultType      the output type of join
  * @param genJoinFuncName the function code of other non-equi condition
  * @param genJoinFuncCode the function name of other non-equi condition
  * @param queryConfig     the configuration for the query to generate
  */
class NonWindowLeftJoin(
    leftType: TypeInformation[Row],
    rightType: TypeInformation[Row],
    resultType: TypeInformation[CRow],
    genJoinFuncName: String,
    genJoinFuncCode: String,
    queryConfig: StreamQueryConfig)
  extends NonWindowJoin(
    leftType,
    rightType,
    resultType,
    genJoinFuncName,
    genJoinFuncCode,
    queryConfig) {

  // result row, all fields from right will be null
  private var resultRow: Row = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    resultRow = new Row(resultType.getArity)
    LOG.debug("Instantiating NonWindowLeftJoin.")
  }

  /**
    * Puts or Retract an element from the input stream into state and search the other state to
    * output records meet the condition. The result is NULL from the right side, if there is no
    * match. Records will be expired in state if state retention time has been specified.
    */
  override def processElement(
      value: CRow,
      ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
      out: Collector[CRow],
      timerState: ValueState[Long],
      currentSideState: MapState[Row, JTuple2[Int, Long]],
      otherSideState: MapState[Row, JTuple2[Int, Long]],
      isLeft: Boolean): Unit = {

    val inputRow = value.row
    cRowWrapper.reset()
    cRowWrapper.setCollector(out)
    cRowWrapper.setChange(value.change)

    val curProcessTime = ctx.timerService.currentProcessingTime
    val oldCntAndExpiredTime = currentSideState.get(inputRow)
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

    // update current side stream state
    if (!value.change) {
      cntAndExpiredTime.f0 = cntAndExpiredTime.f0 - 1
      if (cntAndExpiredTime.f0 <= 0) {
        currentSideState.remove(inputRow)
      } else {
        currentSideState.put(inputRow, cntAndExpiredTime)
      }
    } else {
      cntAndExpiredTime.f0 = cntAndExpiredTime.f0 + 1
      currentSideState.put(inputRow, cntAndExpiredTime)
    }

    val otherSideIterator = otherSideState.iterator()
    cRowWrapper.setEmitCnt(0)
    // join other side data
    if (isLeft) {
      while (otherSideIterator.hasNext) {
        val otherSideEntry = otherSideIterator.next()
        val otherSideRow = otherSideEntry.getKey
        val cntAndExpiredTime = otherSideEntry.getValue
        // join
        cRowWrapper.setTimes(cntAndExpiredTime.f0)
        joinFunction.join(inputRow, otherSideRow, cRowWrapper)
        // clear expired data. Note: clear after join to keep closer to the original semantics
        if (stateCleaningEnabled && curProcessTime >= cntAndExpiredTime.f1) {
          otherSideIterator.remove()
        }
      }
      // The result is NULL from the right side, if there is no match.
      if (cRowWrapper.getEmitCnt == 0) {
        cRowWrapper.setTimes(1)
        collectWithNullRight(inputRow, resultRow, cRowWrapper)
      }
    } else {

      // number of right keys, here we only check whether key number equals to 0 or 1.
      val rigthKeyNum = getRightKeysNumber
      // whether retract null right output for current input
      var retractFlag = false
      // whether emit null right output for current input
      var hasReEmittedNullRight = false

      while (otherSideIterator.hasNext) {
        val otherSideEntry = otherSideIterator.next()
        val otherSideRow = otherSideEntry.getKey
        val cntAndExpiredTime = otherSideEntry.getValue
        cRowWrapper.setTimes(cntAndExpiredTime.f0)

        // retract previous record with null right
        if (rigthKeyNum == 1 && value.change) {
          cRowWrapper.setChange(false)
          collectWithNullRight(otherSideRow, resultRow, cRowWrapper)
          retractFlag = true
          cRowWrapper.setChange(true)
        }
        // do normal join
        joinFunction.join(otherSideRow, inputRow, cRowWrapper)
        // output with null right if have to
        if (!value.change && rigthKeyNum == 0) {
          cRowWrapper.setChange(true)
          collectWithNullRight(otherSideRow, resultRow, cRowWrapper)
          hasReEmittedNullRight = true
          cRowWrapper.setChange(false)
        }
      }
    }
  }

  /**
    * Return rightState's entry number. Only return 0, 1, 2 because only these numbers matter.
    */
  private def getRightKeysNumber: Int = {
    var tmpSum = 0
    val it = rightState.iterator()
    while(it.hasNext && tmpSum < 2) {
      tmpSum += 1
      it.next()
    }
    tmpSum
  }
}

