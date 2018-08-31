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
  * Connect data for left stream and right stream. Only use for innerJoin.
  *
  * @param leftType          the input type of left stream
  * @param rightType         the input type of right stream
  * @param resultType        the output type of join
  * @param genJoinFuncName   the function code of other non-equi condition
  * @param genJoinFuncCode   the function name of other non-equi condition
  * @param queryConfig       the configuration for the query to generate
  */
class NonWindowInnerJoin(
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

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    LOG.debug("Instantiating NonWindowInnerJoin.")
  }

  /**
    * Puts or Retract an element from the input stream into state and search the other state to
    * output records meet the condition. Records will be expired in state if state retention time
    * has been specified.
    */
  override def processElement(
      value: CRow,
      ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
      out: Collector[CRow],
      timerState: ValueState[Long],
      currentSideState: MapState[Row, JTuple2[Long, Long]],
      otherSideState: MapState[Row, JTuple2[Long, Long]],
      isLeft: Boolean): Unit = {

    val inputRow = value.row
    updateCurrentSide(value, ctx, timerState, currentSideState)

    cRowWrapper.setCollector(out)
    cRowWrapper.setChange(value.change)
    val otherSideIterator = otherSideState.iterator()
    // join other side data
    while (otherSideIterator.hasNext) {
      val otherSideEntry = otherSideIterator.next()
      val otherSideRow = otherSideEntry.getKey
      val otherSideCntAndExpiredTime = otherSideEntry.getValue
      // join
      cRowWrapper.setTimes(otherSideCntAndExpiredTime.f0)
      callJoinFunction(inputRow, isLeft, otherSideRow, cRowWrapper)
      // clear expired data. Note: clear after join to keep closer to the original semantics
      if (stateCleaningEnabled && curProcessTime >= otherSideCntAndExpiredTime.f1) {
        otherSideIterator.remove()
      }
    }
  }
}
