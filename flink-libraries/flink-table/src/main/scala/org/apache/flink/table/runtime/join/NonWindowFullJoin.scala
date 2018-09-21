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
  * Connect data for left stream and right stream. Only use for full outer join without non-equal
  * predicates.
  *
  * @param leftType        the input type of left stream
  * @param rightType       the input type of right stream
  * @param genJoinFuncName the function code without any non-equi condition
  * @param genJoinFuncCode the function name without any non-equi condition
  * @param queryConfig     the configuration for the query to generate
  */
class NonWindowFullJoin(
    leftType: TypeInformation[Row],
    rightType: TypeInformation[Row],
    genJoinFuncName: String,
    genJoinFuncCode: String,
    queryConfig: StreamQueryConfig)
  extends NonWindowOuterJoin(
    leftType,
    rightType,
    genJoinFuncName,
    genJoinFuncCode,
    false,
    queryConfig) {

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    LOG.debug(s"Instantiating NonWindowFullJoin")
  }

  /**
    * Puts or Retract an element from the input stream into state and search the other state to
    * output records meet the condition. The input row will be preserved and appended with null, if
    * there is no match. Records will be expired in state if state retention time has been
    * specified.
    */
  override def processElement(
      value: CRow,
      ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
      out: Collector[CRow],
      timerState: ValueState[Long],
      currentSideState: MapState[Row, JTuple2[Long, Long]],
      otherSideState: MapState[Row, JTuple2[Long, Long]],
      recordFromLeft: Boolean): Unit = {

    val inputRow = value.row
    updateCurrentSide(value, ctx, timerState, currentSideState)

    cRowWrapper.reset()
    cRowWrapper.setCollector(out)
    cRowWrapper.setChange(value.change)

    retractJoin(value, recordFromLeft, currentSideState, otherSideState)

    // preserved current input if there is no matched rows from the other side.
    if (cRowWrapper.getEmitCnt == 0) {
      cRowWrapper.setTimes(1)
      collectAppendNull(inputRow, recordFromLeft, cRowWrapper)
    }
  }
}

