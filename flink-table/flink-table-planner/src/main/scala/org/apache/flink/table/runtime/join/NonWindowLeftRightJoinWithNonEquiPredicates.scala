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
  * Connect data for left stream and right stream. Only use for left or right join with non-equal
  * predicates. An MapState of type [Row, Long] is used to record how many matched rows for the
  * specified row. Left and right join without non-equal predicates doesn't need it because rows
  * from one side can always join rows from the other side as long as join keys are same.
  *
  * @param leftType        the input type of left stream
  * @param rightType       the input type of right stream
  * @param genJoinFuncName the function code of other non-equi condition
  * @param genJoinFuncCode the function name of other non-equi condition
  * @param isLeftJoin      the type of join, whether it is the type of left join
  * @param queryConfig     the configuration for the query to generate
  */
class NonWindowLeftRightJoinWithNonEquiPredicates(
    leftType: TypeInformation[Row],
    rightType: TypeInformation[Row],
    genJoinFuncName: String,
    genJoinFuncCode: String,
    isLeftJoin: Boolean,
    queryConfig: StreamQueryConfig)
  extends NonWindowOuterJoinWithNonEquiPredicates(
    leftType,
    rightType,
    genJoinFuncName,
    genJoinFuncCode,
    isLeftJoin,
    queryConfig) {

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val joinType = if (isLeftJoin) "Left" else "Right"
    LOG.debug(s"Instantiating NonWindow${joinType}JoinWithNonEquiPredicates.")
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
      currentSideState: MapState[Row, JTuple2[Long, Long]],
      otherSideState: MapState[Row, JTuple2[Long, Long]],
      recordFromLeft: Boolean): Unit = {

    val currentJoinCntState = getJoinCntState(joinCntState, recordFromLeft)
    val inputRow = value.row
    val cntAndExpiredTime = updateCurrentSide(value, ctx, currentSideState)
    if (!value.change && cntAndExpiredTime.f0 <= 0 && recordFromLeft == isLeftJoin) {
      currentJoinCntState.remove(inputRow)
    }

    cRowWrapper.reset()
    cRowWrapper.setCollector(out)
    cRowWrapper.setChange(value.change)
    // join other side data
    if (recordFromLeft == isLeftJoin) {
      val joinCnt = preservedJoin(inputRow, recordFromLeft, otherSideState)
      // init matched cnt only when row cnt is changed from 0 to 1. Each time encountered a
      // new record from the other side, joinCnt will also be updated.
      if (cntAndExpiredTime.f0 == 1 && value.change) {
        currentJoinCntState.put(inputRow, joinCnt)
      }
    } else {
      val otherSideJoinCntState = getJoinCntState(joinCntState, !recordFromLeft)
      retractJoinWithNonEquiPreds(value, recordFromLeft, otherSideState, otherSideJoinCntState)
    }
  }

  /**
    * Called when a processing timer trigger.
    * Expire left/right expired records and expired joinCnt state.
    */
  override def onTimer(
      timestamp: Long,
      ctx: CoProcessFunction[CRow, CRow, CRow]#OnTimerContext,
      out: Collector[CRow]): Unit = {

    // expired timer has already been removed, delete state directly.
    if (stateCleaningEnabled) {
      cleanupState(
        leftState,
        rightState,
        getJoinCntState(joinCntState, isLeftJoin))
    }
  }
}

