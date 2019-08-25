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
import org.apache.flink.table.api.{StreamQueryConfig, Types}
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row

/**
  * Connect data for left stream and right stream. Base class for stream non-window outer Join
  * with non-equal predicates.
  *
  * @param leftType        the input type of left stream
  * @param rightType       the input type of right stream
  * @param genJoinFuncName the function code of other non-equi condition
  * @param genJoinFuncCode the function name of other non-equi condition
  * @param isLeftJoin      the type of join, whether it is the type of left join
  * @param queryConfig     the configuration for the query to generate
  */
  abstract class NonWindowOuterJoinWithNonEquiPredicates(
    leftType: TypeInformation[Row],
    rightType: TypeInformation[Row],
    genJoinFuncName: String,
    genJoinFuncCode: String,
    isLeftJoin: Boolean,
    queryConfig: StreamQueryConfig)
  extends NonWindowOuterJoin(
    leftType,
    rightType,
    genJoinFuncName,
    genJoinFuncCode,
    isLeftJoin,
    queryConfig) {

  // how many matched rows from the right table for each left row. Index 0 is used for left
  // stream, index 1 is used for right stream.
  protected var joinCntState: Array[MapState[Row, Long]] = _

  protected var countingCollector: CountingCollector = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val arity = leftType.getArity + rightType.getArity
    leftResultRow = new Row(arity)
    rightResultRow = new Row(arity)

    joinCntState = new Array[MapState[Row, Long]](2)
    val leftJoinCntStateDescriptor = new MapStateDescriptor[Row, Long](
      "leftJoinCnt", leftType, Types.LONG.asInstanceOf[TypeInformation[Long]])
    joinCntState(0) = getRuntimeContext.getMapState(leftJoinCntStateDescriptor)
    val rightJoinCntStateDescriptor = new MapStateDescriptor[Row, Long](
      "rightJoinCnt", rightType, Types.LONG.asInstanceOf[TypeInformation[Long]])
    joinCntState(1) = getRuntimeContext.getMapState(rightJoinCntStateDescriptor)

    countingCollector = new CountingCollector()
    LOG.debug(s"Instantiating NonWindowOuterJoin")
  }

  /**
    * Join current row with other side rows when contains non-equal predicates. Retract previous
    * output row if matched condition changed, i.e, matched condition is changed from matched to
    * unmatched or vice versa. The RowWrapper has been reset before we call retractJoin and we
    * also assume that the current change of cRowWrapper is equal to value.change.
    */
  protected def retractJoinWithNonEquiPreds(
      value: CRow,
      inputRowFromLeft: Boolean,
      otherSideState: MapState[Row, JTuple2[Long, Long]],
      otherSideJoinCntState: MapState[Row, Long]): Unit = {

    val inputRow = value.row
    val otherSideIterator = otherSideState.iterator()
    while (otherSideIterator.hasNext) {
      val otherSideEntry = otherSideIterator.next()
      val otherSideRow = otherSideEntry.getKey
      val otherSideCntAndExpiredTime = otherSideEntry.getValue

      countingCollector.reset()
      callJoinFunction(inputRow, inputRowFromLeft, otherSideRow, countingCollector)
      if (countingCollector.getEmitCnt > 0) {
        cRowWrapper.setTimes(otherSideCntAndExpiredTime.f0)
        val joinCnt = otherSideJoinCntState.get(otherSideRow)
        if (value.change) {
          otherSideJoinCntState.put(otherSideRow, joinCnt + 1L)
          if (joinCnt == 0) {
            // retract previous non matched result row
            cRowWrapper.setChange(false)
            collectAppendNull(otherSideRow, !inputRowFromLeft, cRowWrapper)
            // recover for the next iteration
            cRowWrapper.setChange(true)
          }
          // do normal join
          callJoinFunction(inputRow, inputRowFromLeft, otherSideRow, cRowWrapper)
        } else {
          otherSideJoinCntState.put(otherSideRow, joinCnt - 1L)
          // do normal join
          callJoinFunction(inputRow, inputRowFromLeft, otherSideRow, cRowWrapper)
          if (joinCnt == 1) {
            // output non matched result row
            cRowWrapper.setChange(true)
            collectAppendNull(otherSideRow, !inputRowFromLeft, cRowWrapper)
            // recover for the next iteration
            cRowWrapper.setChange(false)
          }
        }
      }
      if (stateCleaningEnabled && curProcessTime >= otherSideCntAndExpiredTime.f1) {
        otherSideIterator.remove()
      }
    }
  }

  /**
    * Get left or right join cnt state.
    *
    * @param joinCntState    the join cnt state array, index 0 is left join cnt state, index 1
    *                        is right
    * @param isLeftCntState the flag whether get the left join cnt state
    * @return the corresponding join cnt state
    */
  protected def getJoinCntState(
      joinCntState: Array[MapState[Row, Long]],
      isLeftCntState: Boolean)
    : MapState[Row, Long] = {

    if (isLeftCntState) {
      joinCntState(0)
    } else {
      joinCntState(1)
    }
  }
}
