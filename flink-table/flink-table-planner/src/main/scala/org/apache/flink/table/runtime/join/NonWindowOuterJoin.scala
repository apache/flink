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
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * Connect data for left stream and right stream. Base class for stream non-window outer Join.
  *
  * @param leftType        the input type of left stream
  * @param rightType       the input type of right stream
  * @param genJoinFuncName the function code of other non-equi condition
  * @param genJoinFuncCode the function name of other non-equi condition
  * @param isLeftJoin      the type of join, whether it is the type of left join
  * @param config          configuration that determines runtime behavior
  */
abstract class NonWindowOuterJoin(
    leftType: TypeInformation[Row],
    rightType: TypeInformation[Row],
    genJoinFuncName: String,
    genJoinFuncCode: String,
    isLeftJoin: Boolean,
    minRetentionTime: Long,
    maxRetentionTime: Long)
  extends NonWindowJoin(
    leftType,
    rightType,
    genJoinFuncName,
    genJoinFuncCode,
    minRetentionTime,
    maxRetentionTime) {

  // result row, all fields from right will be null. Used for output when there is no matched rows.
  protected var leftResultRow: Row = _
  // result row, all fields from left will be null. Used for output when there is no matched rows.
  protected var rightResultRow: Row = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val arity = leftType.getArity + rightType.getArity
    leftResultRow = new Row(arity)
    rightResultRow = new Row(arity)
    LOG.debug(s"Instantiating NonWindowOuterJoin")
  }

  /**
    * Join current row with other side rows. Preserve current row if there are no matched rows
    * from other side. The RowWrapper has been reset before we call preservedJoin and we also
    * assume that the current change of cRowWrapper is equal to value.change.
    *
    * @param inputRow         the input row
    * @param inputRowFromLeft the flag indicat whether input row is from left
    * @param otherSideState   the other side state
    * @return the number of matched rows
    */
  protected def preservedJoin(
      inputRow: Row,
      inputRowFromLeft: Boolean,
      otherSideState: MapState[Row, JTuple2[Long, Long]]): Long = {

    val otherSideIterator = otherSideState.iterator()
    while (otherSideIterator.hasNext) {
      val otherSideEntry = otherSideIterator.next()
      val otherSideRow = otherSideEntry.getKey
      val otherSideCntAndExpiredTime = otherSideEntry.getValue
      // join
      cRowWrapper.setTimes(otherSideCntAndExpiredTime.f0)
      callJoinFunction(inputRow, inputRowFromLeft, otherSideRow, cRowWrapper)
      // clear expired data. Note: clear after join to keep closer to the original semantics
      if (stateCleaningEnabled && curProcessTime >= otherSideCntAndExpiredTime.f1) {
        otherSideIterator.remove()
      }
    }
    val joinCnt = cRowWrapper.getEmitCnt
    // The result is NULL from the other side, if there is no match.
    if (joinCnt == 0) {
      cRowWrapper.setTimes(1)
      collectAppendNull(inputRow, inputRowFromLeft, cRowWrapper)
    }
    joinCnt
  }

  /**
    * Join current row with other side rows. Retract previous output row if matched condition
    * changed, i.e, matched condition is changed from matched to unmatched or vice versa. The
    * RowWrapper has been reset before we call retractJoin and we also assume that the current
    * change of cRowWrapper is equal to value.change.
    */
  protected def retractJoin(
      value: CRow,
      inputRowFromLeft: Boolean,
      currentSideState: MapState[Row, JTuple2[Long, Long]],
      otherSideState: MapState[Row, JTuple2[Long, Long]]): Unit = {

    val inputRow = value.row
    val otherSideIterator = otherSideState.iterator()
    // approximate number of record in current side. We only check whether number equals to 0, 1
    // or bigger
    val recordNum: Long = approxiRecordNumInState(currentSideState)

    while (otherSideIterator.hasNext) {
      val otherSideEntry = otherSideIterator.next()
      val otherSideRow = otherSideEntry.getKey
      val otherSideCntAndExpiredTime = otherSideEntry.getValue
      cRowWrapper.setTimes(otherSideCntAndExpiredTime.f0)

      // retract previous preserved record append with null
      if (recordNum == 1 && value.change) {
        cRowWrapper.setChange(false)
        collectAppendNull(otherSideRow, !inputRowFromLeft, cRowWrapper)
        // recover for the next iteration
        cRowWrapper.setChange(true)
      }
      // do normal join
      callJoinFunction(inputRow, inputRowFromLeft, otherSideRow, cRowWrapper)

      // output preserved record append with null if have to
      if (!value.change && recordNum == 0) {
        cRowWrapper.setChange(true)
        collectAppendNull(otherSideRow, !inputRowFromLeft, cRowWrapper)
        // recover for the next iteration
        cRowWrapper.setChange(false)
      }
      // clear expired data. Note: clear after join to keep closer to the original semantics
      if (stateCleaningEnabled && curProcessTime >= otherSideCntAndExpiredTime.f1) {
        otherSideIterator.remove()
      }
    }
  }

  /**
    * Return approximate number of records in corresponding state. Only check if record number is
    * 0, 1 or bigger.
    */
  protected def approxiRecordNumInState(
      currentSideState: MapState[Row, JTuple2[Long, Long]]): Long = {
    var recordNum = 0L
    val it = currentSideState.iterator()
    while(it.hasNext && recordNum < 2) {
      recordNum += it.next().getValue.f0
    }
    recordNum
  }

  /**
    * Append input row with default null value if there is no match and Collect.
    */
  protected def collectAppendNull(
      inputRow: Row,
      inputFromLeft: Boolean,
      out: Collector[Row]): Unit = {

    var i = 0
    if (inputFromLeft) {
      while (i < inputRow.getArity) {
        leftResultRow.setField(i, inputRow.getField(i))
        i += 1
      }
      out.collect(leftResultRow)
    } else {
      while (i < inputRow.getArity) {
        val idx = rightResultRow.getArity - inputRow.getArity + i
        rightResultRow.setField(idx, inputRow.getField(i))
        i += 1
      }
      out.collect(rightResultRow)
    }
  }
}
