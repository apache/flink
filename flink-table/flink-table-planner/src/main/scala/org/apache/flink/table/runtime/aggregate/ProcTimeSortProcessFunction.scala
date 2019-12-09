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
package org.apache.flink.table.runtime.aggregate

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.util.{Collector, Preconditions}
import java.util.ArrayList
import java.util.Collections

import org.apache.flink.streaming.api.operators.TimestampedCollector


/**
 * ProcessFunction to sort on processing time and additional attributes.
 *
 * @param inputRowType The data type of the input data.
 * @param rowComparator A comparator to sort rows.
 */
class ProcTimeSortProcessFunction[K](
    private val inputRowType: CRowTypeInfo,
    private val rowComparator: CollectionRowComparator)
  extends KeyedProcessFunction[K, CRow, CRow] {

  Preconditions.checkNotNull(rowComparator)

  private var bufferedEvents: ListState[Row] = _
  private val sortBuffer: ArrayList[Row] = new ArrayList[Row]
  
  private var outputC: CRow = _
  
  override def open(config: Configuration) {
    val sortDescriptor = new ListStateDescriptor[Row](
      "sortState",
      inputRowType.asInstanceOf[CRowTypeInfo].rowType)
    bufferedEvents = getRuntimeContext.getListState(sortDescriptor)

    outputC = new CRow()
  }

  override def processElement(
    inputC: CRow,
    ctx: KeyedProcessFunction[K, CRow, CRow]#Context,
    out: Collector[CRow]): Unit = {

    val input = inputC.row
    val currentTime = ctx.timerService.currentProcessingTime

    // buffer the event incoming event
    bufferedEvents.add(input)

    // register a timer for the next millisecond to sort and emit buffered data
    ctx.timerService.registerProcessingTimeTimer(currentTime + 1)
    
  }
  
  override def onTimer(
    timestamp: Long,
    ctx: KeyedProcessFunction[K, CRow, CRow]#OnTimerContext,
    out: Collector[CRow]): Unit = {

    // remove timestamp set outside of ProcessFunction.
    out.asInstanceOf[TimestampedCollector[_]].eraseTimestamp()

    val iter =  bufferedEvents.get.iterator()

    // insert all rows into the sort buffer
    sortBuffer.clear()
    while(iter.hasNext) {
      sortBuffer.add(iter.next())
    }
    // sort the rows
    Collections.sort(sortBuffer, rowComparator)
    
    // Emit the rows in order
    var i = 0
    while (i < sortBuffer.size) {
      outputC.row = sortBuffer.get(i)
      out.collect(outputC)
      i += 1
    }
    
    // remove all buffered rows
    bufferedEvents.clear()
  }
  
}
