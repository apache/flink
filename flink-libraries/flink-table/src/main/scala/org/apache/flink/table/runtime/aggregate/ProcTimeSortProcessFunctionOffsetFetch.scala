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
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.util.{Collector, Preconditions}

import java.util.ArrayList
import java.util.Collections


/**
 * ProcessFunction to sort on processing time and additional attributes with offset/fetch
 *
 * @param offset Is used to indicate the number of elements to be skipped in the current context
 * (0 offset allows to execute only fetch)
 * @param fetch Is used to indicate the number of elements to be outputted in the current context
 * @param inputType It is used to mark the type of the incoming data
 * @param rowComparator the [[java.util.Comparator]] is used for this sort aggregation
 */
class ProcTimeSortProcessFunctionOffsetFetch(
  private val offset: Int,
  private val fetch: Int,
  private val inputRowType: CRowTypeInfo,
  private val rowComparator: CollectionRowComparator)
    extends ProcessFunction[CRow, CRow] {

  Preconditions.checkNotNull(rowComparator)

  private var bufferedEvents: ListState[Row] = _
  private var bufferedEventsRetract: ListState[Row] = _
  private val sortBuffer: ArrayList[Row] = new ArrayList[Row]
  
  private var outputC: CRow = _
  private var outputR: CRow = _
  private val adjustedFetchLimit = offset + fetch
  
  override def open(config: Configuration) {
    val sortDescriptor = new ListStateDescriptor[Row]("sortState",
        inputRowType.asInstanceOf[CRowTypeInfo].rowType)
    bufferedEvents = getRuntimeContext.getListState(sortDescriptor)
    val sortDescriptorRetract = new ListStateDescriptor[Row]("sortStateRetract",
        inputRowType.asInstanceOf[CRowTypeInfo].rowType)
    bufferedEventsRetract = getRuntimeContext.getListState(sortDescriptorRetract)

    val arity:Integer = inputRowType.getArity
    outputC = new CRow()
    outputR = new CRow(Row.of(arity), false)
    
  }

  override def processElement(
    inputC: CRow,
    ctx: ProcessFunction[CRow, CRow]#Context,
    out: Collector[CRow]): Unit = {

    val input = inputC.row
    
    val currentTime = ctx.timerService.currentProcessingTime
    //buffer the event incoming event
  
    //we accumulate the events as they arrive within the given proctime
    bufferedEvents.add(input)
    
    // register a timer for the next millisecond to sort and emit buffered data
    ctx.timerService.registerProcessingTimeTimer(currentTime + 1)  
    
  }
  
  override def onTimer(
    timestamp: Long,
    ctx: ProcessFunction[CRow, CRow]#OnTimerContext,
    out: Collector[CRow]): Unit = {
    
    var iter =  bufferedEvents.get.iterator()
    
    sortBuffer.clear()
    while(iter.hasNext()) {
      sortBuffer.add(iter.next())
    }
    
    Collections.sort(sortBuffer, rowComparator)
            
    //retract previous emitted results
    var element: Row = null
    iter = bufferedEventsRetract.get.iterator()
    while (iter.hasNext) {
      outputR.row = iter.next()   
      out.collect(outputR)
    }
    bufferedEventsRetract.clear()
    
    //we need to build the output and emit the events in order
    var i = 0
    while (i < sortBuffer.size) {
      // display only elements beyond the offset limit
      if (i >= offset && i < adjustedFetchLimit) {
        outputC.row = sortBuffer.get(i)   
        out.collect(outputC)
        bufferedEventsRetract.add(sortBuffer.get(i))
      }
      i += 1
    }
    bufferedEvents.clear()
    
  }
  
}

