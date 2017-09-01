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

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.util.{Collector, Preconditions}
import java.util.{List => JList}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ListTypeInfo

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
class ProcTimeDescSortProcessFunctionOffsetFetch(
  private val offset: Int,
  private val fetch: Int,
  private val inputRowType: CRowTypeInfo,
  private val rowComparator: CollectionRowComparator)
    extends ProcessFunction[CRow, CRow] {

  Preconditions.checkNotNull(rowComparator)

  private var bufferedEvents: ListState[Row] = _
  private var stateEventsBufferRetract: MapState[Integer,JList[Row]] = _
  private val sortBuffer: ArrayList[Row] = new ArrayList[Row]
  private var bufferedEventsLeftover: ListState[Row] = _
  
  private var outputC: CRow = _
  private var outputR: CRow = _
  private val adjustedFetchLimit = offset + Math.max(fetch, 0)
  
  override def open(config: Configuration) {
    val sortDescriptor = new ListStateDescriptor[Row]("sortState",
        inputRowType.asInstanceOf[CRowTypeInfo].rowType)
    bufferedEvents = getRuntimeContext.getListState(sortDescriptor)
    
    val rowListTypeInfo: TypeInformation[JList[Row]] =
      new ListTypeInfo[Row]( inputRowType.asInstanceOf[CRowTypeInfo].rowType)
        .asInstanceOf[TypeInformation[JList[Row]]]
    val sortDescriptorRetract: MapStateDescriptor[Integer, JList[Row]] =
      new MapStateDescriptor[Integer, JList[Row]]("rowmapstate",
        BasicTypeInfo.INT_TYPE_INFO.asInstanceOf[TypeInformation[Integer]], rowListTypeInfo)
    stateEventsBufferRetract = getRuntimeContext.getMapState(sortDescriptorRetract)
    
    
    val sortDescriptorLeftRetract = new ListStateDescriptor[Row]("sortStateLeftOverRetract",
        inputRowType.asInstanceOf[CRowTypeInfo].rowType)
    bufferedEventsLeftover = getRuntimeContext.getListState(sortDescriptorLeftRetract)

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
    
     //add leftover events
    iter = bufferedEventsLeftover.get.iterator()
    while (iter.hasNext()) {
      sortBuffer.add(iter.next())
    }
    bufferedEventsLeftover.clear()
            

      //retract previous emitted results
    var element: Row = null
    var iElements = 0
    var retractionList = stateEventsBufferRetract.get(0)
    if (retractionList == null) {
      retractionList = new ArrayList[Row] 
    }
    
    //we need to build the output and emit the events in order
    var i = 0
    while (i < sortBuffer.size) {
      // display only elements beyond the offset limit
      if (i >= offset && (fetch == -1 || i < adjustedFetchLimit)) {
        //for each element we emit we need to retract one ...if fetch is not infinite
         if (fetch!= -1 && retractionList.size() >= fetch) {
          outputR.row = retractionList.get(0)
          retractionList.remove(0)
          out.collect(outputR)
        }
        outputC.row = sortBuffer.get(i)   
        out.collect(outputC)
        retractionList.add(sortBuffer.get(i))
      } else if (i < offset ){
        //add for future use
        bufferedEventsLeftover.add(sortBuffer.get(i))
      }
      i += 1
    }
    bufferedEvents.clear()
    stateEventsBufferRetract.put(0, retractionList)
  }
  
}

