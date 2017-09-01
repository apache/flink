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

import org.apache.flink.api.common.state.{ ListState, ListStateDescriptor, MapState, MapStateDescriptor }
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{ FunctionInitializationContext, FunctionSnapshotContext }
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.util.{ Collector, Preconditions }
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import scala.util.control.Breaks._
import org.apache.flink.api.java.tuple.{ Tuple2 => JTuple2 }
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ListTypeInfo
import java.util.Comparator
import java.util.ArrayList
import java.util.Collections
import org.apache.flink.api.common.typeutils.TypeComparator
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import java.util.{List => JList}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import java.util

/**
 * Process Function used for the sort based solely on proctime with offset/fetch
 * [[org.apache.flink.streaming.api.datastream.DataStream]]
 *
 * @param offset Is used to indicate the number of elements to be skipped in the current context
 * (0 offset allows to execute only fetch)
 * @param fetch Is used to indicate the number of elements to be outputted in the current context
 * @param inputType It is used to mark the type of the incoming data
 */
class ProcTimeDescIdentitySortProcessFunctionOffsetFetch(
  private val offset: Int,
  private val fetch: Int,
  private val inputRowType: CRowTypeInfo)
    extends ProcessFunction[CRow, CRow] {

  private var stateEventsBuffer: ListState[Row] = _
  private var stateEventsBufferRetract: MapState[Integer,JList[Row]] = _
  private var bufferedEventsLeftover: ListState[Row] = _
  
  private var outputC: CRow = _
  private var outputR: CRow = _
  private val adjustedFetchLimit = offset + Math.max(fetch, 0)
  
  override def open(config: Configuration) {
    val sortDescriptor = new ListStateDescriptor[Row]("sortState",
        inputRowType.asInstanceOf[CRowTypeInfo].rowType)
    stateEventsBuffer = getRuntimeContext.getListState(sortDescriptor)
    val rowListTypeInfo: TypeInformation[JList[Row]] =
      new ListTypeInfo[Row]( inputRowType.asInstanceOf[CRowTypeInfo].rowType)
        .asInstanceOf[TypeInformation[JList[Row]]]
    val sortDescriptorRetract: MapStateDescriptor[Integer, JList[Row]] =
      new MapStateDescriptor[Integer, JList[Row]]("rowmapstate",
        BasicTypeInfo.INT_TYPE_INFO.asInstanceOf[TypeInformation[Integer]], rowListTypeInfo)
    stateEventsBufferRetract = getRuntimeContext.getMapState(sortDescriptorRetract)
    
    stateEventsBuffer = getRuntimeContext.getListState(sortDescriptor)
    val sortDescriptorLeftRetract = new ListStateDescriptor[Row]("sortStateLeftOverRetract",
        inputRowType.asInstanceOf[CRowTypeInfo].rowType)
    bufferedEventsLeftover = getRuntimeContext.getListState(sortDescriptorLeftRetract)

    val arity:Integer = inputRowType.getArity
    if (outputC == null) {
      outputC = new CRow(Row.of(arity), true)
    }
    
    if (outputR == null) {
      outputR = new CRow(Row.of(arity), false)
    }
    
  }

  override def processElement(
    inputC: CRow,
    ctx: ProcessFunction[CRow, CRow]#Context,
    out: Collector[CRow]): Unit = {

    val input = inputC.row
    
    val currentTime = ctx.timerService.currentProcessingTime
    //buffer the event incoming event
    stateEventsBuffer.add(input)
    
    //deduplication of multiple registered timers is done automatically
    ctx.timerService.registerProcessingTimeTimer(currentTime + 1)  
    
  }
  
  override def onTimer(
    timestamp: Long,
    ctx: ProcessFunction[CRow, CRow]#OnTimerContext,
    out: Collector[CRow]): Unit = {
    
    //retract previous emitted results
    var element: Row = null
    var iElements = 0
    var retractionList = stateEventsBufferRetract.get(0)
    if (retractionList == null) {
      retractionList = new util.ArrayList[Row] 
    }
    
    //add leftover events
    var iter = bufferedEventsLeftover.get.iterator()
    while (iter.hasNext()) {
      stateEventsBuffer.add(iter.next())
    }
    bufferedEventsLeftover.clear()
    
    //we need to build the output and emit the events in order
    iter =  stateEventsBuffer.get.iterator()
    iElements = 0
    while (iter.hasNext()) {
      // display only elements beyond the offset limit
      element = iter.next()
      if (iElements >= offset && (fetch == -1 || iElements < adjustedFetchLimit)) {
        outputC.row = element    
        //for each element we emit we need to retract one ...if fetch is not infinite
        if (fetch!= -1 && retractionList.size() >= fetch) {
          outputR.row = retractionList.get(0)
          retractionList.remove(0)
          out.collect(outputR)
        }
        out.collect(outputC)
        retractionList.add(element)
      } else if (iElements < offset ){
        //add for future use
        bufferedEventsLeftover.add(element)
      }
      iElements += 1
    }
    stateEventsBuffer.clear()
    stateEventsBufferRetract.put(0, retractionList)
    
  }
  
}
