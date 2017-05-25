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

import org.apache.flink.api.common.state.{ ListState, ListStateDescriptor }
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
import java.lang.{Integer=>JInt}

/**
 * Process Function used for the aggregate in bounded proctime sort without offset/fetch
 * [[org.apache.flink.streaming.api.datastream.DataStream]]
 *
 * @param inputRowType It is used to mark the type of the incoming data
 * @param rowComparator the [[java.util.Comparator]] is used for this sort aggregation
 */
class ProcTimeSortProcessFunction(
  private val inputRowType: CRowTypeInfo,
  private val rowComparator: CollectionRowComparator)
    extends ProcessFunction[CRow, CRow] {

  Preconditions.checkNotNull(rowComparator)

  private var stateEventsBuffer: ListState[Row] = _
  private val sortArray: ArrayList[Row] = new ArrayList[Row]
  
  private var outputC: CRow = _
  
  override def open(config: Configuration) {
    val sortDescriptor = new ListStateDescriptor[Row]("sortState",
        inputRowType.asInstanceOf[CRowTypeInfo].rowType)
    stateEventsBuffer = getRuntimeContext.getListState(sortDescriptor)
    
    if (outputC == null) {
      val arity:Integer = inputRowType.getArity
      outputC = new CRow(Row.of(arity), true)
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
    
    val iter =  stateEventsBuffer.get.iterator()
    
    sortArray.clear()
    while(iter.hasNext()) {
      sortArray.add(iter.next())
    }
    
    //if we do not rely on java collections to do the sort we could implement 
    //an insertion sort as we get the elements  from the state
    Collections.sort(sortArray, rowComparator)
    
    //we need to build the output and emit the events in order
    var iElemenets = 0
    while (iElemenets < sortArray.size) {
      // do we need to recreate the object no to mess references in previous results?
      outputC.row = sortArray.get(iElemenets)   
      out.collect(outputC)
      iElemenets += 1
    }
    
    //we need to  clear the events accumulated in the last millisecond
    stateEventsBuffer.clear()
    
  }
  
}
