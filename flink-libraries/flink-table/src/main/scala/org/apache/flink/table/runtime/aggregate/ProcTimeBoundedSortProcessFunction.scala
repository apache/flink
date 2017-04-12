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
import org.apache.flink.table.functions.{ Accumulator, AggregateFunction }
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
import java.util.LinkedList
import java.util.Comparator


/**
 * Process Function used for the aggregate in bounded proctime sort without offset/fetch
 * [[org.apache.flink.streaming.api.datastream.DataStream]]
 *
 * @param fieldCount Is used to indicate fields in the current element to forward
 * @param inputType It is used to mark the type of the incoming data
 * @param rowComparator the [[java.util.Comparator]] is used for this sort aggregation
 */
class ProcTimeBoundedSortProcessFunction(
  private val fieldCount: Int,
  private val inputType: TypeInformation[Row],
  private val rowComparator:Comparator[Row])
    extends ProcessFunction[Row, Row] {

  Preconditions.checkNotNull(rowComparator)

  private var stateEventsBuffer: ListState[Row] = _

  override def open(config: Configuration) {
    val sortDescriptor = new ListStateDescriptor[Row]("sortState", inputType)
    stateEventsBuffer = getRuntimeContext.getListState(sortDescriptor)
  }

  override def processElement(
    input: Row,
    ctx: ProcessFunction[Row, Row]#Context,
    out: Collector[Row]): Unit = {

    val currentTime = ctx.timerService.currentProcessingTime
    //buffer the event incoming event
  
    //we accumulate the events as they arrive within the given proctime
    stateEventsBuffer.add(input)
    
    //deduplication of multiple registered timers is done automatically
    ctx.timerService.registerProcessingTimeTimer(currentTime + 1)  
    
  }
  
  override def onTimer(
    timestamp: Long,
    ctx: ProcessFunction[Row, Row]#OnTimerContext,
    out: Collector[Row]): Unit = {
    
    var i = 0
    val sortList = new LinkedList[Row]
    val iter =  stateEventsBuffer.get.iterator()
    
    
    while(iter.hasNext())
    {
      sortList.add(iter.next())  
    }
    
    //if we do not rely on java collections to do the sort we could implement 
    //an insertion sort as we get the elements  from the state
    sortList.sort(rowComparator)
    
    //no retraction now
            
    //no selection of offset/fetch
    
    //we need to build the output and emit the events in order
    var iElemenets = 0
    while (iElemenets < sortList.size) {
      out.collect(sortList.get(iElemenets))
      iElemenets += 1
    }
    
    //we need to  clear the events accumulated in the last millisecond
    stateEventsBuffer.clear()
    
  }
  
}
