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
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.{RowTypeInfo, ListTypeInfo}
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
import org.apache.flink.configuration.Configuration
import java.util.Comparator
import java.util.ArrayList
import java.util.Collections
import org.apache.flink.api.common.typeutils.TypeComparator
import java.util.{List => JList, ArrayList => JArrayList}

/**
 * Process Function used for the aggregate in bounded rowtime sort without offset/fetch
 * [[org.apache.flink.streaming.api.datastream.DataStream]]
 *
 * @param fieldCount Is used to indicate fields in the current element to forward
 * @param inputType It is used to mark the type of the incoming data
 * @param rowComparator the [[java.util.Comparator]] is used for this sort aggregation
 */
class RowTimeSortProcessFunction(
  private val fieldCount: Int,
  private val inputRowType: RowTypeInfo,
  private val rowComparator: CollectionRowComparator)
    extends ProcessFunction[Row, Row] {

  Preconditions.checkNotNull(rowComparator)

  private val sortArray: ArrayList[Row] = new ArrayList[Row]
  
  // the state which keeps all the events that are not expired.
  // Each timestamp will contain an associated list with the events 
  // received at that timestamp
  private var dataState: MapState[Long, JList[Row]] = _

    // the state which keeps the last triggering timestamp to filter late events
  private var lastTriggeringTsState: ValueState[Long] = _
  
  
  override def open(config: Configuration) {
     
    val keyTypeInformation: TypeInformation[Long] =
      BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]]
    val valueTypeInformation: TypeInformation[JList[Row]] = new ListTypeInfo[Row](inputRowType)

    val mapStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]](
        "dataState",
        keyTypeInformation,
        valueTypeInformation)

    dataState = getRuntimeContext.getMapState(mapStateDescriptor)
    
    val lastTriggeringTsDescriptor: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("lastTriggeringTsState", classOf[Long])
    lastTriggeringTsState = getRuntimeContext.getState(lastTriggeringTsDescriptor)
  }

  
  override def processElement(
    input: Row,
    ctx: ProcessFunction[Row, Row]#Context,
    out: Collector[Row]): Unit = {

    // triggering timestamp for trigger calculation
    val triggeringTs = ctx.timestamp

    val lastTriggeringTs = lastTriggeringTsState.value

    // check if the data is expired, if not, save the data and register event time timer
    if (triggeringTs > lastTriggeringTs) {
      val data = dataState.get(triggeringTs)
      if (null != data) {
        data.add(input)
        dataState.put(triggeringTs, data)
      } else {
        val data = new JArrayList[Row]
        data.add(input)
        dataState.put(triggeringTs, data)
        // register event time timer
        ctx.timerService.registerEventTimeTimer(triggeringTs)
      }
    }
  }
  
  
  override def onTimer(
    timestamp: Long,
    ctx: ProcessFunction[Row, Row]#OnTimerContext,
    out: Collector[Row]): Unit = {
    
    // gets all window data from state for the calculation
    val inputs: JList[Row] = dataState.get(timestamp)

    if (null != inputs) {
      
      var dataListIndex = 0

      // no retraction needed for time order sort
      
      //no selection of offset/fetch
      
      dataListIndex = 0
      sortArray.clear()
      while (dataListIndex < inputs.size()) {
        val curRow = inputs.get(dataListIndex)
        sortArray.add(curRow)
        dataListIndex += 1
      }
      
      //if we do not rely on java collections to do the sort we could implement 
      //an insertion sort as we get the elements  from the state
      Collections.sort(sortArray, rowComparator)
    
    
      //we need to build the output and emit the events in order
      dataListIndex = 0
      while (dataListIndex < sortArray.size) {
        out.collect(sortArray.get(dataListIndex))
        dataListIndex += 1
      }
    
      //we need to  clear the events processed
      dataState.remove(timestamp)
      lastTriggeringTsState.update(timestamp)
    
    }
  }
  
}
