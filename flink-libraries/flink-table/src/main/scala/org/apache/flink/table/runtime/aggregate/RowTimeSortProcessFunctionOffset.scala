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
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}

/**
 * Process Function used for the aggregate in bounded rowtime sort without offset/fetch
 * [[org.apache.flink.streaming.api.datastream.DataStream]]
 *
 * @param offset Is used to indicate the number of elements to be skipped in the current context
 * @param inputType It is used to mark the type of the incoming data
 * @param rowComparator the [[java.util.Comparator]] is used for this sort aggregation
 */
class RowTimeSortProcessFunctionOffset(
  private val offset: Int,
  private val inputRowType: CRowTypeInfo,
  private val rowComparator: CollectionRowComparator)
    extends ProcessFunction[CRow, CRow] {

  Preconditions.checkNotNull(rowComparator)

  // the state which keeps all the events that are not expired.
  // Each timestamp will contain an associated list with the events 
  // received at that timestamp
  private var dataState: MapState[Long, JList[Row]] = _
  
  // the state which keeps the last triggering timestamp to filter late events
  private var lastTriggeringTsState: ValueState[Long] = _
  
  private var outputC: CRow = _
  private var outputR: CRow = _
  
  override def open(config: Configuration) {
     
    val keyTypeInformation: TypeInformation[Long] =
      BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]]
    val valueTypeInformation: TypeInformation[JList[Row]] = new ListTypeInfo[Row](
        inputRowType.asInstanceOf[CRowTypeInfo].rowType)

    val mapStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]](
        "dataState",
        keyTypeInformation,
        valueTypeInformation)

    dataState = getRuntimeContext.getMapState(mapStateDescriptor)
    
    val lastTriggeringTsDescriptor: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("lastTriggeringTsState", classOf[Long])
    lastTriggeringTsState = getRuntimeContext.getState(lastTriggeringTsDescriptor)

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
    ctx: ProcessFunction[CRow, CRow]#OnTimerContext,
    out: Collector[CRow]): Unit = {

    
    //retract previous elements that were emitted
    val lastTriggeringTs = lastTriggeringTsState.value
    var inputs: JList[Row] = dataState.get(lastTriggeringTs)
    var dataListIndex = 0
    
    if (null != inputs) {
      while (dataListIndex < inputs.size) {
        if (dataListIndex >= offset ) {
          outputR.row = inputs.get(dataListIndex)   
          out.collect(outputR)
        }
        dataListIndex += 1
      }
    }
    
    // gets all window data from state for the calculation
    inputs = dataState.get(timestamp)

    if (null != inputs) {
      
      Collections.sort(inputs, rowComparator)
    
      //we need to build the output and emit the events in order
      var dataListIndex = 0
      while (dataListIndex < inputs.size) {
        if (dataListIndex >= offset ) {
          outputC.row = inputs.get(dataListIndex)  
          out.collect(outputC)
        }
        dataListIndex += 1
      }
    
      //we need to  clear the events processed and keep the sort list for retracting next time
      dataState.put(timestamp, inputs)
    }
    
    //we need to  clear the events retracted
    lastTriggeringTsState.update(timestamp)    
    dataState.remove(lastTriggeringTs)
  }
  
}
