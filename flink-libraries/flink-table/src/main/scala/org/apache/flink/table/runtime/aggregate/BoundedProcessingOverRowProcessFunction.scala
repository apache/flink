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

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.util.{ Collector, Preconditions }
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.table.functions.{ Accumulator, AggregateFunction }
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.typeinfo.TypeHint
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ListTypeInfo
import java.util.{ ArrayList, LinkedList, List => JList }
import org.apache.flink.api.common.typeinfo.BasicTypeInfo

class BoundedProcessingOverRowProcessFunction(
  private val aggregates: Array[AggregateFunction[_]],
  private val aggFields: Array[Int],
  private val bufferSize: Int,
  private val forwardedFieldCount: Int,
  private val aggregatesTypeInfo: RowTypeInfo,
  private val inputType: TypeInformation[Row])
    extends ProcessFunction[Row, Row] {

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(aggFields)
  Preconditions.checkArgument(aggregates.length == aggFields.length)
  Preconditions.checkArgument(bufferSize > 0)

  private var accumulators: Row = _
  private var output: Row = _
  private var accumulatorState: ValueState[Row] = _
  private var rowMapState: MapState[Long, JList[Row]] = _
  
  override def open(config: Configuration) {
    
    output = new Row(forwardedFieldCount + aggregates.length)
   // We keep the elements received in a list state 
    // together with the ingestion time in the operator
    val rowListTypeInfo: TypeInformation[JList[Row]] =
      new ListTypeInfo[Row](inputType).asInstanceOf[TypeInformation[JList[Row]]]
    
    val mapStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]]("windowBufferMapState",
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]], rowListTypeInfo)
    
    rowMapState = getRuntimeContext.getMapState(mapStateDescriptor)

    val stateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("aggregationState", aggregatesTypeInfo)
      
    accumulatorState = getRuntimeContext.getState(stateDescriptor)
  }

  override def processElement(
    input: Row,
    ctx: ProcessFunction[Row, Row]#Context,
    out: Collector[Row]): Unit = {
    
    val currentTime = ctx.timerService().currentProcessingTime()
    var i = 0

    var accumulators = accumulatorState.value()
    // initialize state for the first processed element
    if(accumulators == null){
      accumulators = new Row(aggregates.length)
      while (i < aggregates.length) {
        accumulators.setField(i, aggregates(i).createAccumulator())
        i += 1
      }
    }
    
    val keyIter = rowMapState.keys.iterator
    var oldestTimeStamp = currentTime
    var toRetract: JList[Row] = null
    var currentKeyTime: Long = 0L
    i = 0
    while(keyIter.hasNext){
      currentKeyTime = keyIter.next
      i += rowMapState.get(currentKeyTime).size()
      if(currentKeyTime <= oldestTimeStamp){
        oldestTimeStamp = currentKeyTime
        toRetract = rowMapState.get(currentKeyTime)
      }
    }
    
    // get oldest element beyond buffer size   
    // and if oldest element exist, retract value
    if(i == bufferSize){
      i = 0 
       while (i < aggregates.length) {
        val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
        aggregates(i).retract(accumulator, toRetract.get(0).getField(aggFields(i)))
        i += 1
      }
      toRetract.remove(0)
      if(!toRetract.isEmpty()){
        rowMapState.put(oldestTimeStamp, toRetract)
      }else{
        rowMapState.remove(oldestTimeStamp)
      }
    }

    //carry on the last element data with aggregates
    i = 0
    while (i < forwardedFieldCount) {
      output.setField(i, input.getField(i))
      i += 1
    }

    //accumulate last value
    i = 0
    while (i < aggregates.length) {
      val index = forwardedFieldCount + i
      val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
      aggregates(i).accumulate(accumulator, input.getField(aggFields(i)))
      output.setField(index, aggregates(i).getValue(accumulator))
      i += 1
    }

    // update map state and accumulator state
    if (rowMapState.contains(currentTime)) {
      rowMapState.get(currentTime).add(input)
    } else { // add new input
      val newList = new ArrayList[Row]
      newList.add(input)
      rowMapState.put(currentTime, newList)
    }
    accumulatorState.update(accumulators)
    
    out.collect(output)
  }

}
