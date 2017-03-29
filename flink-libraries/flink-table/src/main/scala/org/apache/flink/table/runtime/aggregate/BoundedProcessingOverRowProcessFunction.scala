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
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ListTypeInfo
import java.util.{ ArrayList, LinkedList, List => JList }
import org.apache.flink.api.common.typeinfo.BasicTypeInfo

class BoundedProcessingOverRowProcessFunction(
  private val aggregates: Array[AggregateFunction[_]],
  private val aggFields: Array[Int],
  private val precedingOffset: Int,
  private val forwardedFieldCount: Int,
  private val aggregatesTypeInfo: RowTypeInfo,
  private val inputType: TypeInformation[Row])
    extends ProcessFunction[Row, Row] {

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(aggFields)
  Preconditions.checkArgument(aggregates.length == aggFields.length)
  Preconditions.checkArgument(precedingOffset > 0)

  private var accumulators: Row = _
  private var accumulatorState: ValueState[Row] = _
  private var rowMapState: MapState[Long, JList[Row]] = _
  private var output: Row = _
  private var counterState: ValueState[Long] = _
  private var counter : Long = _
  private var smallestTsState: ValueState[Long] = _
  private var smallestTs : Long = _
  
  override def open(config: Configuration) {
    
    output = new Row(forwardedFieldCount + aggregates.length)
    // We keep the elements received in a list state 
    // together with the ingestion time in the operator
    // we also keep counter of processed elements
    // and timestamp of oldest element
    val rowListTypeInfo: TypeInformation[JList[Row]] =
      new ListTypeInfo[Row](inputType).asInstanceOf[TypeInformation[JList[Row]]]
    
    val mapStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]]("windowBufferMapState",
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]], rowListTypeInfo)
    
    rowMapState = getRuntimeContext.getMapState(mapStateDescriptor)

    val stateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("aggregationState", aggregatesTypeInfo)
      
    accumulatorState = getRuntimeContext.getState(stateDescriptor)
    
    val processedCountDescriptor : ValueStateDescriptor[Long] =
       new ValueStateDescriptor[Long]("processedCountState", classOf[Long])
    
    counterState = getRuntimeContext.getState(processedCountDescriptor)
    
    val smallesTimestampDescriptor : ValueStateDescriptor[Long] =
       new ValueStateDescriptor[Long]("smallesTSState", classOf[Long])
    
    smallestTsState = getRuntimeContext.getState(smallesTimestampDescriptor)
    
  }

  override def processElement(
    input: Row,
    ctx: ProcessFunction[Row, Row]#Context,
    out: Collector[Row]): Unit = {
    
    val currentTime = ctx.timerService().currentProcessingTime()
    var i = 0

    accumulators = accumulatorState.value()
    // initialize state for the first processed element
    if(accumulators == null){
      accumulators = new Row(aggregates.length)
      while (i < aggregates.length) {
        accumulators.setField(i, aggregates(i).createAccumulator())
        i += 1
      }
    }
    
    // get smallest timestamp 
    smallestTs = smallestTsState.value()
    if(smallestTs == 0L){
      smallestTs = currentTime
    }
    // get previous counter value
    counter = counterState.value()
    
    if (counter == precedingOffset) {
      val retractTs = smallestTs
      val retractList = rowMapState.get(smallestTs)

      // get oldest element beyond buffer size   
      // and if oldest element exist, retract value
      i = 0
      while (i < aggregates.length) {
        val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
        aggregates(i).retract(accumulator, retractList.get(0).getField(aggFields(i)))
        i += 1
      }
      retractList.remove(0)
      counter -= 1
      // if reference timestamp list not empty, keep the list
      if (!retractList.isEmpty()) {
        rowMapState.put(retractTs, retractList)
      } // if smallest timestamp list is empty, remove and find new smallest
      else {
        rowMapState.remove(retractTs)
        val iter = rowMapState.keys.iterator()
        var currentTs: Long = 0L
        var newSmallesTs: Long = Long.MaxValue
        while(iter.hasNext){
          currentTs = iter.next
          if(currentTs < newSmallesTs){
            newSmallesTs = currentTs
          }
        }
        smallestTs = newSmallesTs
      }
    }

    // copy forwarded fields in output row
    i = 0
    while (i < forwardedFieldCount) {
      output.setField(i, input.getField(i))
      i += 1
    }

    // accumulate current row and set aggregate in output row
    i = 0
    while (i < aggregates.length) {
      val index = forwardedFieldCount + i
      val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
      aggregates(i).accumulate(accumulator, input.getField(aggFields(i)))
      output.setField(index, aggregates(i).getValue(accumulator))
      i += 1
    }

    // update map state, accumulator state, counter and timestamp
    if (rowMapState.contains(currentTime)) {
      rowMapState.get(currentTime).add(input)
    } else { // add new input
      val newList = new ArrayList[Row]
      newList.add(input)
      rowMapState.put(currentTime, newList)
    }
    counter += 1
    accumulatorState.update(accumulators)
    smallestTsState.update(smallestTs)
    counterState.update(counter)
    out.collect(output)
  }

}
