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
import java.util.{ ArrayList, LinkedList, List => JList }
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.runtime.aggregate.MultiOutputAggregateFunction

/**
 * Process Function used for the aggregate in bounded proc-time OVER window
 * [[org.apache.flink.streaming.api.datastream.DataStream]]
 *
 * @param aggregates the [[org.apache.flink.table.functions.aggfunctions.SortAggFunction]]
 *                   used for this sort aggregation
 * @param fieldCount Is used to indicate fields in the current element to forward
 * @param aggType It is used to mark the Aggregate type
 */
class ProcTimeUnboundedSortProcessFunction(
  private val aggregates: MultiOutputAggregateFunction[_],
  private val fieldCount: Int,
  private val aggType: TypeInformation[Row])
    extends ProcessFunction[Row, Row] {

  Preconditions.checkNotNull(aggregates)

  private var output: Row = _
  private var accumulatorState: ValueState[Row] = _

  override def open(config: Configuration) {
    output = new Row(fieldCount)

    val stateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("sortState", aggType)
    accumulatorState = getRuntimeContext.getState(stateDescriptor)
  }

  override def processElement(
    input: Row,
    ctx: ProcessFunction[Row, Row]#Context,
    out: Collector[Row]): Unit = {

    val currentTime = ctx.timerService.currentProcessingTime
    //buffer the event incoming event
  
    //initialize the accumulators 
    var accumulators = accumulatorState.value()
    if (null == accumulators) {
      accumulators = new Row(1)
      accumulators.setField(0, aggregates.createAccumulator)
    }
    
    //we aggregate(sort) the events as they arrive. However, this works only
    //if the onTimer is called before the processElement which should be the case
    val accumulator = accumulators.getField(0).asInstanceOf[Accumulator]
    aggregates.accumulate(accumulator, input)    
    accumulatorState.update(accumulators)
    
    //deduplication of multiple registered timers is done automatically
    ctx.timerService.registerProcessingTimeTimer(currentTime + 1)  
    
  }
  
  override def onTimer(
    timestamp: Long,
    ctx: ProcessFunction[Row, Row]#OnTimerContext,
    out: Collector[Row]): Unit = {
    
    var i = 0

    //initialize the accumulators 
    var accumulators = accumulatorState.value()
    if (null == accumulators) {
      accumulators = new Row(1)
      accumulators.setField(0, aggregates.createAccumulator)
    }

    val accumulator = accumulators.getField(0).asInstanceOf[Accumulator]
    
    //no retraction now
    //...
        
    //get the list of elements of current proctime
    var sortedValues = aggregates.getValues(accumulator)
    
    
    //we need to build the output and emit for every event received at this proctime
    var iElemenets = 0
    while (iElemenets < sortedValues.size) {
      val input = sortedValues.get(iElemenets).asInstanceOf[Row]
      
      //set the fields of the last event to carry on with the aggregates
      i = 0
      while (i < fieldCount) {
        output.setField(i, input.getField(i))
        i += 1
      }
      
      out.collect(output)
      iElemenets += 1
    }
    
    //update the value of accumulators for future incremental computation
    aggregates.resetAccumulator(accumulator)
    accumulatorState.update(accumulators)
    
  }
  
}
