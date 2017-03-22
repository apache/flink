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
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import scala.util.control.Breaks._
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import scala.collection.mutable.Queue
import org.apache.flink.api.common.typeinfo.TypeHint

/**
  * Process Function used for the aggregate in partitioned bounded windows in
  * [[org.apache.flink.streaming.api.datastream.DataStream]]
  *
  * @param aggregates the list of all [[org.apache.flink.table.functions.AggregateFunction]]
  *                   used for this aggregation
  * @param aggFields  the position (in the input Row) of the input value for each aggregate
  * @param forwardedFieldCount Is used to indicate fields in the current element to forward
  * @param rowTypeInfo Is used to indicate the field schema
  * @param timeBoundary Is used to indicate the processing time boundaries
  */
class ProcTimeBoundedProcessingOverProcessFunction(
    private val aggregates: Array[AggregateFunction[_]],
    private val aggFields: Array[Int],
    private val forwardedFieldCount: Int,
    private val rowTypeInfo: RowTypeInfo,
    private val timeBoundary: Long)
  extends ProcessFunction[Row, Row] {

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(aggFields)
  Preconditions.checkArgument(aggregates.length == aggFields.length)

  private var output: Row = _
  private var accumulatorState: ValueState[Row] = _
  private var windowBuffer:  ValueState[Queue[JTuple2[Long,Row]]]= _
    
  override def open(config: Configuration) {
    output = new Row(forwardedFieldCount + aggregates.length)
    
    // We keep the elements received in a list state 
    // together with the ingestion time in the operator
    var queueTypeInfo = new TypeHint[Queue[JTuple2[Long,Row]]](){}.getTypeInfo;
    val bufferDescriptor: ValueStateDescriptor[Queue[JTuple2[Long,Row]]] = 
      new ValueStateDescriptor[Queue[JTuple2[Long,Row]]]("overRetractableState", 
          queueTypeInfo)
    
    windowBuffer = getRuntimeContext.getState(bufferDescriptor)

    val stateDescriptor: ValueStateDescriptor[Row] =
    new ValueStateDescriptor[Row]("overState", rowTypeInfo)      
    accumulatorState = getRuntimeContext.getState(stateDescriptor)
  }

  override def processElement(
    input: Row,
    ctx: ProcessFunction[Row, Row]#Context,
    out: Collector[Row]): Unit = {

    val currentTime = ctx.timerService().currentProcessingTime()
    //buffer the event incoming event
     
    var windowReference = windowBuffer.value()
     if (null == windowReference) {
       windowReference = new Queue[JTuple2[Long,Row]]()
     }
    windowReference.enqueue(new JTuple2(currentTime, input))
      
    var i = 0
    
    //initialize the accumulators 
    var accumulators = accumulatorState.value()
    
    if (null == accumulators) {
      accumulators = new Row(aggregates.length)
      i = 0
      while (i < aggregates.length) {
        accumulators.setField(i, aggregates(i).createAccumulator())
        i += 1
      }
    }

    //set the fields of the last event to carry on with the aggregates
    i = 0
    while (i < forwardedFieldCount) {
      output.setField(i, input.getField(i))
      i += 1
    }

     //update the elements to be removed and retract them from aggregators
    var continue: Boolean = true
    val limit = currentTime - timeBoundary
    while (continue && windowReference.length>0 ) {
      var currentElement: JTuple2[Long,Row]= windowReference.front  
      if (currentElement.f0 < limit) {
        windowReference.dequeue
        i= 0
        while (i < aggregates.length) { 
          val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
          aggregates(i).retract(accumulator, currentElement.f1.getField(aggFields(i)))
          i += 1
        }
      }
      else {
        continue = false
      }        
    }
    
    //add current element to aggregator  
    i = 0
    while (i < aggregates.length) {
      val index = forwardedFieldCount + i
      val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
      aggregates(i).accumulate(accumulator, input.getField(aggFields(i)))
      output.setField(index, aggregates(i).getValue(accumulator))
      i += 1
    }
    
    accumulatorState.update(accumulators)
    windowBuffer.update(windowReference)
    
    out.collect(output)
  }
}
