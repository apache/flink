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
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import scala.collection.mutable.ListBuffer
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.TupleTypeInfo

/**
  * Process Function used for the aggregate in non-partitioned bounded windows in
  * [[org.apache.flink.streaming.api.datastream.DataStream]]
  *
  * @param aggregates the list of all [[org.apache.flink.table.functions.AggregateFunction]]
  *                   used for this aggregation
  * @param aggFields  the position (in the input Row) of the input value for each aggregate
  * @param forwardedFieldCount Is used to indicate fields in the current element to forward
  * @param rowTypeInfo Is used to indicate the field schema
  * @param time_boundary Is used to indicate the processing time boundaries
  */
class ProcTimeBoundedNonPartitionedProcessingOverProcessFunction(
    private val aggregates: Array[AggregateFunction[_]],
    private val aggFields: Array[Int],
    private val forwardedFieldCount: Int,
    private val rowTypeInfo: RowTypeInfo,
    private val timeBoundary: Long)
  extends ProcessFunction[Row, Row] with CheckpointedFunction{

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(aggFields)
  Preconditions.checkArgument(aggregates.length == aggFields.length)

  private var accumulators: Row = _
  private var output: Row = _
  private var accumulatorState: ListState[Row] = _
  private var windowBufferState: ListState[JTuple2[Long, Row]] = _
  private var windowBuffer: ListBuffer[JTuple2[Long, Row]] = _

  override def open(config: Configuration) {
    output = new Row(forwardedFieldCount + aggregates.length)
    //restore the aggregators in case they exist
    if (null == accumulators) {
      val it = accumulatorState.get.iterator
      if (it.hasNext) {
        accumulators = it.next()
      } else {
        accumulators = new Row(aggregates.length)
        var i = 0
        while (i < aggregates.length) {
          accumulators.setField(i, aggregates(i).createAccumulator())
          i += 1
        }
      }
    }
    //restore the elements in case they exist
    if (windowBuffer==null) {
      windowBuffer = new ListBuffer[JTuple2[Long,Row]]()
      val it = windowBufferState.get.iterator
      if (it.hasNext) {
         windowBuffer.iterator.map(f => windowBuffer.+=(f))
      } 
    }
  }

  override def processElement(
    input: Row,
    ctx: ProcessFunction[Row, Row]#Context,
    out: Collector[Row]): Unit = {
   
     val currentTime = ctx.timerService().currentProcessingTime()
    //buffer the event incoming event
    windowBuffer.+=(new JTuple2(
      currentTime,
      input))
      
    var i = 0

    //set the fields of the last event to carry on with the aggregates
    i = 0
    while (i < forwardedFieldCount) {
      output.setField(i, input.getField(i))
      i += 1
    }

    //update the elements to be removed and retract them from aggregators
    val iter = windowBuffer.iterator
    val limit = currentTime - timeBoundary
    var continue: Boolean = true
    while (continue && iter.hasNext) {
      var currentElement: JTuple2[Long,Row]= iter.next()  
      if (currentElement.f0 < limit) {
        windowBuffer.remove(1)
        i = 0
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
    
    out.collect(output)
  }

  //save the elements in the window and the accumulators
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    accumulatorState.clear()
    if (accumulators != null) {
      accumulatorState.add(accumulators)
    }
    windowBufferState.clear()
    windowBuffer.iterator.map(f => windowBufferState.add(f))
  }

  //initialize the states
  override def initializeState(context: FunctionInitializationContext): Unit = {
    
     val bufferType = new TupleTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, rowTypeInfo)
         .asInstanceOf[TupleTypeInfo[JTuple2[Long, Row]]]
    val bufferDescriptor: ListStateDescriptor[JTuple2[Long,Row]] = 
    new ListStateDescriptor[JTuple2[Long,Row]]("windowBufferState", bufferType)
    windowBufferState = context.getOperatorStateStore.getOperatorState(bufferDescriptor)

    val stateDescriptor: ListStateDescriptor[Row] =
    new ListStateDescriptor[Row]("overState", rowTypeInfo)      
    accumulatorState = context.getOperatorStateStore.getOperatorState(stateDescriptor)
  }
}
