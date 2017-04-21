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

import java.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ListTypeInfo
import java.util.{List => JList}

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.codegen.{GeneratedAggregationsFunction, Compiler}
import org.slf4j.LoggerFactory
import org.apache.flink.table.functions.Accumulator

/**
  * Process Function for ROW clause processing-time bounded OVER window
  *
  * @param genAggregations              Generated aggregate helper function
  * @param genDistinctAggregations      Generated aggregate helper function for distinct
  * @param distictAggField              The fields to be aggregated as distinct
  * @param precedingOffset              preceding offset
  * @param aggregatesTypeInfo           row type info of aggregation
  * @param distinctAggregationType      the types for distinct aggregations
  * @param inputType                    row type info of input row
  */
class ProcTimeBoundedRowsOver(
    genAggregations: GeneratedAggregationsFunction,
    genDistinctAggregations: Array[GeneratedAggregationsFunction],
    distinctAggField: Array[Array[Int]],
    precedingOffset: Long,
    aggregatesTypeInfo: RowTypeInfo,
    distinctAggregationType: Array[RowTypeInfo],
    inputType: TypeInformation[Row])
  extends ProcessFunction[Row, Row]
    with Compiler[GeneratedAggregations] {

  Preconditions.checkArgument(precedingOffset > 0)

  private var accumulatorState: ValueState[Row] = _
  private var distinctAccumulatorsState: Array[ValueState[Row]] = _
  private var rowMapState: MapState[Long, JList[Row]] = _
  private var output: Row = _
  private var counterState: ValueState[Long] = _
  private var smallestTsState: ValueState[Long] = _
  private var distinctValueStateList: Array[MapState[Any, Long]] = _

  val LOG = LoggerFactory.getLogger(this.getClass)
  private var function: GeneratedAggregations = _
  private var distFunction: Array[GeneratedAggregations] = _

  override def open(config: Configuration) {
    LOG.debug(s"Compiling AggregateHelper: $genAggregations.name \n\n " +
                s"Code:\n$genAggregations.code")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genAggregations.name,
      genAggregations.code)
    LOG.debug("Instantiating AggregateHelper.")
    function = clazz.newInstance()
    
    // initialize distint functions and state
    distFunction = new Array[GeneratedAggregations](genDistinctAggregations.size)
    distinctAccumulatorsState = new Array(genDistinctAggregations.size)
    distinctValueStateList = new Array(genDistinctAggregations.size)
    for(i <- 0 until genDistinctAggregations.size){
      val distClazz = compile(
        getRuntimeContext.getUserCodeClassLoader,
        genDistinctAggregations(i).name,
        genDistinctAggregations(i).code)
      LOG.debug("Instantiating DistinctAggregateHelper-"+i+".")
      distFunction(i) = distClazz.newInstance()
      
      val distinctValDescriptor = new MapStateDescriptor[Any, Long](
                                         "distinctValuesBufferMapState" + i,
                                         classOf[Any],
                                         classOf[Long])
      distinctValueStateList(i) = getRuntimeContext.getMapState(distinctValDescriptor)
      
      val distinctAggregationStateDescriptor: ValueStateDescriptor[Row] =
        new ValueStateDescriptor[Row]("distinctAggregationState" + i, distinctAggregationType(i))
      distinctAccumulatorsState(i) = getRuntimeContext.getState(distinctAggregationStateDescriptor)
    }
    
    output = new Row(function.createOutputRow().getArity + distFunction.size)

    // We keep the elements received in a Map state keyed
    // by the ingestion time in the operator.
    // we also keep counter of processed elements
    // and timestamp of oldest element
    val rowListTypeInfo: TypeInformation[JList[Row]] =
      new ListTypeInfo[Row](inputType).asInstanceOf[TypeInformation[JList[Row]]]

    val mapStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]]("windowBufferMapState",
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]], rowListTypeInfo)
    rowMapState = getRuntimeContext.getMapState(mapStateDescriptor)

    val aggregationStateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("aggregationState", aggregatesTypeInfo)
    accumulatorState = getRuntimeContext.getState(aggregationStateDescriptor)
    
    
    val processedCountDescriptor : ValueStateDescriptor[Long] =
       new ValueStateDescriptor[Long]("processedCountState", classOf[Long])
    counterState = getRuntimeContext.getState(processedCountDescriptor)

    val smallestTimestampDescriptor : ValueStateDescriptor[Long] =
       new ValueStateDescriptor[Long]("smallestTSState", classOf[Long])
    smallestTsState = getRuntimeContext.getState(smallestTimestampDescriptor)
   
  }

  override def processElement(
    input: Row,
    ctx: ProcessFunction[Row, Row]#Context,
    out: Collector[Row]): Unit = {

    val currentTime = ctx.timerService.currentProcessingTime

    // initialize state for the processed element
    var accumulators = accumulatorState.value
    if (accumulators == null) {
      accumulators = function.createAccumulators()
    }
    
    val distinctAccumulators = new Array[Row](distFunction.size)
    for(i <- 0 until distFunction.size){
      distinctAccumulators(i) = distinctAccumulatorsState(i).value
      if (distinctAccumulators(i) == null) {
        distinctAccumulators(i) = distFunction(i).createAccumulators()
      }
    }
    // get smallest timestamp
    var smallestTs = smallestTsState.value
    if (smallestTs == 0L) {
      smallestTs = currentTime
      smallestTsState.update(smallestTs)
    }
    // get previous counter value
    var counter = counterState.value

    if (counter == precedingOffset) {
      val retractList = rowMapState.get(smallestTs)

      // get oldest element beyond buffer size
      // and if oldest element exist, retract value
      val retractRow = retractList.get(0)
      function.retract(accumulators, retractRow)
      retractList.remove(0)
      
      // check if distinct value should be retracted
      for(i <- 0 until distFunction.size){
        val retractVal = retractRow.getField(distinctAggField(i)(0))

        var distinctValCounter: Long = distinctValueStateList(i).get(retractVal)
        // if the value to be retract is the last one added
        // the remove it and retract the value
        if (distinctValCounter == 1L) {
          distFunction(i).retract(distinctAccumulators(i), retractRow)
          distinctValueStateList(i).remove(retractVal)
          distinctAccumulatorsState(i).update(distinctAccumulators(i))
        } // else if the are other values in the buffer 
          // decrease the counter and continue
        else {
          distinctValCounter -= 1
          distinctValueStateList(i).put(retractVal, distinctValCounter)
        }
      }
      
      

      // if reference timestamp list not empty, keep the list
      if (!retractList.isEmpty) {
        rowMapState.put(smallestTs, retractList)
      } // if smallest timestamp list is empty, remove and find new smallest
      else {
        rowMapState.remove(smallestTs)
        val iter = rowMapState.keys.iterator
        var currentTs: Long = 0L
        var newSmallestTs: Long = Long.MaxValue
        while (iter.hasNext) {
          currentTs = iter.next
          if (currentTs < newSmallestTs) {
            newSmallestTs = currentTs
          }
        }
        smallestTsState.update(newSmallestTs)
      }
    } // we update the counter only while buffer is getting filled
    else {
      counter += 1
      counterState.update(counter)
    }

    // copy forwarded fields in output row
    function.setForwardedFields(input, output)

    // accumulate current row and set aggregate in output row
    function.accumulate(accumulators, input)
    function.setAggregationResults(accumulators, output)
    
    for(i <- 0 until distFunction.size){
      val inputValue = input.getField(distinctAggField(i)(0))
      var distinctValCounter: Long = distinctValueStateList(i).get(inputValue)
      // if counter is 0L, it is the first time we aggregate
      // for this value and have therefore to accumulated
      if (distinctValCounter == 0L) {
        distFunction(i).accumulate(distinctAccumulators(i), input)
        // we update the state just when it is updated for the specific accumulator
        distinctAccumulatorsState(i).update(distinctAccumulators(i))
      }
      distinctValCounter += 1
      distinctValueStateList(i).put(inputValue, distinctValCounter)
      // set output result of each distint aggregation
      distFunction(i).setAggregationResults(distinctAccumulators(i), output)
    }

    // update map state, accumulator state, counter and timestamp
    val currentTimeState = rowMapState.get(currentTime)
    if (currentTimeState != null) {
      currentTimeState.add(input)
      rowMapState.put(currentTime, currentTimeState)
    } else { // add new input
      val newList = new util.ArrayList[Row]
      newList.add(input)
      rowMapState.put(currentTime, newList)
    }

    accumulatorState.update(accumulators)

    out.collect(output)
  }

}
