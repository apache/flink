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

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ListTypeInfo
import java.util.{ArrayList, List => JList}

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.slf4j.LoggerFactory

/**
  * Process Function used for the aggregate in bounded proc-time OVER window
  * [[org.apache.flink.streaming.api.datastream.DataStream]]
  *
  * @param genAggregations          Generated aggregate helper function
  * @param precedingTimeBoundary    Is used to indicate the processing time boundaries
  * @param aggregatesTypeInfo       row type info of aggregation
  * @param inputType                row type info of input row
  */
class ProcTimeBoundedRangeOver(
    genAggregations: GeneratedAggregationsFunction,
    precedingTimeBoundary: Long,
    aggregatesTypeInfo: RowTypeInfo,
    inputType: TypeInformation[CRow])
  extends ProcessFunction[CRow, CRow]
    with Compiler[GeneratedAggregations] {
  private var output: CRow = _
  private var accumulatorState: ValueState[Row] = _
  private var rowMapState: MapState[Long, JList[Row]] = _

  val LOG = LoggerFactory.getLogger(this.getClass)
  private var function: GeneratedAggregations = _

  override def open(config: Configuration) {
    LOG.debug(s"Compiling AggregateHelper: $genAggregations.name \n\n " +
                s"Code:\n$genAggregations.code")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genAggregations.name,
      genAggregations.code)
    LOG.debug("Instantiating AggregateHelper.")
    function = clazz.newInstance()
    output = new CRow(function.createOutputRow(), true)

    // We keep the elements received in a MapState indexed based on their ingestion time
    val rowListTypeInfo: TypeInformation[JList[Row]] =
      new ListTypeInfo[Row](inputType.asInstanceOf[CRowTypeInfo].rowType)
        .asInstanceOf[TypeInformation[JList[Row]]]
    val mapStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]]("rowmapstate",
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]], rowListTypeInfo)
    rowMapState = getRuntimeContext.getMapState(mapStateDescriptor)

    val stateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("overState", aggregatesTypeInfo)
    accumulatorState = getRuntimeContext.getState(stateDescriptor)
  }

  override def processElement(
    input: CRow,
    ctx: ProcessFunction[CRow, CRow]#Context,
    out: Collector[CRow]): Unit = {

    val currentTime = ctx.timerService.currentProcessingTime
    // buffer the event incoming event

    // add current element to the window list of elements with corresponding timestamp
    var rowList = rowMapState.get(currentTime)
    // null value means that this si the first event received for this timestamp
    if (rowList == null) {
      rowList = new ArrayList[Row]()
      // register timer to process event once the current millisecond passed
      ctx.timerService.registerProcessingTimeTimer(currentTime + 1)
    }
    rowList.add(input.row)
    rowMapState.put(currentTime, rowList)

  }

  override def onTimer(
    timestamp: Long,
    ctx: ProcessFunction[CRow, CRow]#OnTimerContext,
    out: Collector[CRow]): Unit = {

    // we consider the original timestamp of events that have registered this time trigger 1 ms ago
    val currentTime = timestamp - 1
    var i = 0

    // initialize the accumulators
    var accumulators = accumulatorState.value()

    if (null == accumulators) {
      accumulators = function.createAccumulators()
    }

    // update the elements to be removed and retract them from aggregators
    val limit = currentTime - precedingTimeBoundary

    // we iterate through all elements in the window buffer based on timestamp keys
    // when we find timestamps that are out of interest, we retrieve corresponding elements
    // and eliminate them. Multiple elements could have been received at the same timestamp
    // the removal of old elements happens only once per proctime as onTimer is called only once
    val iter = rowMapState.keys.iterator
    val markToRemove = new ArrayList[Long]()
    while (iter.hasNext) {
      val elementKey = iter.next
      if (elementKey < limit) {
        // element key outside of window. Retract values
        val elementsRemove = rowMapState.get(elementKey)
        var iRemove = 0
        while (iRemove < elementsRemove.size()) {
          val retractRow = elementsRemove.get(iRemove)
          function.retract(accumulators, retractRow)
          iRemove += 1
        }
        // mark element for later removal not to modify the iterator over MapState
        markToRemove.add(elementKey)
      }
    }
    // need to remove in 2 steps not to have concurrent access errors via iterator to the MapState
    i = 0
    while (i < markToRemove.size()) {
      rowMapState.remove(markToRemove.get(i))
      i += 1
    }

    // get the list of elements of current proctime
    val currentElements = rowMapState.get(currentTime)
    // add current elements to aggregator. Multiple elements might have arrived in the same proctime
    // the same accumulator value will be computed for all elements
    var iElemenets = 0
    while (iElemenets < currentElements.size()) {
      val input = currentElements.get(iElemenets)
      function.accumulate(accumulators, input)
      iElemenets += 1
    }

    // we need to build the output and emit for every event received at this proctime
    iElemenets = 0
    while (iElemenets < currentElements.size()) {
      val input = currentElements.get(iElemenets)

      // set the fields of the last event to carry on with the aggregates
      function.setForwardedFields(input, output.row)

      // add the accumulators values to result
      function.setAggregationResults(accumulators, output.row)
      out.collect(output)
      iElemenets += 1
    }

    // update the value of accumulators for future incremental computation
    accumulatorState.update(accumulators)

  }

}
