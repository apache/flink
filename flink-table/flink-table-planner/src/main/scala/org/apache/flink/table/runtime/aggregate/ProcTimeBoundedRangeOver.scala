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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ListTypeInfo
import java.util.{ArrayList, List => JList}

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.operators.TimestampedCollector
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.util.Logging

/**
  * Process Function used for the aggregate in bounded proc-time OVER window
  * [[org.apache.flink.streaming.api.datastream.DataStream]]
  *
  * @param genAggregations          Generated aggregate helper function
  * @param precedingTimeBoundary    Is used to indicate the processing time boundaries
  * @param aggregatesTypeInfo       row type info of aggregation
  * @param inputType                row type info of input row
  */
class ProcTimeBoundedRangeOver[K](
    genAggregations: GeneratedAggregationsFunction,
    precedingTimeBoundary: Long,
    aggregatesTypeInfo: RowTypeInfo,
    inputType: TypeInformation[CRow],
    minRetentionTime: Long,
    maxRetentionTime: Long)
  extends ProcessFunctionWithCleanupState[K, CRow, CRow](minRetentionTime, maxRetentionTime)
    with Compiler[GeneratedAggregations]
    with Logging {

  private var output: CRow = _
  private var accumulatorState: ValueState[Row] = _
  private var rowMapState: MapState[Long, JList[Row]] = _

  private var function: GeneratedAggregations = _

  override def open(config: Configuration) {
    LOG.debug(s"Compiling AggregateHelper: ${genAggregations.name} \n\n" +
                s"Code:\n${genAggregations.code}")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genAggregations.name,
      genAggregations.code)
    LOG.debug("Instantiating AggregateHelper.")
    function = clazz.newInstance()
    function.open(getRuntimeContext)

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

    initCleanupTimeState("ProcTimeBoundedRangeOverCleanupTime")
  }

  override def processElement(
    input: CRow,
    ctx: KeyedProcessFunction[K, CRow, CRow]#Context,
    out: Collector[CRow]): Unit = {

    val currentTime = ctx.timerService.currentProcessingTime
    // register state-cleanup timer
    processCleanupTimer(ctx, currentTime)

    // buffer the event incoming event

    // add current element to the window list of elements with corresponding timestamp
    var rowList = rowMapState.get(currentTime)
    // null value means that this is the first event received for this timestamp
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
    ctx: KeyedProcessFunction[K, CRow, CRow]#OnTimerContext,
    out: Collector[CRow]): Unit = {

    if (stateCleaningEnabled) {
      val cleanupTime = cleanupTimeState.value()
      if (null != cleanupTime && timestamp == cleanupTime) {
        // clean up and return
        cleanupState(rowMapState, accumulatorState)
        function.cleanup()
        return
      }
    }

    // remove timestamp set outside of ProcessFunction.
    out.asInstanceOf[TimestampedCollector[_]].eraseTimestamp()

    // we consider the original timestamp of events
    // that have registered this time trigger 1 ms ago

    val currentTime = timestamp - 1
    // get the list of elements of current proctime
    val currentElements = rowMapState.get(currentTime)

    // Expired clean-up timers pass the needToCleanupState check.
    // Perform a null check to verify that we have data to process.
    if (null == currentElements) {
      return
    }

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
    val iter = rowMapState.iterator
    while (iter.hasNext) {
      val entry = iter.next()
      val elementKey = entry.getKey
      if (elementKey < limit) {
        // element key outside of window. Retract values
        val elementsRemove = entry.getValue
        var iRemove = 0
        while (iRemove < elementsRemove.size()) {
          val retractRow = elementsRemove.get(iRemove)
          function.retract(accumulators, retractRow)
          iRemove += 1
        }
        iter.remove()
      }
    }

    // add current elements to aggregator. Multiple elements might
    // have arrived in the same proctime
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

  override def close(): Unit = {
    if (function != null) {
      function.close()
    }
  }
}
