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
import java.util.{List => JList}

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.{ListTypeInfo, RowTypeInfo}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}

/**
  * Process Function for ROW clause processing-time bounded OVER window
  *
  * @param genAggregations      Generated aggregate helper function
  * @param precedingOffset      preceding offset
  * @param aggregatesTypeInfo   row type info of aggregation
  * @param inputType            row type info of input row
  */
class ProcTimeBoundedRowsOver[K](
    genAggregations: GeneratedAggregationsFunction,
    precedingOffset: Long,
    aggregatesTypeInfo: RowTypeInfo,
    inputType: TypeInformation[CRow],
    queryConfig: StreamQueryConfig)
  extends ProcessFunctionWithCleanupState[K, CRow, CRow](queryConfig)
    with Compiler[GeneratedAggregations]
    with Logging {

  Preconditions.checkArgument(precedingOffset > 0)

  private var accumulatorState: ValueState[Row] = _
  private var rowMapState: MapState[Long, JList[Row]] = _
  private var output: CRow = _
  private var counterState: ValueState[Long] = _
  private var smallestTsState: ValueState[Long] = _

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
    // We keep the elements received in a Map state keyed
    // by the ingestion time in the operator.
    // we also keep counter of processed elements
    // and timestamp of oldest element
    val rowListTypeInfo: TypeInformation[JList[Row]] =
      new ListTypeInfo[Row](inputType.asInstanceOf[CRowTypeInfo].rowType)
        .asInstanceOf[TypeInformation[JList[Row]]]

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

    initCleanupTimeState("ProcTimeBoundedRowsOverCleanupTime")
  }

  override def processElement(
    inputC: CRow,
    ctx: KeyedProcessFunction[K, CRow, CRow]#Context,
    out: Collector[CRow]): Unit = {

    val input = inputC.row

    val currentTime = ctx.timerService.currentProcessingTime

    // register state-cleanup timer
    processCleanupTimer(ctx, currentTime)

    // initialize state for the processed element
    var accumulators = accumulatorState.value
    if (accumulators == null) {
      accumulators = function.createAccumulators()
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
    function.setForwardedFields(input, output.row)

    // accumulate current row and set aggregate in output row
    function.accumulate(accumulators, input)
    function.setAggregationResults(accumulators, output.row)

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

  override def onTimer(
    timestamp: Long,
    ctx: KeyedProcessFunction[K, CRow, CRow]#OnTimerContext,
    out: Collector[CRow]): Unit = {

    if (stateCleaningEnabled) {
      cleanupState(rowMapState, accumulatorState, counterState, smallestTsState)
      function.cleanup()
    }
  }

  override def close(): Unit = {
    function.close()
  }
}
