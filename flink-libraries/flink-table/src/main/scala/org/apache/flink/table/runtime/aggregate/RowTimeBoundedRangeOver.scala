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

import java.util.{ArrayList => JArrayList, List => JList}

import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.{ListTypeInfo, RowTypeInfo}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.operators.TimestampedCollector
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}

/**
 * Process Function for RANGE clause event-time bounded OVER window
 *
  * @param genAggregations Generated aggregate helper function
  * @param aggregationStateType     row type info of aggregation
  * @param inputRowType             row type info of input row
  * @param precedingOffset          preceding offset
 */
class RowTimeBoundedRangeOver(
    genAggregations: GeneratedAggregationsFunction,
    aggregationStateType: RowTypeInfo,
    inputRowType: CRowTypeInfo,
    precedingOffset: Long,
    rowTimeIdx: Int,
    queryConfig: StreamQueryConfig)
  extends ProcessFunctionWithCleanupState[CRow, CRow](queryConfig)
    with Compiler[GeneratedAggregations]
    with Logging {
  Preconditions.checkNotNull(aggregationStateType)
  Preconditions.checkNotNull(precedingOffset)

  private var output: CRow = _

  // the state which keeps the last triggering timestamp
  private var lastTriggeringTsState: ValueState[Long] = _

  // the state which used to materialize the accumulator for incremental calculation
  private var accumulatorState: ValueState[Row] = _

  // the state which keeps all the data that are not expired.
  // The first element (as the mapState key) of the tuple is the time stamp. Per each time stamp,
  // the second element of tuple is a list that contains the entire data of all the rows belonging
  // to this time stamp.
  private var dataState: MapState[Long, JList[Row]] = _

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

    val lastTriggeringTsDescriptor: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("lastTriggeringTsState", classOf[Long])
    lastTriggeringTsState = getRuntimeContext.getState(lastTriggeringTsDescriptor)

    val accumulatorStateDescriptor =
      new ValueStateDescriptor[Row]("accumulatorState", aggregationStateType)
    accumulatorState = getRuntimeContext.getState(accumulatorStateDescriptor)

    val keyTypeInformation: TypeInformation[Long] =
      BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]]
    val valueTypeInformation: TypeInformation[JList[Row]] =
      new ListTypeInfo[Row](inputRowType.asInstanceOf[CRowTypeInfo].rowType)

    val mapStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]](
        "dataState",
        keyTypeInformation,
        valueTypeInformation)

    dataState = getRuntimeContext.getMapState(mapStateDescriptor)

    initCleanupTimeState("RowTimeBoundedRangeOverCleanupTime")
  }

  override def processElement(
    inputC: CRow,
    ctx: ProcessFunction[CRow, CRow]#Context,
    out: Collector[CRow]): Unit = {

    val input = inputC.row

    // register state-cleanup timer
    registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime())

    // triggering timestamp for trigger calculation
    val triggeringTs = input.getField(rowTimeIdx).asInstanceOf[Long]

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

    if (isProcessingTimeTimer(ctx.asInstanceOf[OnTimerContext])) {
      if (needToCleanupState(timestamp)) {

        val keysIt = dataState.keys.iterator()
        val lastProcessedTime = lastTriggeringTsState.value

        // is data left which has not been processed yet?
        var noRecordsToProcess = true
        while (keysIt.hasNext && noRecordsToProcess) {
          if (keysIt.next() > lastProcessedTime) {
            noRecordsToProcess = false
          }
        }

        if (noRecordsToProcess) {
          // we clean the state
          cleanupState(dataState, accumulatorState, lastTriggeringTsState)
          function.cleanup()
        } else {
          // There are records left to process because a watermark has not been received yet.
          // This would only happen if the input stream has stopped. So we don't need to clean up.
          // We leave the state as it is and schedule a new cleanup timer
          registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime())
        }
      }
      return
    }

    // remove timestamp set outside of ProcessFunction.
    out.asInstanceOf[TimestampedCollector[_]].eraseTimestamp()

    // gets all window data from state for the calculation
    val inputs: JList[Row] = dataState.get(timestamp)

    if (null != inputs) {

      var accumulators = accumulatorState.value
      var dataListIndex = 0
      var aggregatesIndex = 0

      // initialize when first run or failover recovery per key
      if (null == accumulators) {
        accumulators = function.createAccumulators()
        aggregatesIndex = 0
      }

      // keep up timestamps of retract data
      val retractTsList: JList[Long] = new JArrayList[Long]

      // do retraction
      val iter = dataState.iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val dataTs: Long = entry.getKey
        val offset = timestamp - dataTs
        if (offset > precedingOffset) {
          val retractDataList = entry.getValue
          dataListIndex = 0
          while (dataListIndex < retractDataList.size()) {
            val retractRow = retractDataList.get(dataListIndex)
            function.retract(accumulators, retractRow)
            dataListIndex += 1
          }
          retractTsList.add(dataTs)
        }
      }

      // do accumulation
      dataListIndex = 0
      while (dataListIndex < inputs.size()) {
        val curRow = inputs.get(dataListIndex)
        // accumulate current row
        function.accumulate(accumulators, curRow)
        dataListIndex += 1
      }

      // set aggregate in output row
      function.setAggregationResults(accumulators, output.row)

      // copy forwarded fields to output row and emit output row
      dataListIndex = 0
      while (dataListIndex < inputs.size()) {
        aggregatesIndex = 0
        function.setForwardedFields(inputs.get(dataListIndex), output.row)
        out.collect(output)
        dataListIndex += 1
      }

      // remove the data that has been retracted
      dataListIndex = 0
      while (dataListIndex < retractTsList.size) {
        dataState.remove(retractTsList.get(dataListIndex))
        dataListIndex += 1
      }

      // update state
      accumulatorState.update(accumulators)
    }
    lastTriggeringTsState.update(timestamp)

    // update cleanup timer
    registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime())
  }

  override def close(): Unit = {
    function.close()
  }
}


