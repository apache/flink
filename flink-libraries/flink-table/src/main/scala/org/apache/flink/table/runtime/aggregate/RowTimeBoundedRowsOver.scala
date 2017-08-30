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
 * Process Function for ROWS clause event-time bounded OVER window
 *
  * @param genAggregations Generated aggregate helper function
  * @param aggregationStateType     row type info of aggregation
  * @param inputRowType             row type info of input row
  * @param precedingOffset          preceding offset
 */
class RowTimeBoundedRowsOver(
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

  // the state which keeps the count of data
  private var dataCountState: ValueState[Long] = _

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

    val dataCountStateDescriptor =
      new ValueStateDescriptor[Long]("dataCountState", classOf[Long])
    dataCountState = getRuntimeContext.getState(dataCountStateDescriptor)

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

    initCleanupTimeState("RowTimeBoundedRowsOverCleanupTime")
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
        val data = new util.ArrayList[Row]
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
          // We clean the state
          cleanupState(dataState, accumulatorState, dataCountState, lastTriggeringTsState)
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
      var dataCount = dataCountState.value

      var retractList: JList[Row] = null
      var retractTs: Long = Long.MaxValue
      var retractCnt: Int = 0
      var i = 0

      while (i < inputs.size) {
        val input = inputs.get(i)

        // initialize when first run or failover recovery per key
        if (null == accumulators) {
          accumulators = function.createAccumulators()
        }

        var retractRow: Row = null

        if (dataCount >= precedingOffset) {
          if (null == retractList) {
            // find the smallest timestamp
            retractTs = Long.MaxValue
            val dataTimestampIt = dataState.keys.iterator
            while (dataTimestampIt.hasNext) {
              val dataTs = dataTimestampIt.next
              if (dataTs < retractTs) {
                retractTs = dataTs
              }
            }
            // get the oldest rows to retract them
            retractList = dataState.get(retractTs)
          }

          retractRow = retractList.get(retractCnt)
          retractCnt += 1

          // remove retracted values from state
          if (retractList.size == retractCnt) {
            dataState.remove(retractTs)
            retractList = null
            retractCnt = 0
          }
        } else {
          dataCount += 1
        }

        // copy forwarded fields to output row
        function.setForwardedFields(input, output.row)

        // retract old row from accumulators
        if (null != retractRow) {
          function.retract(accumulators, retractRow)
        }

        // accumulate current row and set aggregate in output row
        function.accumulate(accumulators, input)
        function.setAggregationResults(accumulators, output.row)
        i += 1

        out.collect(output)
      }

      // update all states
      if (dataState.contains(retractTs)) {
        if (retractCnt > 0) {
          retractList.subList(0, retractCnt).clear()
          dataState.put(retractTs, retractList)
        }
      }
      dataCountState.update(dataCount)
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


