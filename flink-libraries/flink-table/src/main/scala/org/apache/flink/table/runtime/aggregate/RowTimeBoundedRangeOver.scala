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
import java.lang.{Long => JLong}
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.runtime.state.keyed.{KeyedMapState, KeyedValueState}
import org.apache.flink.table.api.types.{DataTypes, InternalType, TypeConverters}
import org.apache.flink.table.api.{TableConfig, Types}
import org.apache.flink.table.codegen.GeneratedAggsHandleFunction
import org.apache.flink.table.dataformat.{BaseRow, JoinedRow}
import org.apache.flink.table.runtime.functions.ProcessFunction.{Context, OnTimerContext}
import org.apache.flink.table.runtime.functions.{AggsHandleFunction, ExecutionContext}
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, TypeUtils}
import org.apache.flink.table.util.{Logging, StateUtil}
import org.apache.flink.util.{Collector, Preconditions}

/**
  * Process Function for RANGE clause event-time bounded OVER window
  *
  * @param genAggsHandler           Generated aggregate helper function
  * @param accTypes                 accumulator types of aggregation
  * @param inputFieldTypes          input field type infos of input row
  * @param precedingOffset          preceding offset
  */
class RowTimeBoundedRangeOver(
    genAggsHandler: GeneratedAggsHandleFunction,
    accTypes: Seq[InternalType],
    inputFieldTypes: Seq[InternalType],
    precedingOffset: Long,
    rowTimeIdx: Int,
    tableConfig: TableConfig)
  extends ProcessFunctionWithCleanupState[BaseRow, BaseRow](tableConfig)
  with Logging {

  Preconditions.checkNotNull(precedingOffset)

  private var output: JoinedRow = _

  // the state which keeps the last triggering timestamp
  private var lastTriggeringTsState: KeyedValueState[BaseRow, JLong] = _

  // the state which used to materialize the accumulator for incremental calculation
  private var accState: KeyedValueState[BaseRow, BaseRow] = _

  // the state which keeps all the data that are not expired.
  // The first element (as the mapState key) of the tuple is the time stamp. Per each time stamp,
  // the second element of tuple is a list that contains the entire data of all the rows belonging
  // to this time stamp.
  private var inputState: KeyedMapState[BaseRow, JLong, JList[BaseRow]] = _

  private var function: AggsHandleFunction = _

  override def open(ctx: ExecutionContext) {
    super.open(ctx)
    LOG.debug(s"Compiling AggregateHelper: ${genAggsHandler.name} \n\n" +
        s"Code:\n${genAggsHandler.code}")
    function = genAggsHandler.newInstance(ctx.getRuntimeContext.getUserCodeClassLoader)
    function.open(ctx)

    output = new JoinedRow()

    val lastTriggeringTsDescriptor = new ValueStateDescriptor[JLong](
      "lastTriggeringTsState",
      Types.LONG)
    lastTriggeringTsState = ctx.getKeyedValueState(lastTriggeringTsDescriptor)

    val accTypeInfo = new BaseRowTypeInfo(
      accTypes.map(TypeConverters.createExternalTypeInfoFromDataType): _*)
    val accStateDesc = new ValueStateDescriptor[BaseRow]("accState", accTypeInfo)
    accState = ctx.getKeyedValueState(accStateDesc)

    // input element are all binary row as they are came from network
    val inputType = new BaseRowTypeInfo(
      inputFieldTypes.map(TypeConverters.createExternalTypeInfoFromDataType): _*)
      .asInstanceOf[BaseRowTypeInfo]
    val rowListTypeInfo = new ListTypeInfo[BaseRow](inputType)
    val inputStateDesc = new MapStateDescriptor[JLong, JList[BaseRow]](
      "inputState",
      Types.LONG,
      rowListTypeInfo)
    inputState = ctx.getKeyedMapState(inputStateDesc)

    initCleanupTimeState("RowTimeBoundedRangeOverCleanupTime")
  }

  override def processElement(
      input: BaseRow,
      ctx: Context,
      out: Collector[BaseRow]): Unit = {

    val currentKey = executionContext.currentKey()
    // register state-cleanup timer
    registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime())

    // triggering timestamp for trigger calculation
    val triggeringTs = input.getLong(rowTimeIdx)

    var lastTriggeringTs = lastTriggeringTsState.get(currentKey)
    if (lastTriggeringTs == null) {
      lastTriggeringTs = 0L
    }

    // check if the data is expired, if not, save the data and register event time timer
    if (triggeringTs > lastTriggeringTs) {
      val data = inputState.get(currentKey, triggeringTs)
      if (null != data) {
        data.add(input)
        inputState.add(currentKey, triggeringTs, data)
      } else {
        val data = new JArrayList[BaseRow]
        data.add(input)
        inputState.add(currentKey, triggeringTs, data)
        // register event time timer
        ctx.timerService.registerEventTimeTimer(triggeringTs)
      }
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: OnTimerContext,
      out: Collector[BaseRow]): Unit = {

    val currentKey = executionContext.currentKey()

    if (isProcessingTimeTimer(ctx.asInstanceOf[OnTimerContext])) {
      if (needToCleanupState(timestamp)) {

        val keysIt = inputState.iterator(currentKey)
        var lastProcessedTime = lastTriggeringTsState.get(currentKey)
        if (lastProcessedTime == null) {
          lastProcessedTime = 0L
        }

        // is data left which has not been processed yet?
        var noRecordsToProcess = true
        while (keysIt.hasNext && noRecordsToProcess) {
          if (keysIt.next().getKey > lastProcessedTime) {
            noRecordsToProcess = false
          }
        }

        if (noRecordsToProcess) {
          // we clean the state
          cleanupState(inputState, accState, lastTriggeringTsState)
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

    // gets all window data from state for the calculation
    val inputs: JList[BaseRow] = inputState.get(currentKey, timestamp)

    if (null != inputs) {

      var dataListIndex = 0
      var accumulators = accState.get(currentKey)

      // initialize when first run or failover recovery per key
      if (null == accumulators) {
        accumulators = function.createAccumulators()
      }
      // set accumulators in context first
      function.setAccumulators(accumulators)

      // keep up timestamps of retract data
      val retractTsList: JList[JLong] = new JArrayList[JLong]

      // do retraction
      val dataTimestampIt = inputState.iterator(currentKey)
      while (dataTimestampIt.hasNext) {
        val dataTs = dataTimestampIt.next().getKey
        val offset = timestamp - dataTs
        if (offset > precedingOffset) {
          val retractDataList = inputState.get(currentKey, dataTs)
          if (retractDataList != null) {
            dataListIndex = 0
            while (dataListIndex < retractDataList.size()) {
              val retractRow = retractDataList.get(dataListIndex)
              function.retract(retractRow)
              dataListIndex += 1
            }
            retractTsList.add(dataTs)
          } else {
            // Does not retract values which are outside of window if the state is cleared already.
            LOG.warn(StateUtil.STATE_CLEARED_WARN_MSG)
          }
        }
      }

      // do accumulation
      dataListIndex = 0
      while (dataListIndex < inputs.size()) {
        val curRow = inputs.get(dataListIndex)
        // accumulate current row
        function.accumulate(curRow)
        dataListIndex += 1
      }

      // get aggregate result
      val aggValue = function.getValue

      // copy forwarded fields to output row and emit output row
      dataListIndex = 0
      while (dataListIndex < inputs.size()) {
        val curRow = inputs.get(dataListIndex)
        output.replace(curRow, aggValue)
        out.collect(output)
        dataListIndex += 1
      }

      // remove the data that has been retracted
      dataListIndex = 0
      while (dataListIndex < retractTsList.size) {
        inputState.remove(currentKey, retractTsList.get(dataListIndex))
        dataListIndex += 1
      }

      // update the value of accumulators for future incremental computation
      accumulators = function.getAccumulators
      accState.put(currentKey, accumulators)
    }
    lastTriggeringTsState.put(currentKey, timestamp)

    // update cleanup timer
    registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime())
  }

  override def close(): Unit = {
    function.close()
  }
}


