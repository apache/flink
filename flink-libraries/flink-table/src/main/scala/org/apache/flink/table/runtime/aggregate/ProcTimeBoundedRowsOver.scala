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
  * Process Function for ROW clause processing-time bounded OVER window
  *
  * @param genAggsHandler     Generated aggregate helper function
  * @param accTypes           accumulator types of aggregation
  * @param inputFieldTypes    input field type infos of input row
  * @param precedingOffset    preceding offset
  */
class ProcTimeBoundedRowsOver(
    genAggsHandler: GeneratedAggsHandleFunction,
    accTypes: Seq[InternalType],
    inputFieldTypes: Seq[InternalType],
    precedingOffset: Long,
    tableConfig: TableConfig)
  extends ProcessFunctionWithCleanupState[BaseRow, BaseRow](tableConfig)
  with Logging {

  Preconditions.checkArgument(precedingOffset > 0)

  private var function: AggsHandleFunction = _

  private var accState: KeyedValueState[BaseRow, BaseRow] = _
  private var inputState: KeyedMapState[BaseRow, JLong, JList[BaseRow]] = _
  private var counterState: KeyedValueState[BaseRow, JLong] = _
  private var smallestTsState: KeyedValueState[BaseRow, JLong] = _

  private var output: JoinedRow = _

  override def open(ctx: ExecutionContext) {
    super.open(ctx)
    LOG.debug(s"Compiling AggregateHelper: ${genAggsHandler.name} \n\n" +
      s"Code:\n${genAggsHandler.code}")
    function = genAggsHandler.newInstance(ctx.getRuntimeContext.getUserCodeClassLoader)
    function.open(ctx)

    output = new JoinedRow()

    // input element are all binary row as they are came from network
    val inputType = new BaseRowTypeInfo(
      inputFieldTypes.map(TypeConverters.createExternalTypeInfoFromDataType): _*)
      .asInstanceOf[BaseRowTypeInfo]
    // We keep the elements received in a Map state keyed
    // by the ingestion time in the operator.
    // we also keep counter of processed elements
    // and timestamp of oldest element
    val rowListTypeInfo = new ListTypeInfo[BaseRow](inputType)

    val mapStateDescriptor: MapStateDescriptor[JLong, JList[BaseRow]] =
      new MapStateDescriptor[JLong, JList[BaseRow]](
        "inputState",
        Types.LONG,
        rowListTypeInfo)
    inputState = ctx.getKeyedMapState(mapStateDescriptor)

    val accTypeInfo = new BaseRowTypeInfo(
      accTypes.map(TypeConverters.createExternalTypeInfoFromDataType): _*)
    val accStateDesc = new ValueStateDescriptor[BaseRow]("accState", accTypeInfo)
    accState = ctx.getKeyedValueState(accStateDesc)

    val processedCountDescriptor = new ValueStateDescriptor[JLong](
      "processedCountState",
      Types.LONG)
    counterState = ctx.getKeyedValueState(processedCountDescriptor)

    val smallestTimestampDescriptor = new ValueStateDescriptor[JLong](
      "smallestTSState",
      Types.LONG)
    smallestTsState = ctx.getKeyedValueState(smallestTimestampDescriptor)

    initCleanupTimeState("ProcTimeBoundedRowsOverCleanupTime")
  }

  override def processElement(
    input: BaseRow,
    ctx: Context,
    out: Collector[BaseRow]): Unit = {

    val currentKey = executionContext.currentKey()
    val currentTime = ctx.timerService.currentProcessingTime

    // register state-cleanup timer
    registerProcessingCleanupTimer(ctx, currentTime)

    // initialize state for the processed element
    var accumulators = accState.get(currentKey)
    if (accumulators == null) {
      accumulators = function.createAccumulators()
    }
    // set accumulators in context first
    function.setAccumulators(accumulators)

    // get smallest timestamp
    var smallestTs = smallestTsState.get(currentKey)
    if (smallestTs == null) {
      smallestTs = currentTime
      smallestTsState.put(currentKey, smallestTs)
    }
    // get previous counter value
    var counter = counterState.get(currentKey)
    if (counter == null) {
      counter = 0L
    }

    if (counter == precedingOffset) {
      val retractList = inputState.get(currentKey, smallestTs)
      if (retractList != null) {
        // get oldest element beyond buffer size
        // and if oldest element exist, retract value
        val retractRow = retractList.get(0)
        function.retract(retractRow)
        retractList.remove(0)
      } else {
        // Does not retract values which are outside of window if the state is cleared already.
        LOG.warn(StateUtil.STATE_CLEARED_WARN_MSG)
      }
      // if reference timestamp list not empty, keep the list
      if (retractList != null && !retractList.isEmpty) {
        inputState.add(currentKey, smallestTs, retractList)
      } // if smallest timestamp list is empty, remove and find new smallest
      else {
        inputState.remove(currentKey, smallestTs)
        val iter = inputState.iterator(currentKey)
        var currentTs: Long = 0L
        var newSmallestTs: Long = Long.MaxValue
        while (iter.hasNext) {
          currentTs = iter.next.getKey
          if (currentTs < newSmallestTs) {
            newSmallestTs = currentTs
          }
        }
        smallestTsState.put(currentKey, newSmallestTs)
      }
    } // we update the counter only while buffer is getting filled
    else {
      counter += 1
      counterState.put(currentKey, counter)
    }

    // update map state, counter and timestamp
    val currentTimeState = inputState.get(currentKey, currentTime)
    if (currentTimeState != null) {
      currentTimeState.add(input)
      inputState.add(currentKey, currentTime, currentTimeState)
    } else { // add new input
      val newList = new JArrayList[BaseRow]
      newList.add(input)
      inputState.add(currentKey, currentTime, newList)
    }

    // accumulate current row
    function.accumulate(input)
    // update the value of accumulators for future incremental computation
    accumulators = function.getAccumulators
    accState.put(currentKey, accumulators)

    // prepare output row
    val aggValue = function.getValue
    output.replace(input, aggValue)
    out.collect(output)
  }

  override def onTimer(
    timestamp: Long,
    ctx: OnTimerContext,
    out: Collector[BaseRow]): Unit = {

    if (needToCleanupState(timestamp)) {
      cleanupState(inputState, accState, counterState, smallestTsState)
      function.cleanup()
    }
  }

  override def close(): Unit = {
    function.close()
  }
}
