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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.runtime.state.keyed.{KeyedMapState, KeyedValueState}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.types.{DataTypes, InternalType, TypeConverters}
import org.apache.flink.table.codegen.GeneratedAggsHandleFunction
import org.apache.flink.table.dataformat.{BaseRow, JoinedRow}
import org.apache.flink.table.runtime.functions.ProcessFunction.{Context, OnTimerContext}
import org.apache.flink.table.runtime.functions.{AggsHandleFunction, ExecutionContext}
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, TypeUtils}
import org.apache.flink.table.util.{Logging, StateUtil}
import org.apache.flink.util.Collector

/**
  * Process Function used for the aggregate in bounded proc-time OVER window
  *
  * @param genAggsHandler           Generated aggregate helper function
  * @param accTypes                 accumulator types of aggregation
  * @param inputFieldTypes          input field type infos of input row
  * @param precedingTimeBoundary    Is used to indicate the processing time boundaries
  */
class ProcTimeBoundedRangeOver(
    genAggsHandler: GeneratedAggsHandleFunction,
    accTypes: Seq[InternalType],
    inputFieldTypes: Seq[InternalType],
    precedingTimeBoundary: Long,
    tableConfig: TableConfig)
  extends ProcessFunctionWithCleanupState[BaseRow, BaseRow](tableConfig)
  with Logging {

  private var accState: KeyedValueState[BaseRow, BaseRow] = _
  private var inputState: KeyedMapState[BaseRow, JLong, JList[BaseRow]] = _

  private var function: AggsHandleFunction = _

  private var output: JoinedRow = _

  override def open(ctx: ExecutionContext): Unit = {
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
    // we keep the elements received in a map state indexed based on their ingestion time
    val rowListTypeInfo = new ListTypeInfo[BaseRow](inputType)
    val mapStateDescriptor: MapStateDescriptor[JLong, JList[BaseRow]] =
      new MapStateDescriptor[JLong, JList[BaseRow]](
        "inputState",
        BasicTypeInfo.LONG_TYPE_INFO,
        rowListTypeInfo)
    inputState = ctx.getKeyedMapState(mapStateDescriptor)

    val accTypeInfo = new BaseRowTypeInfo(
      accTypes.map(TypeConverters.createExternalTypeInfoFromDataType): _*)
    val stateDescriptor: ValueStateDescriptor[BaseRow] =
      new ValueStateDescriptor[BaseRow]("accState", accTypeInfo)
    accState = ctx.getKeyedValueState(stateDescriptor)

    initCleanupTimeState("ProcTimeBoundedRangeOverCleanupTime")
  }

  override def processElement(
      input: BaseRow,
      ctx: Context,
      out: Collector[BaseRow]): Unit = {

    val currentKey = executionContext.currentKey()
    val currentTime = ctx.timerService.currentProcessingTime
    // register state-cleanup timer
    registerProcessingCleanupTimer(ctx, currentTime)

    // buffer the event incoming event

    // add current element to the window list of elements with corresponding timestamp
    var rowList = inputState.get(currentKey, currentTime)
    // this is the first event received for this timestamp
    if (rowList == null) {
      rowList = new JArrayList[BaseRow]()
      // register timer to process event once the current millisecond passed
      ctx.timerService.registerProcessingTimeTimer(currentTime + 1)
    }
    // add current input into state
    rowList.add(input)
    inputState.add(currentKey, currentTime, rowList)
  }

  override def onTimer(
      timestamp: Long,
      ctx: OnTimerContext,
      out: Collector[BaseRow]): Unit = {

    val currentKey = executionContext.currentKey()
    if (needToCleanupState(timestamp)) {
      // clean up and return
      cleanupState(inputState, accState)
      function.cleanup()
      return
    }

    // we consider the original timestamp of events
    // that have registered this time trigger 1 ms ago

    val currentTime = timestamp - 1
    var i = 0

    // get the list of elements of current proctime
    val currentElements = inputState.get(currentKey, currentTime)

    // Expired clean-up timers pass the needToCleanupState() check.
    // Perform a null check to verify that we have data to process.
    if (null == currentElements) {
      return
    }

    // initialize the accumulators
    var accumulators = accState.get(currentKey)

    if (null == accumulators) {
      accumulators = function.createAccumulators()
    }
    // set accumulators in context first
    function.setAccumulators(accumulators)

    // update the elements to be removed and retract them from aggregators
    val limit = currentTime - precedingTimeBoundary

    // we iterate through all elements in the window buffer based on timestamp keys
    // when we find timestamps that are out of interest, we retrieve corresponding elements
    // and eliminate them. Multiple elements could have been received at the same timestamp
    // the removal of old elements happens only once per proctime as onTimer is called only once
    val iter = inputState.iterator(currentKey)
    val markToRemove = new JArrayList[JLong]()
    while (iter.hasNext) {
      val elementKey = iter.next.getKey
      if (elementKey < limit) {
        // element key outside of window. Retract values
        val elementsRemove = inputState.get(currentKey, elementKey)
        if (elementsRemove != null) {
          var iRemove = 0
          while (iRemove < elementsRemove.size()) {
            val retractRow = elementsRemove.get(iRemove)
            function.retract(retractRow)
            iRemove += 1
          }
        } else {
          // Does not retract values which are outside of window if the state is cleared already.
          LOG.warn(StateUtil.STATE_CLEARED_WARN_MSG)
        }
        // mark element for later removal not to modify the iterator over MapState
        markToRemove.add(elementKey)
      }
    }
    // need to remove in 2 steps not to have concurrent access errors via iterator to the MapState
    i = 0
    while (i < markToRemove.size()) {
      inputState.remove(currentKey, markToRemove.get(i))
      i += 1
    }

    // add current elements to aggregator. Multiple elements might
    // have arrived in the same proctime
    // the same accumulator value will be computed for all elements
    var iElemenets = 0
    while (iElemenets < currentElements.size()) {
      val input = currentElements.get(iElemenets)
      function.accumulate(input)
      iElemenets += 1
    }

    // we need to build the output and emit for every event received at this proctime
    iElemenets = 0
    val aggValue = function.getValue
    while (iElemenets < currentElements.size()) {
      val input = currentElements.get(iElemenets)
      output.replace(input, aggValue)
      out.collect(output)
      iElemenets += 1
    }

    // update the value of accumulators for future incremental computation
    accumulators = function.getAccumulators
    accState.put(currentKey, accumulators)
  }

  override def close(): Unit = {
    function.close()
  }
}
