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

import java.lang.{Long => JLong}
import java.util.{ArrayList => JArrayList, LinkedList => JLinkedList, List => JList}
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.runtime.state.keyed.{KeyedMapState, KeyedValueState}
import org.apache.flink.table.api.types.{InternalType, RowType, TypeConverters}
import org.apache.flink.table.api.{TableConfig, Types}
import org.apache.flink.table.codegen.GeneratedAggsHandleFunction
import org.apache.flink.table.dataformat.{BaseRow, JoinedRow}
import org.apache.flink.table.runtime.functions.ProcessFunction.{Context, OnTimerContext}
import org.apache.flink.table.runtime.functions.{AggsHandleFunction, ExecutionContext}
import org.apache.flink.table.typeutils.TypeUtils
import org.apache.flink.table.util.{Logging, StateUtil}
import org.apache.flink.util.Collector


/**
  * A ProcessFunction to support unbounded event-time over-window
  *
  * @param genAggsHandler           Generated aggregate helper function
  * @param accTypes                 accumulator types of aggregation
  * @param inputFieldTypes          input field type infos of input row
  */
abstract class RowTimeUnboundedOver(
    genAggsHandler: GeneratedAggsHandleFunction,
    accTypes: Seq[InternalType],
    inputFieldTypes: Seq[InternalType],
    rowTimeIdx: Int,
    tableConfig: TableConfig)
  extends ProcessFunctionWithCleanupState[BaseRow, BaseRow](tableConfig)
  with Logging {

  protected var output: JoinedRow = _
  // state to hold the accumulators of the aggregations
  private var accState: KeyedValueState[BaseRow, BaseRow] = _
  // state to hold rows until the next watermark arrives
  private var inputState: KeyedMapState[BaseRow, JLong, JList[BaseRow]] = _
  // list to sort timestamps to access rows in timestamp order
  private var sortedTimestamps: JLinkedList[JLong] = _

  protected var function: AggsHandleFunction = _

  override def open(ctx: ExecutionContext) {
    super.open(ctx)
    LOG.debug(s"Compiling AggregateHelper: ${genAggsHandler.name}\n" +
        s"Code:\n${genAggsHandler.code}")
    function = genAggsHandler.newInstance(getRuntimeContext.getUserCodeClassLoader)
    function.open(ctx)

    output = new JoinedRow()

    sortedTimestamps = new JLinkedList[JLong]()

    // initialize accumulator state
    val accStateDesc = new ValueStateDescriptor[BaseRow](
      "accState",
      TypeConverters.toBaseRowTypeInfo(new RowType(accTypes: _*)))
    accState = ctx.getKeyedValueState(accStateDesc)

    // input element are all binary row as they are came from network
    val rowListTypeInfo = new ListTypeInfo[BaseRow](
      TypeConverters.toBaseRowTypeInfo(new RowType(inputFieldTypes: _*)))
    val inputStateDesc = new MapStateDescriptor[JLong, JList[BaseRow]](
      "inputState",
      Types.LONG,
      rowListTypeInfo)
    inputState = ctx.getKeyedMapState(inputStateDesc)

    initCleanupTimeState("RowTimeUnboundedOverCleanupTime")
  }

  /**
    * Puts an element from the input stream into state if it is not late.
    * Registers a timer for the next watermark.
    *
    * @param input The input value.
    * @param ctx    The ctx to register timer or get current time
    * @param out    The collector for returning result values.
    *
    */
  override def processElement(
      input: BaseRow,
      ctx: Context,
      out: Collector[BaseRow]): Unit = {

    val currentKey = executionContext.currentKey()
    // register state-cleanup timer
    registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime())

    val timestamp = input.getLong(rowTimeIdx)
    val curWatermark = ctx.timerService().currentWatermark()

    // discard late record
    if (timestamp > curWatermark) {
      // ensure every key just registers one timer
      // default watermark is Long.Min, avoid overflow we use zero when watermark < 0
      val triggerTs = if (curWatermark < 0) 0 else curWatermark + 1
      ctx.timerService.registerEventTimeTimer(triggerTs)

      // put row into state
      var rowList = inputState.get(currentKey, timestamp)
      if (rowList == null) {
        rowList = new JArrayList[BaseRow]()
      }
      rowList.add(input)
      inputState.add(currentKey, timestamp, rowList)
    }
  }

  /**
    * Called when a watermark arrived.
    * Sorts records according the timestamp, computes aggregates, and emits all records with
    * timestamp smaller than the watermark in timestamp order.
    *
    * @param timestamp The timestamp of the firing timer.
    * @param ctx       The ctx to register timer or get current time
    * @param out       The collector for returning result values.
    */
  override def onTimer(
      timestamp: Long,
      ctx: OnTimerContext,
      out: Collector[BaseRow]): Unit = {

    val currentKey = executionContext.currentKey()

    if (isProcessingTimeTimer(ctx.asInstanceOf[OnTimerContext])) {
      if (needToCleanupState(timestamp)) {

        // we check whether there are still records which have not been processed yet
        val noRecordsToProcess = !inputState.contains(currentKey)
        if (noRecordsToProcess) {
          // we clean the state
          cleanupState(inputState, accState)
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

    val keyIterator = inputState.iterator(currentKey)
    if (keyIterator != null && keyIterator.hasNext) {
      val curWatermark = ctx.timerService.currentWatermark
      var existEarlyRecord: Boolean = false

      // sort the record timestamps
      do {
        val recordTime = keyIterator.next.getKey
        // only take timestamps smaller/equal to the watermark
        if (recordTime <= curWatermark) {
          insertToSortedList(recordTime)
        } else {
          existEarlyRecord = true
        }
      } while (keyIterator.hasNext)

      // get last accumulator
      var lastAccumulator = accState.get(currentKey)
      if (lastAccumulator == null) {
        // initialize accumulator
        lastAccumulator = function.createAccumulators()
      }
      // set accumulator in function context first
      function.setAccumulators(lastAccumulator)

      // emit the rows in order
      while (!sortedTimestamps.isEmpty) {
        val curTimestamp = sortedTimestamps.removeFirst()
        val curRowList = inputState.get(currentKey, curTimestamp)
        if (curRowList != null) {
          // process the same timestamp datas, the mechanism is different according ROWS or RANGE
          processElementsWithSameTimestamp(curRowList, out)
        } else {
          // Ignore the same timestamp datas if the state is cleared already.
          LOG.warn(StateUtil.STATE_CLEARED_WARN_MSG)
        }
        inputState.remove(currentKey, curTimestamp)
      }

      // update acc state
      lastAccumulator = function.getAccumulators
      accState.put(currentKey, lastAccumulator)

      // if are are rows with timestamp > watermark, register a timer for the next watermark
      if (existEarlyRecord) {
        ctx.timerService.registerEventTimeTimer(curWatermark + 1)
      }
    }

    // update cleanup timer
    registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime())
  }

  /**
    * Inserts timestamps in order into a linked list.
    *
    * If timestamps arrive in order (as in case of using the RocksDB state backend) this is just
    * an append with O(1).
    */
  private def insertToSortedList(recordTimestamp: Long): Unit = {
    val listIterator = sortedTimestamps.listIterator(sortedTimestamps.size)
    var continue = true
    while (listIterator.hasPrevious && continue) {
      val timestamp = listIterator.previous
      if (recordTimestamp >= timestamp) {
        listIterator.next
        listIterator.add(recordTimestamp)
        continue = false
      }
    }

    if (continue) {
      sortedTimestamps.addFirst(recordTimestamp)
    }
  }

  /**
    * Process the same timestamp datas, the mechanism is different between
    * rows and range window.
    */
  def processElementsWithSameTimestamp(
      curRowList: JList[BaseRow],
      out: Collector[BaseRow]): Unit

  override def close(): Unit = {
    function.close()
  }
}

/**
  * A ProcessFunction to support unbounded ROWS window.
  * The ROWS clause defines on a physical level how many rows are included in a window frame.
  */
class RowTimeUnboundedRowsOver(
    genAggsHandler: GeneratedAggsHandleFunction,
    accTypes: Seq[InternalType],
    inputFieldTypes: Seq[InternalType],
    rowTimeIdx: Int,
    tableConfig: TableConfig)
  extends RowTimeUnboundedOver(
    genAggsHandler,
    accTypes,
    inputFieldTypes,
    rowTimeIdx,
    tableConfig) {

  override def processElementsWithSameTimestamp(
      curRowList: JList[BaseRow],
      out: Collector[BaseRow]): Unit = {

    var i = 0
    while (i < curRowList.size) {
      val curRow = curRowList.get(i)
      // accumulate current row
      function.accumulate(curRow)
      // prepare output row
      output.replace(curRow, function.getValue)
      // emit output row
      out.collect(output)
      i += 1
    }
  }
}


/**
  * A ProcessFunction to support unbounded RANGE window.
  * The RANGE option includes all the rows within the window frame
  * that have the same ORDER BY values as the current row.
  */
class RowTimeUnboundedRangeOver(
    genAggsHandler: GeneratedAggsHandleFunction,
    accTypes: Seq[InternalType],
    inputFieldTypes: Seq[InternalType],
    rowTimeIdx: Int,
    tableConfig: TableConfig)
  extends RowTimeUnboundedOver(
    genAggsHandler,
    accTypes,
    inputFieldTypes,
    rowTimeIdx,
    tableConfig) {

  override def processElementsWithSameTimestamp(
      curRowList: JList[BaseRow],
      out: Collector[BaseRow]): Unit = {

    var i = 0
    // all same timestamp data should have same aggregation value.
    while (i < curRowList.size) {
      val curRow = curRowList.get(i)
      function.accumulate(curRow)
      i += 1
    }

    // emit output row
    i = 0
    val aggValue = function.getValue
    while (i < curRowList.size) {
      val curRow = curRowList.get(i)
      // prepare output row
      output.replace(curRow, aggValue)
      out.collect(output)
      i += 1
    }
  }
}
