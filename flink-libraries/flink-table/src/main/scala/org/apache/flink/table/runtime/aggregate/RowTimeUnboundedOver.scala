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
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.operators.TimestampedCollector
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector


/**
  * A ProcessFunction to support unbounded event-time over-window
  *
  * @param genAggregations Generated aggregate helper function
  * @param intermediateType         the intermediate row tye which the state saved
  * @param inputType                the input row tye which the state saved
  */
abstract class RowTimeUnboundedOver(
    genAggregations: GeneratedAggregationsFunction,
    intermediateType: TypeInformation[Row],
    inputType: TypeInformation[CRow],
    rowTimeIdx: Int,
    queryConfig: StreamQueryConfig)
  extends ProcessFunctionWithCleanupState[CRow, CRow](queryConfig)
    with Compiler[GeneratedAggregations]
    with Logging {

  protected var output: CRow = _
  // state to hold the accumulators of the aggregations
  private var accumulatorState: ValueState[Row] = _
  // state to hold rows until the next watermark arrives
  private var rowMapState: MapState[Long, JList[Row]] = _
  // list to sort timestamps to access rows in timestamp order
  private var sortedTimestamps: util.LinkedList[Long] = _

  protected var function: GeneratedAggregations = _

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
    sortedTimestamps = new util.LinkedList[Long]()

    // initialize accumulator state
    val accDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("accumulatorstate", intermediateType)
    accumulatorState = getRuntimeContext.getState[Row](accDescriptor)

    // initialize row state
    val rowListTypeInfo: TypeInformation[JList[Row]] =
      new ListTypeInfo[Row](inputType.asInstanceOf[CRowTypeInfo].rowType)
    val mapStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]]("rowmapstate",
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]], rowListTypeInfo)
    rowMapState = getRuntimeContext.getMapState(mapStateDescriptor)

    initCleanupTimeState("RowTimeUnboundedOverCleanupTime")
  }

  /**
    * Puts an element from the input stream into state if it is not late.
    * Registers a timer for the next watermark.
    *
    * @param inputC The input value.
    * @param ctx   The ctx to register timer or get current time
    * @param out   The collector for returning result values.
    *
    */
  override def processElement(
     inputC: CRow,
     ctx:  ProcessFunction[CRow, CRow]#Context,
     out: Collector[CRow]): Unit = {

    val input = inputC.row

    // register state-cleanup timer
    registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime())

    val timestamp = input.getField(rowTimeIdx).asInstanceOf[Long]
    val curWatermark = ctx.timerService().currentWatermark()

    // discard late record
    if (timestamp > curWatermark) {
      // ensure every key just registers one timer
      ctx.timerService.registerEventTimeTimer(curWatermark + 1)

      // put row into state
      var rowList = rowMapState.get(timestamp)
      if (rowList == null) {
        rowList = new util.ArrayList[Row]()
      }
      rowList.add(input)
      rowMapState.put(timestamp, rowList)
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
      ctx: ProcessFunction[CRow, CRow]#OnTimerContext,
      out: Collector[CRow]): Unit = {

    if (isProcessingTimeTimer(ctx.asInstanceOf[OnTimerContext])) {
      if (needToCleanupState(timestamp)) {

        // we check whether there are still records which have not been processed yet
        val noRecordsToProcess = !rowMapState.keys.iterator().hasNext
        if (noRecordsToProcess) {
          // we clean the state
          cleanupState(rowMapState, accumulatorState)
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

    val keyIterator = rowMapState.keys.iterator
    if (keyIterator.hasNext) {
      val curWatermark = ctx.timerService.currentWatermark
      var existEarlyRecord: Boolean = false

      // sort the record timestamps
      do {
        val recordTime = keyIterator.next
        // only take timestamps smaller/equal to the watermark
        if (recordTime <= curWatermark) {
          insertToSortedList(recordTime)
        } else {
          existEarlyRecord = true
        }
      } while (keyIterator.hasNext)

      // get last accumulator
      var lastAccumulator = accumulatorState.value
      if (lastAccumulator == null) {
        // initialize accumulator
        lastAccumulator = function.createAccumulators()
      }

      // emit the rows in order
      while (!sortedTimestamps.isEmpty) {
        val curTimestamp = sortedTimestamps.removeFirst()
        val curRowList = rowMapState.get(curTimestamp)

        // process the same timestamp data, the mechanism is different according ROWS or RANGE
        processElementsWithSameTimestamp(curRowList, lastAccumulator, out)

        rowMapState.remove(curTimestamp)
      }

      accumulatorState.update(lastAccumulator)

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
  private def insertToSortedList(recordTimestamp: Long) = {
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
   * Process the same timestamp data, the mechanism is different between
   * rows and range window.
   */
  def processElementsWithSameTimestamp(
    curRowList: JList[Row],
    lastAccumulator: Row,
    out: Collector[CRow]): Unit

  override def close(): Unit = {
    function.close()
  }
}

/**
  * A ProcessFunction to support unbounded ROWS window.
  * The ROWS clause defines on a physical level how many rows are included in a window frame.
  */
class RowTimeUnboundedRowsOver(
    genAggregations: GeneratedAggregationsFunction,
    intermediateType: TypeInformation[Row],
    inputType: TypeInformation[CRow],
    rowTimeIdx: Int,
    queryConfig: StreamQueryConfig)
  extends RowTimeUnboundedOver(
    genAggregations: GeneratedAggregationsFunction,
    intermediateType,
    inputType,
    rowTimeIdx,
    queryConfig) {

  override def processElementsWithSameTimestamp(
    curRowList: JList[Row],
    lastAccumulator: Row,
    out: Collector[CRow]): Unit = {

    var i = 0
    while (i < curRowList.size) {
      val curRow = curRowList.get(i)

      // copy forwarded fields to output row
      function.setForwardedFields(curRow, output.row)

      // update accumulators and copy aggregates to output row
      function.accumulate(lastAccumulator, curRow)
      function.setAggregationResults(lastAccumulator, output.row)
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
    genAggregations: GeneratedAggregationsFunction,
    intermediateType: TypeInformation[Row],
    inputType: TypeInformation[CRow],
    rowTimeIdx: Int,
    queryConfig: StreamQueryConfig)
  extends RowTimeUnboundedOver(
    genAggregations: GeneratedAggregationsFunction,
    intermediateType,
    inputType,
    rowTimeIdx,
    queryConfig) {

  override def processElementsWithSameTimestamp(
    curRowList: JList[Row],
    lastAccumulator: Row,
    out: Collector[CRow]): Unit = {

    var i = 0
    // all same timestamp data should have same aggregation value.
    while (i < curRowList.size) {
      val curRow = curRowList.get(i)

      function.accumulate(lastAccumulator, curRow)
      i += 1
    }

    // emit output row
    i = 0
    while (i < curRowList.size) {
      val curRow = curRowList.get(i)

      // copy forwarded fields to output row
      function.setForwardedFields(curRow, output.row)

      //copy aggregates to output row
      function.setAggregationResults(lastAccumulator, output.row)
      out.collect(output)
      i += 1
    }
  }
}
