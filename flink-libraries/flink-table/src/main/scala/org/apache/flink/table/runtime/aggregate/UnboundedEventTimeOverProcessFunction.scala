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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.{Collector, Preconditions}
import org.apache.flink.api.common.state._
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.streaming.api.operators.TimestampedCollector
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}


/**
  * A ProcessFunction to support unbounded event-time over-window
  *
  * @param aggregates the aggregate functions
  * @param aggFields  the filed index which the aggregate functions use
  * @param forwardedFieldCount the input fields count
  * @param intermediateType the intermediate row tye which the state saved
  * @param inputType the input row tye which the state saved
  *
  */
class UnboundedEventTimeOverProcessFunction(
    private val aggregates: Array[AggregateFunction[_]],
    private val aggFields: Array[Int],
    private val forwardedFieldCount: Int,
    private val intermediateType: TypeInformation[Row],
    private val inputType: TypeInformation[Row])
  extends ProcessFunction[Row, Row]{

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(aggFields)
  Preconditions.checkArgument(aggregates.length == aggFields.length)

  private var output: Row = _
  // state to hold the accumulators of the aggregations
  private var accumulatorState: ValueState[Row] = _
  // state to hold rows until the next watermark arrives
  private var rowMapState: MapState[Long, JList[Row]] = _
  // list to sort timestamps to access rows in timestamp order
  private var sortedTimestamps: util.LinkedList[Long] = _


  override def open(config: Configuration) {
    output = new Row(forwardedFieldCount + aggregates.length)
    sortedTimestamps = new util.LinkedList[Long]()

    // initialize accumulator state
    val accDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("accumulatorstate", intermediateType)
    accumulatorState = getRuntimeContext.getState[Row](accDescriptor)

    // initialize row state
    val rowListTypeInfo: TypeInformation[JList[Row]] = new ListTypeInfo[Row](inputType)
    val mapStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]]("rowmapstate",
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]], rowListTypeInfo)
    rowMapState = getRuntimeContext.getMapState(mapStateDescriptor)
  }

  /**
    * Puts an element from the input stream into state if it is not late.
    * Registers a timer for the next watermark.
    *
    * @param input The input value.
    * @param ctx   The ctx to register timer or get current time
    * @param out   The collector for returning result values.
    *
    */
  override def processElement(
     input: Row,
     ctx:  ProcessFunction[Row, Row]#Context,
     out: Collector[Row]): Unit = {

    val timestamp = ctx.timestamp()
    val curWatermark = ctx.timerService().currentWatermark()

    // discard late record
    if (timestamp >= curWatermark) {
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
      ctx: ProcessFunction[Row, Row]#OnTimerContext,
      out: Collector[Row]): Unit = {

    Preconditions.checkArgument(out.isInstanceOf[TimestampedCollector[Row]])
    val collector = out.asInstanceOf[TimestampedCollector[Row]]

    val keyIterator = rowMapState.keys.iterator
    if (keyIterator.hasNext) {
      val curWatermark = ctx.timerService.currentWatermark
      var existEarlyRecord: Boolean = false
      var i = 0

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
        lastAccumulator = new Row(aggregates.length)
        while (i < aggregates.length) {
          lastAccumulator.setField(i, aggregates(i).createAccumulator())
          i += 1
        }
      }

      // emit the rows in order
      while (!sortedTimestamps.isEmpty) {
        val curTimestamp = sortedTimestamps.removeFirst()
        val curRowList = rowMapState.get(curTimestamp)
        collector.setAbsoluteTimestamp(curTimestamp)

        var j = 0
        while (j < curRowList.size) {
          val curRow = curRowList.get(j)
          i = 0

          // copy forwarded fields to output row
          while (i < forwardedFieldCount) {
            output.setField(i, curRow.getField(i))
            i += 1
          }

          // update accumulators and copy aggregates to output row
          i = 0
          while (i < aggregates.length) {
            val index = forwardedFieldCount + i
            val accumulator = lastAccumulator.getField(i).asInstanceOf[Accumulator]
            aggregates(i).accumulate(accumulator, curRow.getField(aggFields(i)))
            output.setField(index, aggregates(i).getValue(accumulator))
            i += 1
          }
          // emit output row
          collector.collect(output)
          j += 1
        }
        rowMapState.remove(curTimestamp)
      }

      accumulatorState.update(lastAccumulator)

      // if are are rows with timestamp > watermark, register a timer for the next watermark
      if (existEarlyRecord) {
        ctx.timerService.registerEventTimeTimer(curWatermark + 1)
      }
    }
  }

  /**
   * Inserts timestamps in order into a linked list.
   *
   * If timestamps arrive in order (as in case of using the RocksDB state backend) this is just
   * an append with O(1).
   */
  private def insertToSortedList(recordTimeStamp: Long) = {
    val listIterator = sortedTimestamps.listIterator(sortedTimestamps.size)
    var continue = true
    while (listIterator.hasPrevious && continue) {
      val timestamp = listIterator.previous
      if (recordTimeStamp >= timestamp) {
        listIterator.next
        listIterator.add(recordTimeStamp)
        continue = false
      }
    }

    if (continue) {
      sortedTimestamps.addFirst(recordTimeStamp)
    }
  }

}
