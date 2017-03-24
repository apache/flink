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

import java.util.{ArrayList, LinkedList, List => JList}

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
  private var accumulatorState: ValueState[Row] = _
  private var rowMapState: MapState[Long, JList[Row]] = _
  private var sortList: LinkedList[Long] = _


  override def open(config: Configuration) {
    output = new Row(forwardedFieldCount + aggregates.length)
    sortList = new LinkedList[Long]()

    val stateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("accumulatorstate", intermediateType)
    accumulatorState = getRuntimeContext.getState[Row](stateDescriptor)

    val rowListTypeInfo: TypeInformation[JList[Row]] =
      new ListTypeInfo[Row](inputType).asInstanceOf[TypeInformation[JList[Row]]]
    val mapStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]]("rowmapstate",
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]], rowListTypeInfo)
    rowMapState = getRuntimeContext.getMapState(mapStateDescriptor)
  }

  /**
    * Process one element from the input stream, not emit the output
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

    // discard later record
    if (ctx.timestamp() >= ctx.timerService().currentWatermark()) {
      // ensure every key just register on timer
      ctx.timerService.registerEventTimeTimer(ctx.timerService.currentWatermark + 1)

      var rowList = rowMapState.get(ctx.timestamp)
      if (rowList == null) {
        rowList = new ArrayList[Row]()
      }
      rowList.add(input)
      rowMapState.put(ctx.timestamp, rowList)
    }
  }

  /**
    * Called when a timer set fires, sort current records according the timestamp
    * and emit the output
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

    val mapIter = rowMapState.keys.iterator
    if (mapIter.hasNext) {
      val curWatermark = ctx.timerService.currentWatermark
      var existEarlyRecord: Boolean = false
      var i = 0

      // sort the record timestamp
      do {
        val recordTime = mapIter.next
        if (recordTime <= curWatermark) {
          insertToSortedList(recordTime)
        } else {
          existEarlyRecord = true
        }
      } while (mapIter.hasNext)

      // get last accumulator
      var lastAccumulator = accumulatorState.value
      if (lastAccumulator == null) {
        lastAccumulator = new Row(aggregates.length)
        while (i < aggregates.length) {
          lastAccumulator.setField(i, aggregates(i).createAccumulator())
          i += 1
        }
      }

      // emit the output in order
      while (!sortList.isEmpty) {
        val curTimeStamp = sortList.removeFirst()
        val curRowList = rowMapState.get(curTimeStamp)
        collector.setAbsoluteTimestamp(curTimeStamp)

        var j = 0
        while (j < curRowList.size) {
          val curRow = curRowList.get(j)
          i = 0
          while (i < forwardedFieldCount) {
            output.setField(i, curRow.getField(i))
            i += 1
          }

          i = 0
          while (i < aggregates.length) {
            val index = forwardedFieldCount + i
            val accumulator = lastAccumulator.getField(i).asInstanceOf[Accumulator]
            aggregates(i).accumulate(accumulator, curRow.getField(aggFields(i)))
            output.setField(index, aggregates(i).getValue(accumulator))
            i += 1
          }
          collector.collect(output)
          j += 1
        }
        rowMapState.remove(curTimeStamp)
      }

      accumulatorState.update(lastAccumulator)

      // if still exist records not emit this time, register a timer
      if (existEarlyRecord) {
        ctx.timerService.registerEventTimeTimer(curWatermark + 1)
      }
    }
  }

  /**
   * consider disorder records are in the minority,so reverse search location
    */
  private def insertToSortedList(recordTimeStamp: Long) = {
    val listIterator = sortList.listIterator(sortList.size)
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
      sortList.addFirst(recordTimeStamp)
    }
  }

}
