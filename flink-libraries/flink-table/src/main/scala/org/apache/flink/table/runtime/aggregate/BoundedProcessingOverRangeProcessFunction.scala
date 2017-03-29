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

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.functions.{ Accumulator, AggregateFunction }
import org.apache.flink.types.Row
import org.apache.flink.util.{ Collector, Preconditions }
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ListTypeInfo
import java.util.{ ArrayList, List => JList }
import org.apache.flink.api.common.typeinfo.BasicTypeInfo

/**
 * Process Function used for the aggregate in bounded proc-time OVER window
 * [[org.apache.flink.streaming.api.datastream.DataStream]]
 *
 * @param aggregates the list of all [[org.apache.flink.table.functions.AggregateFunction]]
 *                   used for this aggregation
 * @param aggFields  the position (in the input Row) of the input value for each aggregate
 * @param forwardedFieldCount Is used to indicate fields in the current element to forward
 * @param rowTypeInfo Is used to indicate the field schema
 * @param precedingTimeBoundary Is used to indicate the processing time boundaries
 * @param inputType It is used to mark the Row type of the input
 */
class BoundedProcessingOverRangeProcessFunction(
  private val aggregates: Array[AggregateFunction[_]],
  private val aggFields: Array[Array[Int]],
  private val forwardedFieldCount: Int,
  private val rowTypeInfo: RowTypeInfo,
  private val precedingTimeBoundary: Long,
  private val inputType: TypeInformation[Row])
    extends ProcessFunction[Row, Row] {

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(aggFields)
  Preconditions.checkArgument(aggregates.length == aggFields.length)

  private var output: Row = _
  private var accumulatorState: ValueState[Row] = _
  private var rowMapState: MapState[Long, JList[Row]] = _

  override def open(config: Configuration) {
    output = new Row(forwardedFieldCount + aggregates.length)

    // We keep the elements received in a MapState indexed based on their ingestion time
    val rowListTypeInfo: TypeInformation[JList[Row]] =
      new ListTypeInfo[Row](inputType).asInstanceOf[TypeInformation[JList[Row]]]
    val mapStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]]("rowmapstate",
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]], rowListTypeInfo)
    rowMapState = getRuntimeContext.getMapState(mapStateDescriptor)

    val stateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("overState", rowTypeInfo)
    accumulatorState = getRuntimeContext.getState(stateDescriptor)
  }

  override def processElement(
    input: Row,
    ctx: ProcessFunction[Row, Row]#Context,
    out: Collector[Row]): Unit = {

    val currentTime = ctx.timerService.currentProcessingTime
    // buffer the event incoming event

    // add current element to the window list of elements with corresponding timestamp
    var rowList = rowMapState.get(currentTime)
    // null value means that this si the first event received for this timestamp
    if (rowList == null) {
      rowList = new ArrayList[Row]()
      // register timer to process event once the current millisecond passed
      ctx.timerService.registerProcessingTimeTimer(currentTime + 1)
    }
    rowList.add(input)
    rowMapState.put(currentTime, rowList)

  }

  override def onTimer(
    timestamp: Long,
    ctx: ProcessFunction[Row, Row]#OnTimerContext,
    out: Collector[Row]): Unit = {

    // we consider the original timestamp of events that have registered this time trigger 1 ms ago
    val currentTime = timestamp - 1
    var i = 0

    // initialize the accumulators
    var accumulators = accumulatorState.value()

    if (null == accumulators) {
      accumulators = new Row(aggregates.length)
      i = 0
      while (i < aggregates.length) {
        accumulators.setField(i, aggregates(i).createAccumulator())
        i += 1
      }
    }

    // update the elements to be removed and retract them from aggregators
    val limit = currentTime - precedingTimeBoundary

    // we iterate through all elements in the window buffer based on timestamp keys
    // when we find timestamps that are out of interest, we retrieve corresponding elements
    // and eliminate them. Multiple elements could have been received at the same timestamp
    // the removal of old elements happens only once per proctime as onTimer is called only once
    val iter = rowMapState.keys.iterator
    val markToRemove = new ArrayList[Long]()
    while (iter.hasNext) {
      val elementKey = iter.next
      if (elementKey < limit) {
        // element key outside of window. Retract values
        val elementsRemove = rowMapState.get(elementKey)
        var iRemove = 0
        while (iRemove < elementsRemove.size()) {
          i = 0
          while (i < aggregates.length) {
            val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
            aggregates(i).retract(accumulator, elementsRemove.get(iRemove)
               .getField(aggFields(i)(0)))
            i += 1
          }
          iRemove += 1
        }
        // mark element for later removal not to modify the iterator over MapState
        markToRemove.add(elementKey)
      }
    }
    // need to remove in 2 steps not to have concurrent access errors via iterator to the MapState
    i = 0
    while (i < markToRemove.size()) {
      rowMapState.remove(markToRemove.get(i))
      i += 1
    }

    // get the list of elements of current proctime
    val currentElements = rowMapState.get(currentTime)
    // add current elements to aggregator. Multiple elements might have arrived in the same proctime
    // the same accumulator value will be computed for all elements
    var iElemenets = 0
    while (iElemenets < currentElements.size()) {
      val input = currentElements.get(iElemenets)
      i = 0
      while (i < aggregates.length) {
        val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
        aggregates(i).accumulate(accumulator, input.getField(aggFields(i)(0)))
        i += 1
      }
      iElemenets += 1
    }

    // we need to build the output and emit for every event received at this proctime
    iElemenets = 0
    while (iElemenets < currentElements.size()) {
      val input = currentElements.get(iElemenets)

      // set the fields of the last event to carry on with the aggregates
      i = 0
      while (i < forwardedFieldCount) {
        output.setField(i, input.getField(i))
        i += 1
      }

      // add the accumulators values to result
      i = 0
      while (i < aggregates.length) {
        val index = forwardedFieldCount + i
        val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
        output.setField(index, aggregates(i).getValue(accumulator))
        i += 1
      }
      out.collect(output)
      iElemenets += 1
    }

    // update the value of accumulators for future incremental computation
    accumulatorState.update(accumulators)

  }

}
