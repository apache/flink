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
import java.util.{ArrayList => JArrayList, List => JList}

import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.{ListTypeInfo, RowTypeInfo}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}

/**
  * Process Function for ROWS clause event-time unbounded OVER window
  *
  * @param aggregates           the list of all [[AggregateFunction]] used for this aggregation
  * @param aggFields            the position (in the input Row) of the input value for each aggregate
  * @param forwardedFieldCount  the count of forwarded fields.
  * @param aggregationStateType the row type info of aggregation
  * @param inputRowType         the row type info of input row
  */
class RowsClauseUnboundedEventTimeOverProcessFunction(
    private val aggregates: Array[AggregateFunction[_]],
    private val aggFields: Array[Int],
    private val forwardedFieldCount: Int,
    private val aggregationStateType: RowTypeInfo,
    private val inputRowType: RowTypeInfo)
  extends ProcessFunction[Row, Row] {

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(aggFields)
  Preconditions.checkArgument(aggregates.length == aggFields.length)
  Preconditions.checkNotNull(forwardedFieldCount)
  Preconditions.checkNotNull(aggregationStateType)

  private var output: Row = _

  // the state which keeps the last triggering timestamp
  private var lastTriggeringTsState: ValueState[Long] = _

  // the state which used to materialize the accumulator for incremental calculation
  private var accumulatorState: ValueState[Row] = _

  // the state which keeps all the data that are not expired.
  // The first element (as the mapState key) of the tuple is the time stamp. Per each time stamp,
  // the second element of tuple is a list that contains the entire data of all the rows belonging
  // to this time stamp.
  private var dataState: MapState[Long, JList[Row]] = _

  override def open(config: Configuration) {

    output = new Row(forwardedFieldCount + aggregates.length)

    val lastTriggeringTsDescriptor: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("lastTriggeringTsState", classOf[Long])
    lastTriggeringTsState = getRuntimeContext.getState(lastTriggeringTsDescriptor)

    val accumulatorStateDescriptor =
      new ValueStateDescriptor[Row]("accumulatorState", aggregationStateType)
    accumulatorState = getRuntimeContext.getState(accumulatorStateDescriptor)

    val keyTypeInformation: TypeInformation[Long] =
      BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]]
    val valueTypeInformation: TypeInformation[JList[Row]] = new ListTypeInfo[Row](inputRowType)

    val mapStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]](
        "dataState",
        keyTypeInformation,
        valueTypeInformation)

    dataState = getRuntimeContext.getMapState(mapStateDescriptor)

  }

  override def processElement(
    input: Row,
    ctx: ProcessFunction[Row, Row]#Context,
    out: Collector[Row]): Unit = {

    // triggering timestamp for trigger calculation
    val triggeringTs = ctx.timestamp

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
    ctx: ProcessFunction[Row, Row]#OnTimerContext,
    out: Collector[Row]): Unit = {

    // gets all window data from state for the calculation
    val inputs: JList[Row] = dataState.get(timestamp)

    if (null != inputs) {

      var accumulators = accumulatorState.value

      var dataListIndex = 0
      var aggregatesIndex = 0

      while (dataListIndex < inputs.size) {
        val input = inputs.get(dataListIndex)

        // initialize when first run or failover recovery per key
        if (null == accumulators) {
          accumulators = new Row(aggregates.length)
          aggregatesIndex = 0
          while (aggregatesIndex < aggregates.length) {
            accumulators.setField(aggregatesIndex, aggregates(aggregatesIndex).createAccumulator())
            aggregatesIndex += 1
          }
        }

        // copy forwarded fields to output row
        aggregatesIndex = 0
        while (aggregatesIndex < forwardedFieldCount) {
          output.setField(aggregatesIndex, input.getField(aggregatesIndex))
          aggregatesIndex += 1
        }

        // accumulate current row and set aggregate in output row
        aggregatesIndex = 0
        while (aggregatesIndex < aggregates.length) {
          val index = forwardedFieldCount + aggregatesIndex
          val accumulator = accumulators.getField(aggregatesIndex).asInstanceOf[Accumulator]
          aggregates(aggregatesIndex).accumulate(accumulator, input.getField(aggFields(aggregatesIndex)))
          output.setField(index, aggregates(aggregatesIndex).getValue(accumulator))
          aggregatesIndex += 1
        }
        dataListIndex += 1

        out.collect(output)
      }
      accumulatorState.update(accumulators)
    }

    // keep up timestamps of no use datas
    val noUseTsList: JList[Long] = new JArrayList[Long]

    val dataTimestampIt = dataState.keys.iterator
    while (dataTimestampIt.hasNext) {
      val dataTs = dataTimestampIt.next
      if (dataTs < timestamp) {
        noUseTsList.add(dataTs)
      }
    }

    var i = 0
    while (i < noUseTsList.size()) {
      dataState.remove(noUseTsList.get(i))
      i += 1
    }

    lastTriggeringTsState.update(timestamp)
  }
}


