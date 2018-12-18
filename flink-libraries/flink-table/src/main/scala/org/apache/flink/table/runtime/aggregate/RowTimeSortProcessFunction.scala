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

import java.util.{Collections, ArrayList => JArrayList, List => JList}

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.operators.TimestampedCollector
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}

/**
 * ProcessFunction to sort on event-time and possibly additional secondary sort attributes.
 *
  * @param inputRowType The data type of the input data.
  * @param rowtimeIdx The index of the rowtime field.
  * @param rowComparator A comparator to sort rows.
 */
class RowTimeSortProcessFunction(
    private val inputRowType: CRowTypeInfo,
    private val rowtimeIdx: Int,
    private val rowComparator: Option[CollectionRowComparator])
  extends ProcessFunction[CRow, CRow] {

  Preconditions.checkNotNull(rowComparator)

  // State to collect rows between watermarks.
  private var dataState: MapState[Long, JList[Row]] = _

  // the state keep the last triggering timestamp. Used to filter late events.
  private var lastTriggeringTsState: ValueState[Long] = _

  private var outputC: CRow = _
  
  override def open(config: Configuration) {
     
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
    
    val lastTriggeringTsDescriptor: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("lastTriggeringTsState", classOf[Long])
    lastTriggeringTsState = getRuntimeContext.getState(lastTriggeringTsDescriptor)
    
    outputC = new CRow()
  }

  
  override def processElement(
    inputC: CRow,
    ctx: ProcessFunction[CRow, CRow]#Context,
    out: Collector[CRow]): Unit = {

    val input = inputC.row
    
    // timestamp of the processed row
    val rowtime = input.getField(rowtimeIdx).asInstanceOf[Long]

    val lastTriggeringTs = lastTriggeringTsState.value

    // check if the row is late and drop it if it is late
    if (rowtime > lastTriggeringTs) {
      // get list for timestamp
      val rows = dataState.get(rowtime)
      if (null != rows) {
        rows.add(input)
        dataState.put(rowtime, rows)
      } else {
        val rows = new JArrayList[Row]
        rows.add(input)
        dataState.put(rowtime, rows)

        // register event time timer
        ctx.timerService.registerEventTimeTimer(rowtime)
      }
    }
  }

  override def onTimer(
    timestamp: Long,
    ctx: ProcessFunction[CRow, CRow]#OnTimerContext,
    out: Collector[CRow]): Unit = {

    // remove timestamp set outside of ProcessFunction.
    out.asInstanceOf[TimestampedCollector[_]].eraseTimestamp()

    // gets all rows for the triggering timestamps
    val inputs: JList[Row] = dataState.get(timestamp)

    if (null != inputs) {

      // sort rows on secondary fields if necessary
      if (rowComparator.isDefined) {
        Collections.sort(inputs, rowComparator.get)
      }

      // emit rows in order
      var i = 0
      while (i < inputs.size) {
        outputC.row = inputs.get(i)
        out.collect(outputC)
        i += 1
      }
    
      // remove emitted rows from state
      dataState.remove(timestamp)
      lastTriggeringTsState.update(timestamp)
    }
  }
  
}
