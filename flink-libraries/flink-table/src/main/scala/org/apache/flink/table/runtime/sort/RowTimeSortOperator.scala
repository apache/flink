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

package org.apache.flink.table.runtime.sort

import java.lang.{Long => JLong}

import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.runtime.operators.sort.{IndexedSorter, QuickSort}
import org.apache.flink.runtime.state.keyed.{KeyedListState, KeyedListStateDescriptor, KeyedValueState, KeyedValueStateDescriptor}
import org.apache.flink.runtime.state.{VoidNamespace, VoidNamespaceSerializer}
import org.apache.flink.streaming.api.operators._
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.codegen.GeneratedSorter
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.typeutils.{AbstractRowSerializer, BaseRowTypeInfo, BinaryRowSerializer}

/**
  * Sort based on event-time and possibly additional secondary sort attributes.
  *
  * @param inputRowType      The data type of the input data.
  * @param gSorter           A generated sorter.
  * @param rowtimeIdx        The index of the rowtime field.
  * @param memorySize        The size of in memory buffer.
  */
class RowTimeSortOperator(
    private val inputRowType: BaseRowTypeInfo,
    private var gSorter: GeneratedSorter,
    private val rowtimeIdx: Int,
    private val memorySize: Double)
  extends SortBaseOperator {

  @transient private var timeListState: KeyedListState[JLong, BaseRow] = _
  // the state keep the last triggering timestamp. Used to filter late events.
  @transient private var lastTriggeringTsState: KeyedValueState[VoidNamespace, JLong] = _

  @transient private var buffer: BinaryInMemorySortBuffer = _

  @transient private var sorter: IndexedSorter = _

  @transient private var binarySerializer: BinaryRowSerializer = _

  override def open() {
    super.open()

    val recordSerializer = inputRowType.createSerializer()
    val dataStateDescriptor = new KeyedListStateDescriptor(
      "dataState",
      LongSerializer.INSTANCE,
      recordSerializer)
    timeListState = getKeyedState(dataStateDescriptor)

    val lastTriggeringTsDescriptor = new KeyedValueStateDescriptor(
      "lastTriggeringTsState",
      VoidNamespaceSerializer.INSTANCE,
      LongSerializer.INSTANCE)
    lastTriggeringTsState = getKeyedState(lastTriggeringTsDescriptor)

    binarySerializer = new BinaryRowSerializer(
      recordSerializer.asInstanceOf[AbstractRowSerializer[_ <: BaseRow]].getTypes: _*)
    val memManager = getContainingTask.getEnvironment.getMemoryManager
    val memorySegments = memManager
      .allocatePages(getContainingTask, (memorySize / memManager.getPageSize).toInt)
    val comparator = getComparator(gSorter)
    val computer = getComputer(gSorter)
    buffer = BinaryInMemorySortBuffer.createBuffer(
      memManager,
      computer,
      recordSerializer,
      binarySerializer,
      comparator,
      memorySegments,
      0,
      0)
    sorter = new QuickSort()

    gSorter = null
  }


  override def processElement(in: StreamRecord[BaseRow]): Unit = {

    val input = in.getValue

    // timestamp of the processed row
    val rowtime = input.getLong(rowtimeIdx)
    val lastTriggeringTs = lastTriggeringTsState.get(VoidNamespace.INSTANCE)
    // check if the row is late and drop it if it is late
    if (lastTriggeringTs == null || rowtime > lastTriggeringTs) {
      timeListState.add(rowtime, input)
      // register event time timer
      timerService.registerEventTimeTimer(rowtime)
    }
  }

  /**
    * Invoked when an event-time timer fires.
    */
  override def onEventTime(timer: InternalTimer[BaseRow, VoidNamespace]): Unit = {
    val timestamp = timer.getTimestamp
    // gets all rows for the triggering timestamps
    val itr = timeListState.get(timestamp).iterator
    if (itr.hasNext) {
      // sort the inputs
      while (itr.hasNext) {
        buffer.write(itr.next)
      }
      sorter.sort(buffer)

      // emit the sorted inputs
      val outputItor = buffer.getIterator
      var row = binarySerializer.createInstance()
      row = outputItor.next(row)
      while (row != null) {
        collector.collect(row)
        row = outputItor.next(row)
      }

      // remove emitted rows from state
      timeListState.remove(timestamp)
      lastTriggeringTsState.put(VoidNamespace.INSTANCE, timestamp)
      buffer.reset()
    }
  }

  override def endInput(): Unit = {}
}
