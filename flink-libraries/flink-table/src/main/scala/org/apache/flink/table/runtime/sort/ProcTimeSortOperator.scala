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

import org.apache.flink.api.common.functions.Comparator
import org.apache.flink.runtime.operators.sort.{IndexedSorter, QuickSort}
import org.apache.flink.runtime.state.keyed.{KeyedListState, KeyedListStateDescriptor}
import org.apache.flink.runtime.state.{VoidNamespace, VoidNamespaceSerializer}
import org.apache.flink.streaming.api.operators._
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.codegen.GeneratedSorter
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.sort.BinaryInMemorySortBuffer
import org.apache.flink.table.typeutils.{AbstractRowSerializer, BaseRowTypeInfo, BinaryRowSerializer}

/**
  * Sort on proc-time and additional secondary sort attributes.
  *
  * @param inputRowType      The data type of the input data.
  * @param gSorter           A generated sorter.
  * @param memorySize        The size of in memory buffer.
  */
class ProcTimeSortOperator(
    private val inputRowType: BaseRowTypeInfo,
    private var gSorter: GeneratedSorter,
    private val memorySize: Double) extends SortBaseOperator {

  @transient private var dataState: KeyedListState[VoidNamespace, BaseRow] = _

  @transient private var rowComparator: Comparator[BaseRow] = _

  @transient private var buffer: BinaryInMemorySortBuffer = _

  @transient private var sorter: IndexedSorter = _

  @transient private var binarySerializer: BinaryRowSerializer = _

  override def open() {
    super.open()

    rowComparator = getComparator(gSorter)

    val recordSerializer = inputRowType.createSerializer(getRuntimeContext.getExecutionConfig)

    val timeSortMapStateDescriptor = new KeyedListStateDescriptor(
      "sortDataState",
      VoidNamespaceSerializer.INSTANCE,
      recordSerializer)
    dataState = getKeyedState(timeSortMapStateDescriptor)

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
    val processingTime = timerService.currentProcessingTime
    dataState.add(VoidNamespace.INSTANCE, input)
    // register proc time timer
    timerService.registerProcessingTimeTimer(processingTime + 1)
  }

  /**
    * Invoked when a processing-time timer fires.
    */
  override def onProcessingTime(timer: InternalTimer[BaseRow, VoidNamespace]): Unit = {
    // gets all rows for the triggering timestamps
    val itr = dataState.get(VoidNamespace.INSTANCE).iterator
    if (itr.hasNext) {
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
      dataState.remove(VoidNamespace.INSTANCE)
      buffer.reset()
    }
  }

  override def endInput(): Unit = {}
}
