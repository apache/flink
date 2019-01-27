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

package org.apache.flink.table.runtime.overagg

import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.codegen._
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow, JoinedRow}
import org.apache.flink.table.runtime.AbstractStreamOperatorWithMetrics
import org.apache.flink.table.runtime.functions.{AggsHandleFunction, ExecutionContextImpl}
import org.apache.flink.table.runtime.sort.RecordComparator
import org.apache.flink.table.runtime.util.{ResettableExternalBuffer, StreamRecordCollector}
import org.apache.flink.table.typeutils.{AbstractRowSerializer, BaseRowSerializer, BinaryRowSerializer}

/**
 * the operator for OVER window need cache data by ResettableExternalBuffer for [[OverWindowFrame]]
 *
 * @param memorySize           the memory is assigned to a resettable external buffer.
 * @param overWindowFrames     the window frames belong to this operator.
 * @param groupingSorter       the generated sort which is used for generating the comparator among
 *                             partitions.
 */
class BufferDataOverWindowOperator(
    memorySize: Int,
    overWindowFrames: Array[OverWindowFrame],
    var groupingSorter: GeneratedSorter)
  extends AbstractStreamOperatorWithMetrics[BaseRow]
  with OneInputStreamOperator[BaseRow, BaseRow] {

  private var partitionComparator: RecordComparator = _

  private var lastInput: BaseRow = _

  private var serializer: AbstractRowSerializer[BaseRow] = _

  private var currentData: ResettableExternalBuffer = _

  private var joinedRows: Array[JoinedRow] = _

  private var collector: StreamRecordCollector[BaseRow] = _

  override def open(): Unit = {
    super.open()

    val memManager = getContainingTask.getEnvironment.getMemoryManager
    val ioManager = getContainingTask.getEnvironment.getIOManager

    this.serializer = getOperatorConfig.getTypeSerializerIn1(getUserCodeClassloader)
      .asInstanceOf[AbstractRowSerializer[BaseRow]]
    this.currentData = new ResettableExternalBuffer(memManager, ioManager, memManager
        .allocatePages(this, memorySize / memManager.getPageSize), serializer)

    partitionComparator = CodeGenUtils.compile(
      Thread.currentThread.getContextClassLoader,
      groupingSorter.comparator.name,
      groupingSorter.comparator.code).newInstance.asInstanceOf[RecordComparator]
    partitionComparator.init(groupingSorter.serializers, groupingSorter.comparators)
    groupingSorter = null
    overWindowFrames.foreach(_.open(new ExecutionContextImpl(this, getRuntimeContext)))

    collector = new StreamRecordCollector[BaseRow](output)
    joinedRows = overWindowFrames.map(_ => new JoinedRow())
  }

  override def processElement(element: StreamRecord[BaseRow]): Unit = {
    val input = element.getValue
    if (lastInput != null && partitionComparator.compare(lastInput, input) != 0) {
      processCurrentData()
      currentData.reset()
    }
    lastInput = serializer.copy(input)
    currentData.add(lastInput)
  }

  override def endInput(): Unit = {
    if (currentData.size() > 0) {
      processCurrentData()
      currentData.reset()
    }
  }

  private def processCurrentData(): Unit = {
    overWindowFrames.foreach(_.resetBuffer(currentData))
    var rowIndex = 0
    val bufferIterator = currentData.newIterator()
    while (bufferIterator.advanceNext()) {
      val currentRow = bufferIterator.getRow
      val ret = overWindowFrames.map(_.write(rowIndex, currentRow))
      var index = 0
      collector.collect(ret.fold(currentRow) { (row1, row2) =>
        joinedRows(index).replace(row1, row2)
        index += 1
        joinedRows(index - 1)
      })
      rowIndex += 1
    }
    bufferIterator.close()
  }

  override def close(): Unit = {
    super.close()
    this.currentData.close()
  }
}

/**
 * The operator for OVER window don't need cache data [[OverWindowFrame]].
 * Then this operator can calculate the accumulator by the current row.
 */
class OverWindowOperator(
    aggsHandles: Array[GeneratedAggsHandleFunction],
    resetAccs: Array[Boolean] ,
    var groupingSorter: GeneratedSorter)
    extends AbstractStreamOperatorWithMetrics[BaseRow]
    with OneInputStreamOperator[BaseRow, BaseRow] {

  private var partitionComparator: RecordComparator = _

  private var lastInput: BaseRow = _

  private var processors: Array[AggsHandleFunction] = _

  private var joinedRows: Array[JoinedRow] = _

  private var collector: StreamRecordCollector[BaseRow] = _

  private var inputRowSerializer: AbstractRowSerializer[BaseRow] = _

  override def open(): Unit = {
    super.open()

    inputRowSerializer = getOperatorConfig.getTypeSerializerIn1(getUserCodeClassloader)
        .asInstanceOf[AbstractRowSerializer[BaseRow]]

    partitionComparator = CodeGenUtils.compile(
      Thread.currentThread.getContextClassLoader,
      groupingSorter.comparator.name,
      groupingSorter.comparator.code).newInstance.asInstanceOf[RecordComparator]
    partitionComparator.init(groupingSorter.serializers, groupingSorter.comparators)
    groupingSorter = null

    processors = aggsHandles.map(_.newInstance(Thread.currentThread.getContextClassLoader))

    processors.foreach(_.open(new ExecutionContextImpl(this, getRuntimeContext)))
    collector = new StreamRecordCollector[BaseRow](output)
    joinedRows = processors.map(_ => new JoinedRow())
  }

  override def processElement(element: StreamRecord[BaseRow]): Unit = {
    val input = element.getValue

    if (lastInput == null || partitionComparator.compare(lastInput, input) != 0) {
      //reset the processor ACC
      processors.foreach { processor =>
        processor.setAccumulators(processor.createAccumulators())
      }
    }
    //calculate the ACC
    val ret = processors.map { processor =>
      processor.accumulate(input)
      processor.getValue
    }
    var index = 0
    collector.collect(ret.fold(input) { (row1, row2) =>
      joinedRows(index).replace(row1, row2)
      index += 1
      joinedRows(index - 1)
    })
    //reset the processor ACC if it need.
    processors.zip(resetAccs).foreach { case (processor, resetAcc) =>
        if (resetAcc) {
          processor.setAccumulators(processor.createAccumulators())
        }
    }
    lastInput = inputRowSerializer.copy(input)
  }

  override def endInput(): Unit = {}
}
