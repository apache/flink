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

import java.util
import org.apache.flink.table.api.types.{DataTypes, RowType}
import org.apache.flink.table.codegen.{CodeGenUtils, GeneratedAggsHandleFunction, GeneratedBoundComparator}
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.runtime.functions.{AggsHandleFunction, ExecutionContext}
import org.apache.flink.table.runtime.util.ResettableExternalBuffer
import org.apache.flink.table.typeutils.{AbstractRowSerializer, TypeUtils}

/**
 * A window frame calculates the results for those records belong to a window frame.
 * Before use a frame must be prepared by passing it all the records in the current partition.
 */
abstract class OverWindowFrame(aggsHandleFunction: GeneratedAggsHandleFunction)
    extends Serializable {
  def open(ctx: ExecutionContext): Unit

  def resetBuffer(rows: ResettableExternalBuffer): Unit

  //return the ACC of the window frame.
  def write(index: Int, current: BaseRow): BaseRow
}

object OverWindowFrame {
  def getNextOrNull(iterator: ResettableExternalBuffer#BufferIterator): BinaryRow = {
    if (iterator.advanceNext()) iterator.getRow.copy() else null
  }
}

/**
 * The UnboundPreceding window frame calculates frames with the following SQL form:
 * ... BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 */
class UnboundedPrecedingOverWindowFrame(
    aggsHandleFunction: GeneratedAggsHandleFunction,
    var boundComparator: GeneratedBoundComparator)
  extends OverWindowFrame(aggsHandleFunction) {

  private[this] var processor: AggsHandleFunction = _
  private[this] var accValue: BaseRow = _
  private[this] var rbound: BoundComparator = _

  /**
   * An iterator over the input
   */
  private[this] var inputIterator: ResettableExternalBuffer#BufferIterator = _

  /** The next row from `input`. */
  private[this] var nextRow: BinaryRow = _

  /**
   * Index of the first input row with a value greater than the upper bound of the current
   * output row.
   */
  private[this] var inputIndex = 0

  override def open(ctx: ExecutionContext): Unit = {
    processor = aggsHandleFunction.newInstance(Thread.currentThread.getContextClassLoader)
    processor.open(ctx)
    rbound = CodeGenUtils.compile(// currentThread must be user class loader.
      Thread.currentThread.getContextClassLoader, boundComparator.name, boundComparator.code)
        .newInstance.asInstanceOf[BoundComparator]
    boundComparator = null
  }

  override def resetBuffer(rows: ResettableExternalBuffer): Unit = {
    inputIndex = 0
    if (inputIterator != null) {
      inputIterator.close()
    }
    inputIterator = rows.newIterator()
    if (inputIterator.advanceNext()) {
      nextRow = inputIterator.getRow.copy()
    }
    //reset the accumulators value
    processor.setAccumulators(processor.createAccumulators())
    rbound.reset()
  }

  override def write(index: Int, current: BaseRow): BaseRow = {
    var bufferUpdated = index == 0

    // Add all rows to the aggregates for which the input row value is equal to or less than
    // the output row upper bound.
    while (nextRow != null && rbound.compare(nextRow, inputIndex, current, index) <= 0) {
      processor.accumulate(nextRow)
      nextRow = OverWindowFrame.getNextOrNull(inputIterator)
      inputIndex += 1
      bufferUpdated = true
    }
    if (bufferUpdated) {
      accValue = processor.getValue
    }
    accValue
  }
}

/**
 * The UnboundFollowing window frame calculates frames with the following SQL form:
 * ... BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING.
 */
class UnboundedFollowingOverWindowFrame(
    aggsHandleFunction: GeneratedAggsHandleFunction,
    var boundComparator: GeneratedBoundComparator,
    valueType: RowType)
  extends OverWindowFrame(aggsHandleFunction) {

  private val valueSer = DataTypes.createInternalSerializer(valueType)
      .asInstanceOf[AbstractRowSerializer[BaseRow]]

  private[this] var processor: AggsHandleFunction = _
  private[this] var accValue: BaseRow = _
  private[this] var lbound: BoundComparator = _

  /** Rows of the partition currently being processed. */
  private[this] var input: ResettableExternalBuffer = _

  /**
   * Index of the first input row with a value equal to or greater than the lower bound of the
   * current output row.
   */
  private[this] var inputIndex = 0

  override def open(ctx: ExecutionContext): Unit = {
    processor = aggsHandleFunction.newInstance(Thread.currentThread.getContextClassLoader)
    processor.open(ctx)
    lbound = CodeGenUtils.compile(// currentThread must be user class loader.
      Thread.currentThread.getContextClassLoader, boundComparator.name,
      boundComparator.code).newInstance.asInstanceOf[BoundComparator]
    boundComparator = null
  }

  /** Prepare the frame for calculating a new partition. */
  override def resetBuffer(rows: ResettableExternalBuffer): Unit = {
    input = rows
    inputIndex = 0
    //cleanup the retired accumulators value
    processor.setAccumulators(processor.createAccumulators())
    lbound.reset()
  }

  override def write(index: Int, current: BaseRow): BaseRow = {
    var bufferUpdated = index == 0

    // Ignore all the rows from the buffer for which the input row value is smaller than
    // the output row lower bound.
    val iterator = input.newIterator(inputIndex)

    var nextRow = OverWindowFrame.getNextOrNull(iterator)
    while (nextRow != null && lbound.compare(nextRow, inputIndex, current, index) < 0) {
      inputIndex += 1
      bufferUpdated = true
      nextRow = OverWindowFrame.getNextOrNull(iterator)
    }

    // Only recalculate and update when the buffer changes.
    if (bufferUpdated) {
      //cleanup the retired accumulators value
      processor.setAccumulators(processor.createAccumulators())

      if (nextRow != null) {
        processor.accumulate(nextRow)
      }
      while (iterator.advanceNext()) {
        processor.accumulate(iterator.getRow)
      }
      accValue = valueSer.copy(processor.getValue)
    }
    iterator.close()
    accValue
  }
}

/**
 * The unbounded window frame calculates frames with the following SQL forms:
 * ... (No Frame Definition)
 * ... BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
 */
class UnboundedOverWindowFrame(
    aggsHandleFunction: GeneratedAggsHandleFunction,
    valueType: RowType)
  extends OverWindowFrame(aggsHandleFunction) {
  private[this] var processor: AggsHandleFunction = _
  private[this] var accValue: BaseRow = _

  private val valueSer = DataTypes.createInternalSerializer(valueType)
      .asInstanceOf[AbstractRowSerializer[BaseRow]]

  override def open(ctx: ExecutionContext): Unit = {
    processor = aggsHandleFunction.newInstance(Thread.currentThread.getContextClassLoader)
    processor.open(ctx)
  }

  /** Prepare the frame for calculating a new partition. Process all rows eagerly. */
  override def resetBuffer(rows: ResettableExternalBuffer): Unit = {
    //cleanup the retired accumulators value
    processor.setAccumulators(processor.createAccumulators())
    val iterator = rows.newIterator()
    while (iterator.advanceNext()) {
      processor.accumulate(iterator.getRow)
    }
    accValue = valueSer.copy(processor.getValue)
    iterator.close()
  }

  override def write(index: Int, current: BaseRow): BaseRow = {
    accValue
  }
}

/**
 * The sliding window frame calculates frames with the following SQL form:
 * ... BETWEEN 1 PRECEDING AND 1 FOLLOWING
 *
 * @param lboundComparator comparator used to identify the lower bound of an output row.
 * @param rboundComparator comparator used to identify the upper bound of an output row.
 */
class SlidingOverWindowFrame(
    inputType: RowType,
    valueType: RowType,
    aggsHandleFunction: GeneratedAggsHandleFunction,
    var lboundComparator: GeneratedBoundComparator,
    var rboundComparator: GeneratedBoundComparator)
  extends OverWindowFrame(aggsHandleFunction) {

  private val inputSer = DataTypes.createInternalSerializer(inputType)
      .asInstanceOf[AbstractRowSerializer[BaseRow]]
  private val valueSer = DataTypes.createInternalSerializer(valueType)
      .asInstanceOf[AbstractRowSerializer[BaseRow]]

  private[this] var processor: AggsHandleFunction = _
  private[this] var accValue: BaseRow = _
  private[this] var lbound: BoundComparator = _
  private[this] var rbound: BoundComparator = _

  private[this] var inputIterator: ResettableExternalBuffer#BufferIterator = _

  /** The next row from `input`. */
  private[this] var nextRow: BaseRow = _

  /** The rows within current sliding window. */
  private[this] val buffer = new util.ArrayDeque[BaseRow]()

  /**
   * Index of the first input row with a value greater than the upper bound of the current
   * output row.
   */
  private[this] var inputHighIndex = 0

  /**
   * Index of the first input row with a value equal to or greater than the lower bound of the
   * current output row.
   */
  private[this] var inputLowIndex = 0

  override def open(ctx: ExecutionContext): Unit = {
    processor = aggsHandleFunction.newInstance(Thread.currentThread.getContextClassLoader)
    processor.open(ctx)
    lbound = CodeGenUtils.compile(// currentThread must be user class loader.
      Thread.currentThread.getContextClassLoader, lboundComparator.name, lboundComparator.code)
        .newInstance.asInstanceOf[BoundComparator]
    rbound = CodeGenUtils.compile(// currentThread must be user class loader.
      Thread.currentThread.getContextClassLoader, rboundComparator.name, rboundComparator.code)
        .newInstance.asInstanceOf[BoundComparator]
    lboundComparator = null
    rboundComparator = null
  }

  /** Prepare the frame for calculating a new partition. Reset all variables. */
  override def resetBuffer(rows: ResettableExternalBuffer): Unit = {
    if (inputIterator != null) {
      inputIterator.close()
    }
    inputIterator = rows.newIterator()
    nextRow = OverWindowFrame.getNextOrNull(inputIterator)
    inputHighIndex = 0
    inputLowIndex = 0
    buffer.clear()
    //cleanup the retired accumulators value
    processor.setAccumulators(processor.createAccumulators())
    lbound.reset()
    rbound.reset()
  }

  /** Write the frame columns for the current row to the given target row. */
  override def write(index: Int, current: BaseRow): BaseRow = {
    var bufferUpdated = index == 0

    // Drop all rows from the buffer for which the input row value is smaller than
    // the output row lower bound.
    while (!buffer.isEmpty && lbound.compare(buffer.peek(), inputLowIndex, current, index) < 0) {
      buffer.remove()
      inputLowIndex += 1
      bufferUpdated = true
    }

    // Add all rows to the buffer for which the input row value is equal to or less than
    // the output row upper bound.
    while (nextRow != null && rbound.compare(nextRow, inputHighIndex, current, index) <= 0) {
      if (lbound.compare(nextRow, inputLowIndex, current, index) < 0) {
        inputLowIndex += 1
      } else {
        buffer.add(inputSer.copy(nextRow))
        bufferUpdated = true
      }
      nextRow = OverWindowFrame.getNextOrNull(inputIterator)
      inputHighIndex += 1
    }

    // Only recalculate and update when the buffer changes.
    if (bufferUpdated) {
      //cleanup the retired accumulators value
      processor.setAccumulators(processor.createAccumulators())
      val iter = buffer.iterator()
      while (iter.hasNext) {
        processor.accumulate(iter.next())
      }
      accValue = valueSer.copy(processor.getValue)
    }
    accValue
  }
}

/**
 * The insensitive window frame calculates the statements which shouldn't care the window frame,
 * for example RANK/DENSE_RANK/PERCENT_RANK/CUME_DIST/ROW_NUMBER.
 * @param aggsHandleFunction the aggregate function
 */
class InsensitiveWindowFrame(
    aggsHandleFunction: GeneratedAggsHandleFunction)
  extends OverWindowFrame(aggsHandleFunction) {
  private[this] var processor: AggsHandleFunction = _
  override def open(ctx: ExecutionContext): Unit = {
    processor = aggsHandleFunction.newInstance(Thread.currentThread.getContextClassLoader)
    processor.open(ctx)
  }

  override def resetBuffer(rows: ResettableExternalBuffer): Unit = {
    //reset the accumulator value
    processor.setAccumulators(processor.createAccumulators())
  }

  override def write(index: Int, current: BaseRow): BaseRow = {
    processor.accumulate(current)
    processor.getValue
  }
}

/**
 * The offset window frame calculates frames containing LEAD/LAG statements.
 * @param aggsHandleFunction the aggregate function
 * @param offset it means the offset within a partition if calcOffsetFunc is null.
 * @param calcOffsetFunc calculate the real offset when the function is not null.
 */
class OffsetOverWindowFrame(
    aggsHandleFunction: GeneratedAggsHandleFunction,
    offset: Long,
    calcOffsetFunc: (BaseRow) => Long)
  extends OverWindowFrame(aggsHandleFunction) {

  private[this] var processor: AggsHandleFunction = _

  //inputIterator and inputIndex are need when calcOffsetFunc is null.
  private[this] var inputIterator: ResettableExternalBuffer#BufferIterator = _
  private[this] var inputIndex = 0L

  //externalBuffer is need when calcOffsetFunc is not null.
  private[this] var externalBuffer: ResettableExternalBuffer = _

  private[this] var currentBufferLength = 0L
  override def open(ctx: ExecutionContext): Unit = {
    processor = aggsHandleFunction.newInstance(Thread.currentThread.getContextClassLoader)
    processor.open(ctx)
  }

  override def resetBuffer(rows: ResettableExternalBuffer): Unit = {
    //reset the accumulator value
    processor.setAccumulators(processor.createAccumulators())
    currentBufferLength = rows.size()
    inputIndex = offset
    if (calcOffsetFunc == null) {
      if (inputIterator != null) {
        inputIterator.close()
      }
      if (offset >= 0) {
        inputIterator = rows.newIterator(offset.toInt)
      } else {
        inputIterator = rows.newIterator()
      }
    } else {
      externalBuffer = rows
    }
  }

  override def write(index: Int, current: BaseRow): BaseRow = {

    if (calcOffsetFunc != null) {
      //poor performance here
      val realIndex = calcOffsetFunc(current) + index
      if (realIndex >= 0 && realIndex < currentBufferLength) {
        val tempIterator = externalBuffer.newIterator(realIndex.toInt)
        processor.accumulate(OverWindowFrame.getNextOrNull(tempIterator))
        tempIterator.close()
      } else {
        //reset the default based current row
        processor.retract(current)
      }
    } else {
      if (inputIndex >= 0 && inputIndex < currentBufferLength) {
        processor.accumulate(OverWindowFrame.getNextOrNull(inputIterator))
      } else {
        //reset the default based current row
        processor.retract(current)
      }
      inputIndex += 1
    }
    processor.getValue
  }
}


