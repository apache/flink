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

import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.runtime.state.keyed.KeyedValueState
import org.apache.flink.table.api.types.{DataTypes, InternalType, RowType, TypeConverters}
import org.apache.flink.table.codegen.{EqualiserCodeGenerator, GeneratedAggsHandleFunction}
import org.apache.flink.table.dataformat.util.{BaseRowUtil, BinaryRowUtil}
import org.apache.flink.table.dataformat.{BaseRow, JoinedRow}
import org.apache.flink.table.runtime.functions.{AggsHandleFunction, BundleFunction, ExecutionContext}
import org.apache.flink.table.runtime.sort.RecordEqualiser
import org.apache.flink.table.typeutils.{AbstractRowSerializer, BaseRowTypeInfo, TypeUtils}
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Collector

/**
  * Class of Aggregate Function used for the groupby (without window) aggregate in miniBatch mode.
  *
  * @param genAggsHandler  the generated aggregate handler
  * @param accTypes        the accumulator types
  * @param aggValueTypes   the aggregate value types
  * @param inputCountIndex None when the input not contains retraction.
  *                        Some when input contains retraction and
  *                        the index represents the count1 agg index.
  * @param generateRetraction whether this operator will generate retraction
  */
class MiniBatchGroupAggFunction(
    inputType: RowType,
    genAggsHandler: GeneratedAggsHandleFunction,
    accTypes: Array[InternalType],
    aggValueTypes: Array[InternalType],
    inputCountIndex: Option[Int],
    generateRetraction: Boolean,
    groupWithoutKey: Boolean)
  extends BundleFunction[BaseRow, JList[BaseRow], BaseRow, BaseRow]
  with Logging {

  protected var function: AggsHandleFunction = _

  // stores the accumulators
  protected var accState: KeyedValueState[BaseRow, BaseRow] = _

  private val inputCounter: InputCounter = InputCounter(inputCountIndex)

  protected var newAggValue: BaseRow = _
  protected var prevAggValue: BaseRow = _

  protected var resultRow: JoinedRow = _

  @transient
  private var equaliser: RecordEqualiser = _

  private val inputSer = DataTypes.createInternalSerializer(inputType)
      .asInstanceOf[AbstractRowSerializer[BaseRow]]

  override def open(ctx: ExecutionContext): Unit = {
    super.open(ctx)
    LOG.debug(s"Compiling AggsHandleFunction: ${genAggsHandler.name} \n\n " +
                s"Code:\n${genAggsHandler.code}")
    function = genAggsHandler.newInstance(ctx.getRuntimeContext.getUserCodeClassLoader)
    function.open(ctx)

    // serialize as GenericRow, deserialize as BinaryRow
    val accTypeInfo = new BaseRowTypeInfo(
      accTypes.map(TypeConverters.createExternalTypeInfoFromDataType): _*)
    val accDesc = new ValueStateDescriptor("accState", accTypeInfo)
    accState = ctx.getKeyedValueState(accDesc)

    val generator = new EqualiserCodeGenerator(aggValueTypes)
    val generatedEqualiser = generator.generateRecordEqualiser("GroupAggValueEqualiser")
    equaliser = generatedEqualiser.newInstance(ctx.getRuntimeContext.getUserCodeClassLoader)

    resultRow = new JoinedRow()
  }

  override def close(): Unit = {
    if (function != null) {
      function.close()
    }
  }

  // value is list of inputs
  override def addInput(value: JList[BaseRow], input: BaseRow): JList[BaseRow] = {
    val acc = if (value == null) {
      new JArrayList[BaseRow]()
    } else {
      value
    }
    acc.add(inputSer.copy(input))
    acc
  }

  override def finishBundle(
    buffer: JMap[BaseRow, JList[BaseRow]],
    out: Collector[BaseRow]): Unit = {

    val accMap = accState.getAll(buffer.keySet())
    val accResult = new JHashMap[BaseRow, BaseRow]()

    val iter = buffer.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val elements = entry.getValue
      val currentKey = entry.getKey
      // set current key to make dataview know current key
      ctx.setCurrentKey(currentKey)

      var firstRow = false

      // step 1: get the acc for the current key
      var acc = accMap.get(currentKey)
      if (acc == null) {
        acc = function.createAccumulators()
        firstRow = true
      }

      // step 2: accumulate
      function.setAccumulators(acc)

      // get previous aggregate result
      prevAggValue = function.getValue

      val elementIter = elements.iterator()
      while (elementIter.hasNext) {
        val input = elementIter.next()
        if (BaseRowUtil.isAccumulateMsg(input)) {
          function.accumulate(input)
        } else {
          function.retract(input)
        }
      }

      // get current aggregate result
      newAggValue = function.getValue

      // get accumulator
      acc = function.getAccumulators

      if (!inputCounter.countIsZero(acc)) {
        // we aggregated at least one record for this key

        // buffer acc result and cnt result to map, call putAll later
        accResult.put(currentKey, acc)

        // if this was not the first row and we have to emit retractions
        if (!firstRow) {
          if (!equaliser.equalsWithoutHeader(prevAggValue, newAggValue)) {
            // new row is not same with prev row
            if (generateRetraction) {
              out.collect(prevResultRow(currentKey))
            }
            out.collect(newResultRow(currentKey))
          } else {
            // new row is same with prev row, no need to output
          }
        } else {
          // this is the first, output new result
          out.collect(newResultRow(currentKey))
        }

      } else {
        // we retracted the last record for this key
        // if this is not first row sent out a delete message
        if (!firstRow) {
          out.collect(prevResultRow(currentKey))
        }
        // and clear all state, remove directly not removeAll because removeAll calls remove
        accState.remove(currentKey)
        // cleanup dataview under current key
        function.cleanup()
      }
    }

    // batch update to state
    if (!accResult.isEmpty) {
      accState.putAll(accResult)
    }
  }

  protected def newResultRow(key: BaseRow): BaseRow = {
    resultRow.replace(key, newAggValue)
    BaseRowUtil.setAccumulate(resultRow)
    resultRow
  }

  protected def prevResultRow(key: BaseRow): BaseRow = {
    resultRow.replace(key, prevAggValue)
    BaseRowUtil.setRetract(resultRow)
    resultRow
  }

  override def endInput(out: Collector[BaseRow]): Unit = {
    // output default value if grouping without key and it's an empty group
    if (groupWithoutKey) {
      ctx.setCurrentKey(BinaryRowUtil.EMPTY_ROW)
      val accumulators = accState.get(BinaryRowUtil.EMPTY_ROW)
      if (inputCounter.countIsZero(accumulators)) {
        function.setAccumulators(function.createAccumulators)
        newAggValue = function.getValue
        out.collect(newResultRow(BinaryRowUtil.EMPTY_ROW))
      }
      // no need to update acc state, because this is the end
    }
  }
}
