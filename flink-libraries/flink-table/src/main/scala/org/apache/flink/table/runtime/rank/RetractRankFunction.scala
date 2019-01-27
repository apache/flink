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
package org.apache.flink.table.runtime.rank

import java.lang.Long
import java.util
import java.util.{List => JList}
import org.apache.calcite.sql.SqlKind
import org.apache.flink.api.common.functions.Comparator
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.{ListTypeInfo, SortedMapTypeInfo}
import org.apache.flink.runtime.state.keyed.{KeyedMapState, KeyedValueState}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.{Compiler, GeneratedSorter}
import org.apache.flink.table.plan.util.RankRange
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.dataformat.util.BaseRowUtil
import org.apache.flink.table.runtime.aggregate.LazyBaseRowComparator
import org.apache.flink.table.runtime.functions.ExecutionContext
import org.apache.flink.table.runtime.functions.ProcessFunction.Context
import org.apache.flink.table.runtime.sort.RecordComparator
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.{Logging, StateUtil}
import org.apache.flink.util.Collector

class RetractRankFunction(
    inputRowType: BaseRowTypeInfo,
    sortKeyType: BaseRowTypeInfo,
    var gSorter: GeneratedSorter,
    sortKeySelector: KeySelector[BaseRow, BaseRow],
    outputArity: Int,
    rankKind: SqlKind,
    rankRange: RankRange,
    generateRetraction: Boolean,
    tableConfig: TableConfig)
  extends AbstractRankFunction(
    tableConfig,
    rankRange,
    inputRowType,
    inputRowType.getArity,
    outputArity,
    generateRetraction)
  with Compiler[RecordComparator]
  with Logging {

  // flag to skip records with non-exist error instead to fail, true by default.
  private val lenient: Boolean = true

  @transient
  // a map state stores mapping from sort key to records list
  private var dataState: KeyedMapState[BaseRow, BaseRow, JList[BaseRow]] = _

  @transient
  // a sorted map stores mapping from sort key to records count
  private var treeMap: KeyedValueState[BaseRow, util.SortedMap[BaseRow, Long]] = _

  private val sortKeyComparator: Comparator[BaseRow] = new LazyBaseRowComparator(
    gSorter.comparator.name, gSorter.comparator.code, gSorter.serializers, gSorter.comparators)

  override def open(ctx: ExecutionContext): Unit = {
    super.open(ctx)
    val valueTypeInfo = new ListTypeInfo[BaseRow](
        inputRowType.asInstanceOf[BaseRowTypeInfo])
    val mapStateDescriptor = new MapStateDescriptor[BaseRow, JList[BaseRow]](
      "data-state",
      new BaseRowTypeInfo(sortKeyType.getFieldTypes: _*)
        .asInstanceOf[BaseRowTypeInfo],
      valueTypeInfo)
    dataState = ctx.getKeyedMapState(mapStateDescriptor)

    val valueStateDescriptor = new ValueStateDescriptor[util.SortedMap[BaseRow, Long]](
      "sorted-map",
      new SortedMapTypeInfo(
        new BaseRowTypeInfo(sortKeyType.getFieldTypes: _*)
          .asInstanceOf[BaseRowTypeInfo],
        BasicTypeInfo.LONG_TYPE_INFO,
        sortKeyComparator))
    treeMap = ctx.getKeyedValueState(valueStateDescriptor)

    gSorter = null
  }

  override def processElement(
    inputBaseRow: BaseRow,
    ctx: Context,
    out: Collector[BaseRow]): Unit = {

    initRankEnd(inputBaseRow)

    val currentKey = executionContext.currentKey()
    var sortedMap = treeMap.get(currentKey)
    if (sortedMap == null) {
      sortedMap = new util.TreeMap[BaseRow, Long](sortKeyComparator)
    }

    val sortKey = sortKeySelector.getKey(inputBaseRow)

    if (BaseRowUtil.isAccumulateMsg(inputBaseRow)) {
      // update sortedMap
      if (sortedMap.containsKey(sortKey)) {
        sortedMap.put(sortKey, sortedMap.get(sortKey) + 1)
      } else {
        sortedMap.put(sortKey, 1L)
      }

      // emit
      rankKind match {
        case SqlKind.ROW_NUMBER =>
          emitRecordsWithRowNumber(sortedMap, sortKey, inputBaseRow, out)

        case SqlKind.RANK => ???
        case SqlKind.DENSE_RANK => ???
      }

      // update data state
      var inputs = dataState.get(currentKey, sortKey)
      if (inputs == null) {
        // the sort key is never seen
        inputs = new util.ArrayList[BaseRow]()
      }
      inputs.add(inputBaseRow)
      dataState.add(currentKey, sortKey, inputs)
    }
    // retract input
    else {

      // emit updates first
      rankKind match {
        case SqlKind.ROW_NUMBER =>
          retractRecordWithRowNumber(sortedMap, sortKey, inputBaseRow, out)

        case SqlKind.RANK => ???
        case SqlKind.DENSE_RANK => ???
      }

      // and then update sortedMap
      if (sortedMap.containsKey(sortKey)) {
        val count = sortedMap.get(sortKey) - 1
        if (count == 0) {
          sortedMap.remove(sortKey)
        } else {
          sortedMap.put(sortKey, count)
        }
      } else {
        if (sortedMap.isEmpty) {
          if (lenient) {
            LOG.warn(StateUtil.STATE_CLEARED_WARN_MSG)
          } else {
            throw new RuntimeException(StateUtil.STATE_CLEARED_WARN_MSG)
          }
        } else {
          throw new RuntimeException(
            s"Can not retract a non-existent record: ${inputBaseRow.toString}. " +
              s"This should never happen.")
        }
      }

      // we have updated the data state in retractRecordWithRowNumber(...)
    }

    treeMap.put(currentKey, sortedMap)
  }

  // ------------- ROW_NUMBER-------------------------------

  private def retractRecordWithRowNumber(
    sortedMap: util.SortedMap[BaseRow, Long],
    sortKey: BaseRow,
    inputRow: BaseRow,
    out: Collector[BaseRow]): Unit = {

    val iterator = sortedMap.entrySet().iterator()
    var curRank = 0L
    var needUpdate = false
    val currentKey = executionContext.currentKey()
    while (iterator.hasNext && isInRankEnd(curRank)) {
      val entry = iterator.next()
      val key = entry.getKey
      if (!needUpdate && key.equals(sortKey)) {
        val inputs = dataState.get(currentKey, key)
        if (inputs == null) {
          // Skip the data if it's state is cleared because of state ttl.
          if (lenient) {
            LOG.warn(StateUtil.STATE_CLEARED_WARN_MSG)
          } else {
            throw new RuntimeException(StateUtil.STATE_CLEARED_WARN_MSG)
          }
        } else {
          val inputIter = inputs.iterator()
          while (inputIter.hasNext && isInRankEnd(curRank)) {
            curRank += 1
            val prevRow = inputIter.next()
            if (!needUpdate && equaliser.equalsWithoutHeader(prevRow, inputRow)) {
              delete(out, prevRow, curRank)
              curRank -= 1
              needUpdate = true
              inputIter.remove()
            } else if (needUpdate) {
              retract(out, prevRow, curRank + 1)
              collect(out, prevRow, curRank)
            }
          }
          if (inputs.isEmpty) {
            dataState.remove(currentKey, key)
          } else {
            dataState.add(currentKey, key, inputs)
          }
        }
      } else if (needUpdate) {
        val inputs = dataState.get(currentKey, entry.getKey)
        var i = 0
        while (i < inputs.size() && isInRankEnd(curRank)) {
          curRank += 1
          val prevRow = inputs.get(i)
          retract(out, prevRow, curRank + 1)
          collect(out, prevRow, curRank)
          i += 1
        }
      } else {
        curRank += entry.getValue
      }
    }

  }

  private def emitRecordsWithRowNumber(
      sortedMap: util.SortedMap[BaseRow, Long],
      sortKey: BaseRow,
      inputRow: BaseRow,
      out: Collector[BaseRow]): Unit = {

    val iterator = sortedMap.entrySet().iterator()
    var curRank = 0L
    var needUpdate = false
    val currentKey = executionContext.currentKey()
    while (iterator.hasNext && isInRankEnd(curRank)) {
      val entry = iterator.next()
      if (!needUpdate && entry.getKey.equals(sortKey)) {
        curRank += entry.getValue
        collect(out, inputRow, curRank)
        needUpdate = true
      } else if (needUpdate) {
        val inputs = dataState.get(currentKey, entry.getKey)
        if (inputs == null) {
          // Skip the data if it's state is cleared because of state ttl.
          if (lenient) {
            LOG.warn(StateUtil.STATE_CLEARED_WARN_MSG)
          } else {
            throw new RuntimeException(StateUtil.STATE_CLEARED_WARN_MSG)
          }
        } else {
          var i = 0
          while (i < inputs.size() && isInRankEnd(curRank)) {
            curRank += 1
            val prevRow = inputs.get(i)
            retract(out, prevRow, curRank - 1)
            collect(out, prevRow, curRank)
            i += 1
          }
        }
      } else {
        curRank += entry.getValue
      }
    }
  }

  // just let it go, retract rank has no interest in this
  override def getMaxSortMapSize: scala.Long = 0L

}
