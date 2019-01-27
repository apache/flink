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

import org.apache.calcite.sql.SqlKind
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.GeneratedSorter
import org.apache.flink.table.plan.util.RankRange
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.functions.ProcessFunction.Context
import org.apache.flink.table.typeutils.{AbstractRowSerializer, BaseRowTypeInfo}
import org.apache.flink.util.Collector

/**
  * A fast version of rank process function which only hold top n data in state,
  * and keep sorted map in heap. This only works in some special scenarios, such
  * as, rank a count(*) stream
  */
class UpdateRankFunction(
    inputRowType: BaseRowTypeInfo,
    rowKeyType: BaseRowTypeInfo,
    rowKeySelector: KeySelector[BaseRow, BaseRow],
    gSorter: GeneratedSorter,
    sortKeySelector: KeySelector[BaseRow, BaseRow],
    outputArity: Int,
    rankKind: SqlKind,
    rankRange: RankRange,
    cacheSize: Long,
    generateRetraction: Boolean,
    tableConfig: TableConfig)
  extends AbstractUpdateRankFunction(
    inputRowType,
    rowKeyType,
    gSorter,
    sortKeySelector,
    outputArity,
    rankRange,
    cacheSize,
    generateRetraction,
    tableConfig) {

  private val inputRowSer =
    inputRowType.createSerializer().asInstanceOf[AbstractRowSerializer[BaseRow]]

  override def processElement(
    inputBaseRow: BaseRow,
    context: Context,
    out: Collector[BaseRow]): Unit = {

    val currentTime = context.timerService().currentProcessingTime()
    // register state-cleanup timer
    registerProcessingCleanupTimer(context, currentTime)

    initHeapStates()
    initRankEnd(inputBaseRow)

    if (isRowNumberAppend || hasOffset) {
      // the without-number-algorithm can't handle topn with offset,
      // so use the with-number-algorithm to handle offset
      processElementWithRowNumber(inputBaseRow, out)
    } else {
      processElementWithoutRowNumber(inputBaseRow, out)
    }
  }

  private def processElementWithRowNumber(inputRow: BaseRow, out: Collector[BaseRow]): Unit = {
    val sortKey = sortKeySelector.getKey(inputRow)
    val rowKey = rowKeySelector.getKey(inputRow)
    if (rowKeyMap.containsKey(rowKey)) {
      // it is an updated record which is in the topN, in this scenario,
      // the new sort key must be higher than old sort key, this is guaranteed by rules
      val oldRow = rowKeyMap.get(rowKey)
      val oldSortKey = sortKeySelector.getKey(oldRow.row)
      if (oldSortKey.equals(sortKey)) {
        rankKind match {
          case SqlKind.ROW_NUMBER =>
            // sort key is not changed, so the rank is the same, only output the row
            val (rank, innerRank) = rowNumber(sortKey, rowKey, sortedMap)
            rowKeyMap.put(rowKey, RankRow(inputRowSer.copy(inputRow), innerRank, dirty = true))
            retract(out, oldRow.row, rank) // retract old record
            collect(out, inputRow, rank)

          case _ => ???
        }
        return
      }

      val (oldRank, _) = rowNumber(oldSortKey, rowKey, sortedMap)
      // remove old sort key
      sortedMap.remove(oldSortKey, rowKey)
      // add new sort key
      val size = sortedMap.put(sortKey, rowKey)
      rowKeyMap.put(rowKey, RankRow(inputRowSer.copy(inputRow), size, dirty = true))
      // update inner rank of records under the old sort key
      updateInnerRank(oldSortKey)

      // emit records
      rankKind match {
        case SqlKind.ROW_NUMBER =>
          emitRecordsWithRowNumber(sortKey, inputRow, out, oldSortKey, oldRow, oldRank)

        case _ => ???
      }
    } else if (checkSortKeyInBufferRange(sortKey, sortedMap, sortKeyComparator)) {
      // it is an unique record but is in the topN
      // insert sort key into sortedMap
      val size = sortedMap.put(sortKey, rowKey)
      rowKeyMap.put(rowKey, RankRow(inputRowSer.copy(inputRow), size, dirty = true))

      // emit records
      rankKind match {
        case SqlKind.ROW_NUMBER =>
          emitRecordsWithRowNumber(sortKey, inputRow, out)

        case _ => ???
      }
    } else {
      // out of topN
    }
  }

  private def processElementWithoutRowNumber(
      inputRow: BaseRow,
      out: Collector[BaseRow]): Unit = {
    val sortKey = sortKeySelector.getKey(inputRow)
    val rowKey = rowKeySelector.getKey(inputRow)
    if (rowKeyMap.containsKey(rowKey)) {
      // it is an updated record which is in the topN, in this scenario,
      // the new sort key must be higher than old sort key, this is guaranteed by rules
      val oldRow = rowKeyMap.get(rowKey)
      val oldSortKey = sortKeySelector.getKey(oldRow.row)
      if (!oldSortKey.equals(sortKey)) {
        // remove old sort key
        sortedMap.remove(oldSortKey, rowKey)
        // add new sort key
        val size = sortedMap.put(sortKey, rowKey)
        rowKeyMap.put(rowKey, RankRow(inputRowSer.copy(inputRow), size, dirty = true))
        // update inner rank of records under the old sort key
        updateInnerRank(oldSortKey)
      } else {
        // row content may change, so we need to update row in map
        rowKeyMap.put(rowKey, RankRow(inputRowSer.copy(inputRow), oldRow.innerRank, dirty = true))
      }
      // row content may change, so a retract is needed
      retract(out, oldRow.row, oldRow.innerRank)
      collect(out, inputRow)
    } else if (checkSortKeyInBufferRange(sortKey, sortedMap, sortKeyComparator)) {
      // it is an unique record but is in the topN
      // insert sort key into sortedMap
      val size = sortedMap.put(sortKey, rowKey)
      rowKeyMap.put(rowKey, RankRow(inputRowSer.copy(inputRow), size, dirty = true))
      collect(out, inputRow)
      // remove retired element
      if (sortedMap.getCurrentTopNum > rankEnd) {
        val lastRowKey = sortedMap.removeLast()
        if (lastRowKey != null) {
          val lastRow = rowKeyMap.remove(lastRowKey)
          dataState.remove(executionContext.currentKey(), lastRowKey)
          // always send a retraction message
          delete(out, lastRow.row)
        }
      }
    } else {
      // out of topN
    }
  }

  def emitRecordsWithRowNumber(
      sortKey: BaseRow,
      inputRow: BaseRow,
      out: Collector[BaseRow],
      oldSortKey: BaseRow = null,
      oldRow: RankRow = null,
      oldRank: Int = -1): Unit = {

    val oldInnerRank = if (oldRow == null) -1 else oldRow.innerRank
    val iterator = sortedMap.entrySet().iterator()
    var curRank = 0
    // whether we have found the sort key in the sorted tree
    var findSortKey = false
    while (iterator.hasNext && isInRankEnd(curRank + 1)) {
      val entry = iterator.next()
      val curKey = entry.getKey
      val rowKeys = entry.getValue
      // meet its own sort key
      if (!findSortKey && curKey.equals(sortKey)) {
        curRank += rowKeys.size()
        if (oldRow != null) {
          retract(out, oldRow.row, oldRank)
        }
        collect(out, inputRow, curRank)
        findSortKey = true
      } else if (findSortKey) {
        if (oldSortKey == null) {
          // this is a new row, emit updates for all rows in the topn
          val rowKeyIter = rowKeys.iterator()
          while (rowKeyIter.hasNext && isInRankEnd(curRank + 1)) {
            curRank += 1
            val rowKey = rowKeyIter.next()
            val prevRow = rowKeyMap.get(rowKey)
            retract(out, prevRow.row, curRank - 1)
            collect(out, prevRow.row, curRank)
          }
        } else {
          // current sort key is higher than old sort key,
          // the rank of current record is changed, need to update the following rank
          val compare = sortKeyComparator.compare(curKey, oldSortKey)
          if (compare <= 0) {
            val rowKeyIter = rowKeys.iterator()
            var curInnerRank = 0
            while (rowKeyIter.hasNext && isInRankEnd(curRank + 1)) {
              curRank += 1
              curInnerRank += 1
              if (compare == 0 && curInnerRank >= oldInnerRank) {
                // match to the previous position
                return
              }

              val rowKey = rowKeyIter.next()
              val prevRow = rowKeyMap.get(rowKey)
              retract(out, prevRow.row, curRank - 1)
              collect(out, prevRow.row, curRank)
            }
          } else {
            // current sort key is smaller than old sort key,
            // the rank is not changed, so skip
            return
          }
        }
      } else {
        curRank += rowKeys.size()
      }
    }

    // remove the records associated to the sort key which is out of topN
    while (iterator.hasNext) {
      val entry = iterator.next()
      val rowkeys = entry.getValue
      val rowkeyIter = rowkeys.iterator()
      while (rowkeyIter.hasNext) {
        val rowkey = rowkeyIter.next()
        rowKeyMap.remove(rowkey)
        dataState.remove(executionContext.currentKey(), rowkey)
      }
      sortedMap.currentTopNum -= entry.getValue.size()
      iterator.remove()
    }
  }

  override def getMaxSortMapSize: Long = {
    getDefaultTopSize
  }

}
