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

import java.util.function.Supplier
import java.util.{Comparator, ArrayList => JArrayList, Collection => JCollection, List => JList, Map => JMap}

import org.apache.calcite.sql.SqlKind
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.runtime.state.keyed.KeyedMapState
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.{Compiler, GeneratedSorter}
import org.apache.flink.table.plan.util.RankRange
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.aggregate.CollectionBaseRowComparator
import org.apache.flink.table.runtime.functions.ExecutionContext
import org.apache.flink.table.runtime.functions.ProcessFunction.{Context, OnTimerContext}
import org.apache.flink.table.runtime.sort.RecordComparator
import org.apache.flink.table.typeutils.{AbstractRowSerializer, BaseRowTypeInfo}
import org.apache.flink.table.util.{LRUMap, Logging}
import org.apache.flink.util.Collector

/**
  * A fast version of rank process function which only hold top n data in state,
  * and keep sorted map in heap. This only works in some special scenarios, such
  * as, rank an append input stream
  */
class AppendRankFunction(
    inputRowType: BaseRowTypeInfo,
    sortKeyType: BaseRowTypeInfo,
    gSorter: GeneratedSorter,
    sortKeySelector: KeySelector[BaseRow, BaseRow],
    outputArity: Int,
    rankKind: SqlKind,
    rankRange: RankRange,
    cacheSize: Long,
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

  private val inputRowSer =
    inputRowType.createSerializer().asInstanceOf[AbstractRowSerializer[BaseRow]]

  @transient
  // a map state stores mapping from sort key to records list which is in topN
  private var dataState: KeyedMapState[BaseRow, BaseRow, JList[BaseRow]] = _

  @transient
  // a sorted map stores mapping from sort key to records list, a heap mirror to dataState
  protected var sortedMap: SortedMap[BaseRow] = _
  @transient
  private var kvSortedMap: JMap[BaseRow, SortedMap[BaseRow]] = _

  private var sortKeyComparator: Comparator[BaseRow] = _

  override def open(ctx: ExecutionContext): Unit = {
    super.open(ctx)
    val lruCacheSize: Int = Math.max(1, (cacheSize / getDefaultTopSize).toInt)
    kvSortedMap = new LRUMap[BaseRow, SortedMap[BaseRow]](lruCacheSize)
    LOG.info("Top{} operator is using LRU caches key-size: {}", getDefaultTopSize, lruCacheSize)

    val valueTypeInfo = new ListTypeInfo[BaseRow](
      inputRowType.asInstanceOf[BaseRowTypeInfo])
    val mapStateDescriptor = new MapStateDescriptor[BaseRow, JList[BaseRow]](
      "data-state-with-append",
      new BaseRowTypeInfo(sortKeyType.getFieldTypes: _*)
        .asInstanceOf[BaseRowTypeInfo],
      valueTypeInfo)
    dataState = ctx.getKeyedMapState(mapStateDescriptor)

    val name = gSorter.comparator.name
    val code = gSorter.comparator.code
    LOG.debug(s"Compiling Sorter: $name \n\n Code:\n$code")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, name, code)
    gSorter.comparator.code = null
    LOG.debug("Instantiating Sorter.")
    val comparator = clazz.newInstance()
    comparator.init(gSorter.serializers, gSorter.comparators)
    sortKeyComparator = new CollectionBaseRowComparator(comparator)

    // metrics
    registerMetric(kvSortedMap.size() * getDefaultTopSize)
  }

  def initHeapStates(): Unit = {
    requestCount += 1
    val currentKey = executionContext.currentKey()
    sortedMap = kvSortedMap.get(currentKey)
    if (sortedMap == null) {
      sortedMap = new SortedMap(sortKeyComparator, ArrayListSupplier)
      kvSortedMap.put(currentKey, sortedMap)
      // restore sorted map
      val iter = dataState.iterator(currentKey)
      if (iter != null) {
        while (iter.hasNext) {
          val entry = iter.next()
          val sortKey = entry.getKey
          val values = entry.getValue
          // the order is preserved
          sortedMap.putAll(sortKey, values)
        }
      }
    } else {
      hitCount += 1
    }
  }

  override def processElement(
    inputBaseRow: BaseRow,
    context: Context,
    out: Collector[BaseRow]): Unit = {

    val currentKey = executionContext.currentKey()
    val currentTime = context.timerService().currentProcessingTime()
    // register state-cleanup timer
    registerProcessingCleanupTimer(context, currentTime)

    initHeapStates()
    initRankEnd(inputBaseRow)

    val sortKey = sortKeySelector.getKey(inputBaseRow)
    // check whether the sortKey is in the topN range
    if (checkSortKeyInBufferRange(sortKey, sortedMap, sortKeyComparator)) {
      // insert sort key into sortedMap
      sortedMap.put(sortKey, inputRowSer.copy(inputBaseRow))
      val inputs = sortedMap.get(sortKey)
      // update data state
      dataState.add(currentKey, sortKey, inputs.asInstanceOf[JList[BaseRow]])
      if (isRowNumberAppend || hasOffset) {
        // the without-number-algorithm can't handle topn with offset,
        // so use the with-number-algorithm to handle offset
        processElementWithRowNumber(inputBaseRow, sortKey, out)
      } else {
        processElementWithoutRowNumber(inputBaseRow, out)
      }
    }
  }

  private def processElementWithRowNumber(
      inputRow: BaseRow,
      sortKey: BaseRow,
      out: Collector[BaseRow]): Unit = {
    // emit records
    rankKind match {
      case SqlKind.ROW_NUMBER =>
        emitRecordsWithRowNumber(sortKey, inputRow, out)

      case _ => ???
    }
  }

  private def processElementWithoutRowNumber(inputRow: BaseRow, out: Collector[BaseRow]): Unit = {
    val currentKey = executionContext.currentKey()
    collect(out, inputRow)
    // remove retired element
    if (sortedMap.currentTopNum > rankEnd) {
      val lastEntry = sortedMap.lastEntry()
      val lastKey = lastEntry.getKey
      val lastList = lastEntry.getValue.asInstanceOf[JList[BaseRow]]
      // remove last one
      val lastElement = lastList.remove(lastList.size() - 1)
      if (lastList.isEmpty) {
        sortedMap.removeAll(lastKey)
        dataState.remove(currentKey, lastKey)
      } else {
        dataState.add(currentKey, lastKey, lastList)
      }
      // lastElement shouldn't be null
      delete(out, lastElement)
    }
  }

  override def onTimer(
    timestamp: Long,
    ctx: OnTimerContext,
    out: Collector[BaseRow]): Unit = {
    if (needToCleanupState(timestamp)) {
      // cleanup cache
      kvSortedMap.remove(executionContext.currentKey())
      cleanupState(dataState)
    }
  }

  /**
    * emit records whose rank is changed, and return the sort key list which is out of topN
    * @param sortKey the input sort key
    * @param inputRow the input row
    * @param out the output collector
    * @return the sort key list which is out of topN
    */
  def emitRecordsWithRowNumber(
    sortKey: BaseRow,
    inputRow: BaseRow,
    out: Collector[BaseRow]): Unit = {

    val iterator = sortedMap.entrySet().iterator()
    var curRank = 0
    var findSortKey = false
    while (iterator.hasNext && isInRankEnd(curRank)) {
      val entry = iterator.next()
      val records = entry.getValue
      // meet its own sort key
      if (!findSortKey && entry.getKey.equals(sortKey)) {
        curRank += records.size()
        collect(out, inputRow, curRank)
        findSortKey = true
      } else if (findSortKey) {
        val recordsIter = records.iterator()
        while (recordsIter.hasNext && isInRankEnd(curRank)) {
          curRank += 1
          val prevRow = recordsIter.next()
          retract(out, prevRow, curRank - 1)
          collect(out, prevRow, curRank)
        }
      } else {
        curRank += records.size()
      }
    }

    // remove the records associated to the sort key which is out of topN
    val currentKey = executionContext.currentKey()
    while (iterator.hasNext) {
      val entry = iterator.next()
      val sortKey = entry.getKey
      dataState.remove(currentKey, sortKey)
      sortedMap.currentTopNum -= entry.getValue.size()
      iterator.remove()
    }

  }

  override def getMaxSortMapSize: Long = getDefaultTopSize

  private object ArrayListSupplier extends Supplier[JCollection[BaseRow]] {
    override def get(): JCollection[BaseRow] = new JArrayList[BaseRow]()
  }

}
