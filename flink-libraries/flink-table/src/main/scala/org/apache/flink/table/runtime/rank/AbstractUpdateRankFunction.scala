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

import java.lang.{Integer => JInt}
import java.util
import java.util.function.Supplier
import java.util.{Comparator, Collection => JCollection, HashMap => JHashMap, Map => JMap}

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.runtime.state.keyed.KeyedMapState
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.{Compiler, GeneratedSorter}
import org.apache.flink.table.plan.util.RankRange
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.aggregate.CollectionBaseRowComparator
import org.apache.flink.table.runtime.functions.ExecutionContext
import org.apache.flink.table.runtime.functions.ProcessFunction.OnTimerContext
import org.apache.flink.table.runtime.sort.RecordComparator
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.LRUMap.RemovalListener
import org.apache.flink.table.util.{LRUMap, Logging}
import org.apache.flink.util.Collector

abstract class AbstractUpdateRankFunction(
    inputRowType: BaseRowTypeInfo,
    rowKeyType: BaseRowTypeInfo,
    gSorter: GeneratedSorter,
    sortKeySelector: KeySelector[BaseRow, BaseRow],
    outputArity: Int,
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
  with CheckpointedFunction
  with Compiler[RecordComparator]
  with Logging {

  @transient
  // a map state stores mapping from row key to record which is in topN
  // in tuple2, f0 is the record row, f1 is the index in the list of the same sort_key
  // the f1 is used to preserve the record order in the same sort_key
  protected var dataState: KeyedMapState[BaseRow, BaseRow, Tuple2[BaseRow, JInt]] = _

  @transient
  // a sorted map stores mapping from sort key to rowkey list
  protected var sortedMap: SortedMap[BaseRow] = _
  @transient
  protected var kvSortedMap: JMap[BaseRow, SortedMap[BaseRow]] = _

  @transient
  // a HashMap stores mapping from rowkey to record, a heap mirror to dataState
  protected var rowKeyMap: JMap[BaseRow, RankRow] = _
  @transient
  protected var kvRowKeyMap: LRUMap[BaseRow, JMap[BaseRow, RankRow]] = _

  protected var sortKeyComparator: Comparator[BaseRow] = _

  override def open(ctx: ExecutionContext): Unit = {
    super.open(ctx)
    val lruCacheSize: Int = Math.max(1, (cacheSize / getMaxSortMapSize).toInt)
    // make sure the cached map is in a fixed size, avoid OOM
    kvSortedMap = new JHashMap[BaseRow, SortedMap[BaseRow]](
      lruCacheSize)
    kvRowKeyMap = new LRUMap[BaseRow, JMap[BaseRow, RankRow]](
      lruCacheSize,
      new CacheRemovalListener())

    LOG.info("Top{} operator is using LRU caches key-size: {}", getMaxSortMapSize, lruCacheSize)

    val valueTypeInfo = new TupleTypeInfo[Tuple2[BaseRow, JInt]](inputRowType, Types.INT)
    val mapStateDescriptor = new MapStateDescriptor[BaseRow, Tuple2[BaseRow, JInt]](
      "data-state-with-update",
      new BaseRowTypeInfo(rowKeyType.getFieldTypes: _*)
        .asInstanceOf[BaseRowTypeInfo],
      valueTypeInfo)
    dataState = ctx.getKeyedMapState(mapStateDescriptor)

    // metrics
    registerMetric(kvSortedMap.size() * getMaxSortMapSize)

    val name = gSorter.comparator.name
    val code = gSorter.comparator.code
    LOG.debug(s"Compiling Sorter: $name \n\n Code:\n$code")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, name, code)
    gSorter.comparator.code = null
    LOG.debug("Instantiating Sorter.")
    val comparator = clazz.newInstance()
    comparator.init(gSorter.serializers, gSorter.comparators)
    sortKeyComparator = new CollectionBaseRowComparator(comparator)
  }

  def initHeapStates(): Unit = {
    requestCount += 1
    val partitionKey = executionContext.currentKey()
    sortedMap = kvSortedMap.get(partitionKey)
    rowKeyMap = kvRowKeyMap.get(partitionKey)
    if (sortedMap == null) {
      sortedMap = new SortedMap(sortKeyComparator, LinkedHashSetSupplier)
      rowKeyMap = new JHashMap[BaseRow, RankRow]()
      kvSortedMap.put(partitionKey, sortedMap)
      kvRowKeyMap.put(partitionKey, rowKeyMap)

      // restore sorted map
      val iter = dataState.iterator(partitionKey)
      if (iter != null) {
        // a temp map associate sort key to tuple2<index, record>
        val tempSortedMap = new JHashMap[BaseRow, util.TreeMap[Int, BaseRow]]()
        while (iter.hasNext) {
          val entry = iter.next()
          val rowkey = entry.getKey
          val recordAndInnerRank = entry.getValue
          val record = recordAndInnerRank.f0
          val innerRank = recordAndInnerRank.f1
          rowKeyMap.put(rowkey, RankRow(record, innerRank, dirty = false))

          // insert into temp sort map to preserve the record order in the same sort key
          val sortKey = sortKeySelector.getKey(record)
          var treeMap = tempSortedMap.get(sortKey)
          if (treeMap == null) {
            treeMap = new util.TreeMap[Int, BaseRow]()
            tempSortedMap.put(sortKey, treeMap)
          }
          treeMap.put(innerRank, rowkey)
        }

        // build sorted map from the temp map
        val tempIter = tempSortedMap.entrySet().iterator()
        while (tempIter.hasNext) {
          val entry = tempIter.next()
          val sortKey = entry.getKey
          val treeMap = entry.getValue
          val treeMapIter = treeMap.entrySet().iterator()
          while (treeMapIter.hasNext) {
            val treeMapEntry = treeMapIter.next()
            val innerRank = treeMapEntry.getKey
            val recordRowKey = treeMapEntry.getValue
            val size = sortedMap.put(sortKey, recordRowKey)
            if (innerRank != size) {
              LOG.warn("Failed to build sorted map from state, this may result in wrong result." +
                s" The sort key is $sortKey, partition key is $partitionKey," +
                s" treeMap is $treeMap. The expected inner rank is $innerRank," +
                s" but current size is $size")
            }
          }
        }
      }
    } else {
      hitCount += 1
    }
  }

  def updateInnerRank(oldSortKey: BaseRow): Unit = {
    val list = sortedMap.get(oldSortKey)
    if (list != null) {
      val iter = list.iterator()
      var innerRank = 1
      while (iter.hasNext) {
        val rowkey = iter.next()
        val row = rowKeyMap.get(rowkey)
        if (row.innerRank != innerRank) {
          row.innerRank = innerRank
          row.dirty = true
        }
        innerRank += 1
      }
    }
  }

  override def onTimer(
    timestamp: Long,
    ctx: OnTimerContext,
    out: Collector[BaseRow]): Unit = {

    if (needToCleanupState(timestamp)) {
      val partitionKey = executionContext.currentKey()
      // cleanup cache
      kvRowKeyMap.remove(partitionKey)
      kvSortedMap.remove(partitionKey)
      cleanupState(dataState)
    }
  }

  override def snapshotState(ctx: FunctionSnapshotContext): Unit = {
    val iter = kvRowKeyMap.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val partitionKey = entry.getKey
      val currentRowKeyMap = entry.getValue
      executionContext.setCurrentKey(partitionKey)
      synchronizeState(currentRowKeyMap)
    }
  }

  def synchronizeState(curRowKeyMap: JMap[BaseRow, RankRow]): Unit = {
    val currentKey = executionContext.currentKey()
    val iter = curRowKeyMap.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val key = entry.getKey
      val rankRow = entry.getValue
      if (rankRow.dirty) {
        // should update state
        dataState.add(currentKey, key, Tuple2.of(rankRow.row, rankRow.innerRank))
        rankRow.dirty = false
      }
    }
  }

  override def initializeState(ctx: FunctionInitializationContext): Unit = {
    // nothing to do
  }

  protected object LinkedHashSetSupplier extends Supplier[JCollection[BaseRow]] {
    override def get(): JCollection[BaseRow] = new util.LinkedHashSet[BaseRow]()
  }

  protected class CacheRemovalListener extends RemovalListener[BaseRow, JMap[BaseRow, RankRow]] {

    override def onRemoval(eldest: JMap.Entry[BaseRow, JMap[BaseRow, RankRow]]): Unit = {
      val previousKey = executionContext.currentKey()
      val partitionKey = eldest.getKey
      val currentRowKeyMap = eldest.getValue
      executionContext.setCurrentKey(partitionKey)
      kvSortedMap.remove(partitionKey)
      synchronizeState(currentRowKeyMap)
      executionContext.setCurrentKey(previousKey)
    }
  }

  class RankRow(var row: BaseRow, var innerRank: Int, var dirty: Boolean)

  object RankRow {
    def apply(row: BaseRow, innerRank: Int, dirty: Boolean): RankRow = {
      new RankRow(row, innerRank, dirty)
    }
  }

}
