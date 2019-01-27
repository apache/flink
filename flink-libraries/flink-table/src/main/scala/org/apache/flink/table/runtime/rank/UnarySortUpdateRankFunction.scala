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

import java.util
import java.util.function.Supplier
import java.util.{ArrayList => JArrayList, Collection => JCollection, HashMap => JHashMap, List => JList, Map => JMap}

import scala.collection.JavaConversions._
import org.apache.calcite.sql.SqlKind
import org.apache.flink.api.common.functions.Comparator
import org.apache.flink.api.common.state.{MapStateDescriptor, SortedMapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.runtime.state.keyed.{KeyedMapState, KeyedSortedMapState}
import org.apache.flink.table.api.dataview.Order
import org.apache.flink.table.api.types.DataType
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.codegen.{CodeGenUtils, Compiler, FieldAccess, GeneratedFieldExtractor}
import org.apache.flink.table.plan.util.RankRange
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.sort.RecordComparator
import org.apache.flink.table.runtime.functions.ExecutionContext
import org.apache.flink.table.typeutils.{AbstractRowSerializer, BaseRowTypeInfo, OrderedTypeUtils}
import org.apache.flink.table.util.{LRUMap, Logging, StateUtil}
import org.apache.flink.table.runtime.functions.ProcessFunction.Context
import org.apache.flink.util.Collector

/**
  * A fast version of rank process function which hold sorted top n data in heap as well as
  * all processed rows in sorted map state. (elems in heap is mirror of parts of elems in state)
  *
  *<p>It utilizes sorted map state's relatively small time complexity of data iterating to speed up
  * ranking process.
  * As a supplement, it works in scenarios where UpdateFastRank cannot work.
  *
  * e.g. topn = 4:
  *    heap:  11  10  9  8             << heap is only mirror of PART of state
  *    state: 11  10  9  8 | 7  6  5   << state holds all processed rows
  *
  *  ranking process occurs mostly on heap, and sometimes on state only when necessary.
  *
  * NOTE:
  *    1. In implementation, there're data state and row key state, data state holds sort key and
  *       row key(to act as row) while row key state holds mapping of row key and row(actual data).
  *       This can speed up lookup process as tremendous times of row moving can be avoided.
  *    2. Same design happens to in-heap sorted map and row key map, which holds topn data.
  *       (or a little more than topn, please see descriptions somewhere in codes below)
  *
  *<p>Conditions for applying this rank:
  *  a. upstream has primary key(s)
  *  b. single sort key, because keyed-sorted-map-state can't handle complex key type.
  */
class UnarySortUpdateRankFunction[K](
    inputRowType: BaseRowTypeInfo,
    rowKeyType: BaseRowTypeInfo,
    sortKeyType: DataType,
    rowKeySelector: KeySelector[BaseRow, BaseRow],
    genSortKeyExtractor: GeneratedFieldExtractor,
    order: Order,
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
  // a keyed sorted map state stores mapping from sort key to row key list
  // Note that sort key is of type TypeInformation[_], because it is for single sort
  // key that we use this rank operator
  private var dataState: KeyedSortedMapState[BaseRow, K, JList[BaseRow]] = _

  // a keyed map state stores mapping from row key to row
  // It's a supplement to dataState, as it can facilitate row key based search
  @transient
  private var rowkeyState: KeyedMapState[BaseRow, BaseRow, BaseRow] = _

  @transient
  // a sorted map stores mapping from sort key to row key list, a part heap mirror of state
  // It only hold topn rows to facilitate rank operation. And it needs to ge synced to state, too
  protected var sortedMap: SortedMap[K] = _
  @transient
  protected var kvSortedMap: LRUMap[BaseRow, SortedMap[K]] = _

  @transient
  // a HashMap stores mapping from rowkey to record.
  // It is a supplement to in-heap sorted map, because in some scenarios it can facilitate
  // lookup operation, such as when deciding whether a row update is within topn range or not,
  // based on row key.
  protected var rowKeyMap: JMap[BaseRow, BaseRow] = _
  @transient
  protected var kvRowKeyMap: LRUMap[BaseRow, JMap[BaseRow, BaseRow]] = _

  // sort key comparator used both in sorted map and state
  protected var sortKeyComparator: Comparator[K] = _

  // sort key selector that would be generated in open() method
  protected var sortKeySelector: FieldAccess[BaseRow,K] = _

  /**
    * open function does initialization.
    * get state, create comparator, register metric, etc.
    */
  override def open(ctx: ExecutionContext): Unit = {
    super.open(ctx)
    val lruCacheSize: Int = Math.max(1, (cacheSize / getDefaultTopSize).toInt)

    // make sure the cached map is in a fixed size, avoid OOM
    kvSortedMap = new LRUMap[BaseRow, SortedMap[K]](lruCacheSize)
    kvRowKeyMap = new LRUMap[BaseRow, JMap[BaseRow, BaseRow]](lruCacheSize)

    LOG.info("Top{} operator is using LRU caches key-size: {}", getDefaultTopSize, lruCacheSize)

    // create sort key comparator
    sortKeyComparator = OrderedTypeUtils
      .createComparatorFromDataType(sortKeyType, order)
      .asInstanceOf[Comparator[K]]

    // create sort key selector
    sortKeySelector = CodeGenUtils.compile(
      Thread.currentThread.getContextClassLoader,
      genSortKeyExtractor.name,
      genSortKeyExtractor.code).newInstance.asInstanceOf[FieldAccess[BaseRow,K]]

    val rowKeyBaseRowType = new BaseRowTypeInfo(rowKeyType.getFieldTypes: _*)
        .asInstanceOf[TypeInformation[BaseRow]]

    // get keyed sorted state
    val valueTypeInfo = new ListTypeInfo[BaseRow](rowKeyBaseRowType)

    val sortedMapStateDesc =
      new SortedMapStateDescriptor(
        "data-state-with-unary-update",
        sortKeyComparator,
        OrderedTypeUtils
          .createOrderedTypeInfoFromDataType(sortKeyType, order)
          .asInstanceOf[TypeInformation[K]],
        valueTypeInfo.asInstanceOf[TypeInformation[JList[BaseRow]]])

    dataState = ctx.getKeyedSortedMapState(sortedMapStateDesc)

    // get keyed map state
    val mapStateDesc =
      new MapStateDescriptor[BaseRow, BaseRow](
        "rowkey-state-with-unary-update",
        rowKeyBaseRowType,
        inputRowType.asInstanceOf[TypeInformation[BaseRow]])

    rowkeyState = ctx.getKeyedMapState(mapStateDesc)

    // metrics
    registerMetric(kvSortedMap.size() * getDefaultTopSize)
  }

  /**
    * for get/create corresponding sorted map & row key map associated with current partition key.
    */
  def initHeapStates(): Unit = {
    requestCount += 1

    val partitionKey = executionContext.currentKey()
    sortedMap = kvSortedMap.get(partitionKey)
    rowKeyMap = kvRowKeyMap.get(partitionKey)
    if (sortedMap == null) {
      // row key map and sorted map coexist and together they provided in-memory buffer for topn.
      // Therefore we only need to do null check for sorted map
      sortedMap = new SortedMap(sortKeyComparator, ArrayListSupplier)
      rowKeyMap = new JHashMap[BaseRow, BaseRow]()
      kvSortedMap.put(partitionKey, sortedMap)
      kvRowKeyMap.put(partitionKey, rowKeyMap)

      // restore sorted map and row key map
      var cnt = 0
      val stateIter = dataState.iterator(partitionKey)
      if (stateIter != null) {
        /* NOTE:
         * For sake of simple state operation, here we don't strictly limit
         * sorted map to topn size, but roughly the same or a few more.
         * And on restoring and/or state saving, we get/put a collection of rows with same
         * sort key as a whole.
         */
        while (cnt < rankEnd && stateIter.hasNext) {
          val stateEntry = stateIter.next()
          val sortKey = stateEntry.getKey
          val rowkeys = stateEntry.getValue

          for (rowkey <- rowkeys) {
            val row: BaseRow = rowkeyState.get(partitionKey, rowkey)
            if (row == null) {
              // state inconsistent between data state and rowkey state
              LOG.warn(s"[Unary topn] state inconsistent between data state and rowkey state! " +
                         StateUtil.STATE_CLEARED_WARN_MSG)
            } else {
              rowKeyMap.put(rowkey, row)
            }
          }

          // the order is preserved
          sortedMap.putAll(sortKey, rowkeys)
          cnt += rowkeys.size()
        }
      }
    } else {
      hitCount += 1
    }
  }

  /**
    * Function for processing every incoming row to this rank operator
    */
  override def processElement(
     inputBaseRow: BaseRow,
     context: Context,
     out: Collector[BaseRow]): Unit = {

    val currentTime = context.timerService().currentProcessingTime()
    // register state-cleanup timer
    registerProcessingCleanupTimer(context, currentTime)

    initHeapStates()
    initRankEnd(inputBaseRow)

    rankKind match {
      case SqlKind.ROW_NUMBER =>

        val currentKey = executionContext.currentKey()
        val sortKey = sortKeySelector.extract(inputBaseRow)
        val rowKey = rowKeySelector.getKey(inputBaseRow)

        var oldRow = rowKeyMap.get(rowKey)
        if (oldRow == null) {
          // check row key state to see if this row is a update of elements, or a new insertr
          oldRow = rowkeyState.get(currentKey, rowKey)
        }

        if (oldRow != null) {
          /* scenario 1: A update of existing element */
          val oldSortKey = sortKeySelector.extract(oldRow)
          doUpdate(
            currentKey,
            inputBaseRow,
            rowKey,
            sortKey,
            oldSortKey,
            oldRow,
            out)
        } else {
          /* scenario 2: An insert of new element */
          val isInHeap = checkSortKeyInBufferRange(currentKey, sortKey)
          doInsert(
            currentKey,
            inputBaseRow,
            rowKey,
            sortKey,
            isInHeap,
            out)
        }

      case _ => ???
    }
  }

  /**
    * Method for handling row insert
    * There're two scenarios here:
    *    1. An insert that should be placed in sorted map
    *    2. A Insert whose sort key is greater than the last in sort map and should be put to state.
    *       This wouldn't happen if sorted map has element counts less than topn.
    *       Therefore, no rank change's needed in scenario #2
    *
    * !!!! NOTE !!!!
    *  For scenario when out-of-range elements need to be removed from heap as a consequence of
    *  new insert:
    *  1. NOT every time we can move in a way such that there's exactly topn size
    *     elements left after removal, as long as we have to remove one sort key along with its
    *     whole lists of row keys per round
    *  2. After the first time total element counts >= topn, always make sure sorted map's
    *     element counts >= topn, to avoid such cases:
    *     Example:
    *     (topn = 5)
    *       sorted map: x x x (sort key = 1) | x x (sort key = 2)
    *       state:      x x x                | x x x              | x x x
    *
    *     OLD WAY (not adopted):
    *       - assuming topn is 5, heap has more than 5 elements
    *       - we need to remove some elements to make sure sorted map have no more than topn elems.
    *       - after removal, for sort key 2, SORTED MAP have 2 rows while there're 3 rows for
    *         same sort key 2 in STATE.
    *         (State holds full elements for sort key 2, sorted map holds parts.)
    *
    *       - This sounds fine at first glance, but remember that data state operation is handling
    *         list of rows rather than single row each round, which makes this situation
    *         particularly inconvenient for state update in future.
    *
    *     NEW WAY (current implementation):
    *       - Therefore, we'd rather let sorted map to hold "a little more" elements in
    *         above scenario like this:
    *
    *        sorted map: x x x | x x x           (topn = 5)
    *        state:      x x x | x x x | x x x
    */
  def doInsert(
     partitionKey: BaseRow,
     inputRow: BaseRow,
     rowKey: BaseRow,
     sortKey: K,
     inHeap: Boolean,
     out: Collector[BaseRow]): Unit = {

    /** step 1. update heap and state **/
    refreshHeapAndStateForInsert(partitionKey, inputRow, rowKey, sortKey, inHeap)

    /* An insert that should be placed in sorted map
     * Note: Insert into places beyond topn only involves state update. NO rank change's needed.
     */
    if (inHeap) {
      /** step2. do rank change **/
      if (isRowNumberAppend || hasOffset) {
        /* with row number case */
        emitRecordsWithRowNumber(
          inputRow,
          sortKey,
          out)
      } else {
        /* without row number case */
        emitRecordsWithoutRowNum(
          partitionKey,
          inputRow,
          rowKey,
          sortKey,
          out)
      }

      /** step3 remove element(s) that are squeezed out of topn range after insertion in sorted map
        */
      if (sortedMap.currentTopNum > rankEnd) {
        removeOutOfRangeElemsFromHeap()
      }
    }
  }

  /**
    * Method for handling row insert in "with row number" scenario
    */
  def emitRecordsWithRowNumber(
    inputRow: BaseRow,
    sortKey: K,
    out: Collector[BaseRow]): Unit = {

    // when we are here, we know insertion happens in heap
    val iter = sortedMap.entrySet().iterator()
    var curRank = 0
    var findSortKey = false

    while (iter.hasNext && curRank < rankEnd) {
      val entry = iter.next()
      val curKey = entry.getKey
      val rowkeys = entry.getValue

      if (!findSortKey && curKey.equals(sortKey)) {
        //located place where this row's inserted
        curRank += rowkeys.size()
        collect(out, inputRow, curRank)  //emit the inserted row
        findSortKey = true
      } else if (findSortKey) {
        //dealing with rows that rank after this new inserted row within topn range
        val rowkeyIter = rowkeys.iterator()
        while (rowkeyIter.hasNext && curRank < rankEnd) {
          curRank += 1
          val rowkey = rowkeyIter.next()
          val tempRow = rowKeyMap.get(rowkey)
          retract(out, tempRow, curRank - 1)
          collect(out, tempRow, curRank)
        }
      } else {
        curRank += rowkeys.size()
      }
    }
  }

  /**
    * Method for handling row insert in "without row number" scenario
    */
  def emitRecordsWithoutRowNum(
    partitionKey: BaseRow,
    inputRow: BaseRow,
    rowKey: BaseRow,
    sortKey: K,
    out: Collector[BaseRow]): Unit = {

    // when we are here, we know insertion happens in heap
    val (rank, _) = rowNumber(sortKey, rowKey, sortedMap)

    if (isInRankEnd(rank)) {
      /* emit this new row */
      collect(out, inputRow)

      if (sortedMap.currentTopNum <= rankEnd) {
        val (_, adjKeyInState) = getAdjacentElems(partitionKey)
        if (adjKeyInState != null.asInstanceOf[K]) {
          if (sortedMap.currentTopNum < rankEnd) {
            throw new TableException("This shouldn't happen. Please file an issue!")
          }
          val tempRowKeys = dataState.get(partitionKey, adjKeyInState)
          if (tempRowKeys != null) {
            val tempRowkey = tempRowKeys.head
            val tempRow = rowkeyState.get(partitionKey, tempRowkey)
            /*
           * if there's row squeezed out of topn as consequence of inserting new row in topn range
           * and it's in state now, send delete msg to downstream
           */
            if (tempRow != null) {
              delete(out, tempRow)
            } else {
              /**
                * Does not send delete msg to downstream which is squeezed out of topn if cannot
                * find the row data of the the row because the data state is cleared.
                */
              LOG.warn(StateUtil.STATE_CLEARED_WARN_MSG)
            }
          } else {
            LOG.warn(StateUtil.STATE_CLEARED_WARN_MSG)
          }
        }
      } else {
        val tempRowkey = sortedMap.getElement(rankEnd.toInt + 1)
        if (tempRowkey == null) {
          throw new TableException("This shouldn't happen. Please file an issue!")
        }
        val tempRow = rowKeyMap.get(tempRowkey)
        /*
         * if there's row squeezed out of topn as consequence of inserting new row in topn range
         * and it's still in heap, send delete msg to downstream
         */
        delete(out, tempRow)
      }
    }

  }

  /**
    * Method for handling row update
    */
  def doUpdate(
     partitionKey: BaseRow,
     inputRow: BaseRow,
     rowKey: BaseRow,
     sortKey: K,
     oldSortKey: K,
     oldRow: BaseRow,
     out: Collector[BaseRow]): Unit = {

    val (oldPosInHeap, newPosInHeap) = isPositionsOfUpdateInHeap(rowKey, sortKey, partitionKey)

    val (oldRank, oldInnerRank) = if (oldPosInHeap) {
      rowNumber(oldSortKey, rowKey, sortedMap)
    } else {
      (null.asInstanceOf[Int], null.asInstanceOf[Int])
    }

    val compareNewAndOldSortKey = sortKeyComparator.compare(sortKey, oldSortKey)
    if (compareNewAndOldSortKey == 0) {
      /*
       * scenario 1: this update only changed row content, sort key being the same
       */
      if (oldPosInHeap) {
        // and it is in heap, update row key map and do rank change
        rowKeyMap.put(rowKey, inputRowSer.copy(inputRow))
        if (oldRank <= rankEnd) {
          if (isRowNumberAppend || hasOffset) {
            /* with row number case */
            retract(out, oldRow, oldRank)
            collect(out, inputRow, oldRank)
          } else {
            /* without row number case */
            collect(out, inputRow)
          }
        }
      }
      rowkeyState.add(partitionKey, rowKey, inputRow)
      //return directly
      return
    }

    // determine whether this update is forward or backward update
    val updateForward = if (compareNewAndOldSortKey < 0) {
      true
    } else {  //"compareNewAndOldSortKey == 0" case is handled above
      false
    }

    /*
     * scenario 2: sort key and old sort key is NOT the same
     */
    /** step1. update heap and state **/
    refreshHeapAndStateForRemoval(partitionKey, rowKey, oldSortKey, oldPosInHeap, false)
    refreshHeapAndStateForInsert(partitionKey, inputRow, rowKey, sortKey, newPosInHeap, true)

    /** step2. loading some elements into heap, if necessary after adjustment **/
    // handle cases when sorted map have less than topn elements after adjust.
    // If there's additional elements in state, loading them into heap until we have
    // at least topn elements in heap
    if (sortedMap.currentTopNum < rankEnd) {
      fillInHeapUntilFull(partitionKey)
    }

    /** step3. Doing rank changes **/
    if (isRowNumberAppend || hasOffset) {
      /* with row number case */
      updateRecordsWithRowNumber(
        inputRow,
        sortKey,
        oldSortKey,
        updateForward,
        oldInnerRank,
        out)
    } else {
      /* without row number case */
      updateRecordsWithoutRowNum(
        partitionKey,
        rowKey,
        sortKey,
        oldRow,
        oldRank,
        oldPosInHeap,
        updateForward,
        out)
    }

    /** step4. remove element(s) that are possibly squeezed out of topn range after update **/
    if (sortedMap.currentTopNum > rankEnd) {
      removeOutOfRangeElemsFromHeap()
    }
  }

  /**
    * Method for handling row update in "with row number" scenario
    */
  def updateRecordsWithRowNumber(
    inputRow: BaseRow,
    sortKey: K,
    oldSortKey: K,
    updateForward: Boolean,
    oldInnerRank: Int,
    out: Collector[BaseRow]): Unit = {

    val (minKey, maxKey) = if (updateForward) {
      (sortKey, oldSortKey)
    } else {
      (oldSortKey, sortKey)
    }

    val iterator = sortedMap.entrySet().iterator()
    var curRank = 0
    var quitFlag = false

    while (iterator.hasNext && curRank < rankEnd && !quitFlag) {
      val entry = iterator.next()
      val curKey = entry.getKey
      val rowKeys = entry.getValue

      val compareWithMin = sortKeyComparator.compare(curKey, minKey)
      val compareWithMax = sortKeyComparator.compare(curKey, maxKey)

      if (compareWithMin < 0) {
        // elements in the front that don't change rank
        curRank += rowKeys.size()
      } else if (compareWithMax > 0) {
        // now we reach after max key. All is finished. quit directly
        quitFlag = true
      } else {

        if (updateForward && compareWithMin == 0) {
          // In forward-update scenario, we are not in new position, emit the updated row
          curRank += rowKeys.size()
          if (curRank <= rankEnd) {
            collect(out, inputRow, curRank)
          }
        } else {
          /* Below while loop handles 2 situations:
           * 1. the updated row's emit in backward-update scenario
           * 2. retract & emit rows that have rank change due to updated row,
           *    both in forward and afterward update scenario
           * NOTE:
           *  when reaching end of rank-changed section, it'll return directly
           */
          val rowKeyIter = rowKeys.iterator()
          var curInnerRank = 0
          while (rowKeyIter.hasNext && curRank < rankEnd && !quitFlag) {
            curRank += 1
            curInnerRank += 1

            if (updateForward) {
              if (compareWithMax == 0 && curInnerRank >= oldInnerRank) {
                // In forward-update scenario, match to the old position, all done and quit
                quitFlag = true
              } else {
                val rowKey = rowKeyIter.next()
                val row = rowKeyMap.get(rowKey)
                retract(out, row, curRank - 1)
                collect(out, row, curRank)
              }
            } else {
              if (compareWithMax == 0 && curInnerRank == rowKeys.size()) {
                // In backward-update scenario, match to new position,
                // emit updated row, all done and quit
                collect(out, inputRow, curRank)
                quitFlag = true
              } else if (!(compareWithMin == 0 && curInnerRank < oldInnerRank)) {
                val rowKey = rowKeyIter.next()
                val row = rowKeyMap.get(rowKey)
                if (curRank < rankEnd) {
                  //It should take into consideration of (topn + 1) changed to (topn),
                  // in which retract is not needed as we didn't emit (topn + 1) previously
                  retract(out, row, curRank + 1)
                }
                collect(out, row, curRank)
              } else {
                rowKeyIter.next()   // move forward
              }
            }
          } /* end of "while (rowKeyIter.hasNext &&..." */
        }
      }
    } /* end of "while (iterator.hasNext &&..." */

  }

  /**
    * Method for handling row update in "without row number" scenario
    */
  def updateRecordsWithoutRowNum(
    partitionKey: BaseRow,
    rowKey: BaseRow,
    sortKey: K,
    oldRow: BaseRow,
    oldRank: Int,
    oldPosInHeap: Boolean,
    updateForward: Boolean,
    out: Collector[BaseRow]): Unit = {

    // First, determine where the updated row resides, after state and heap update.
    val row = rowKeyMap.get(rowKey)
    val isNewRowInRankEnd = if (row != null) {
      /* Updated row now in heap */
      val (rank, _) = rowNumber(sortKey, rowKey, sortedMap)
      isInRankEnd(rank)
    } else {
      false
    }

    val isOldRowInRankEnd = if (oldPosInHeap == false) {
      false
    } else {
      isInRankEnd(oldRank)
    }

    if (updateForward) {
      /** update forward case **/
      if (isNewRowInRankEnd) {

        if (isOldRowInRankEnd) {
          /* emit updated row */
          collect(out, row)
        } else {
          val rowAfterN = if (sortedMap.currentTopNum > rankEnd) {
            val tempRowkey = sortedMap.getElement(rankEnd.toInt + 1)
            rowKeyMap.get(tempRowkey)
          } else {
            val (_, adjKeyInState) = getAdjacentElems(partitionKey)
            if (adjKeyInState != null.asInstanceOf[K]) {
              val tempRowKeys = dataState.get(partitionKey, adjKeyInState)
              if (tempRowKeys != null) {
                val tempRowKey = tempRowKeys.head
                rowkeyState.get(partitionKey, tempRowKey)
              } else {
                /**
                  * Does not send delete msg to downstream which is squeezed out of topn if cannot
                  * find the row data of the the row because the data state is cleared.
                  */
                LOG.warn(StateUtil.STATE_CLEARED_WARN_MSG)
                null.asInstanceOf[BaseRow]
              }
            } else {
              null.asInstanceOf[BaseRow]
            }
          }
          if (rowAfterN != null) {
            /* send delete msg for row that is squeezed out of topn range */
            delete(out, rowAfterN)
          }
          /* emit updated row */
          collect(out, row)
        }
      }

    } else {
      /** update backward case **/
      if (isOldRowInRankEnd) {

        if (isNewRowInRankEnd) {
          /* emit this new row */
          collect(out, row)
        } else {
          val tempRowkey = sortedMap.getElement(rankEnd.toInt)
          val tempRow = rowKeyMap.get(tempRowkey)

          /* send delete msg for updated row */
          delete(out, oldRow)
          /* emit row squeezed in topn range as a consequence of update */
          collect(out, tempRow)
        }

      }
    }
  }

  /**
    * Get positions for row update scenario:
    *  If old row was in heap or merely in state?
    *  If updated row is in heap or merely in state?
    */
  def isPositionsOfUpdateInHeap(
    rowKey: BaseRow,
    sortKey: K,
    partitionKey: BaseRow) : (Boolean, Boolean) = {

    // get last sort key in sorted map and following sort key in state
    val (lastKeyInHeap, adjKeyInState) = getAdjacentElems(partitionKey)

    val oldRow = rowKeyMap.get(rowKey)

    val oldPosInHeap = if (oldRow != null) {
      /* old position is in heap */
      true
    } else {
      /* old position is in state */
      false
    }

    val compare = sortKeyComparator.compare(sortKey, lastKeyInHeap)
    val newPosInHeap = if (compare <= 0) {
      /* new position is in heap */
      true
    } else {
      if (adjKeyInState == null.asInstanceOf[K]) {
        /* new position is in heap */
        true
      } else {
        /* new position is in state */
        if (sortedMap.currentTopNum < rankEnd) {
          throw new TableException("[unary topn] This shouldn't happen. Please file an issue")
        }
        false
      }
    }

    (oldPosInHeap, newPosInHeap)
  }

  /**
    * when heap has more than topn size elements, try to remove some to the point that there're
    * topn or a little more than topn elements remaining, but won't make elements counts
    * less than topn
    */
  def removeOutOfRangeElemsFromHeap(): Unit = {
    var lastEntry = sortedMap.lastEntry()

    // do removal from tail
    while (lastEntry != null
      && sortedMap.currentTopNum - lastEntry.getValue.size() >= rankEnd) {
      val sortKey = lastEntry.getKey
      val rowkeys = lastEntry.getValue
      for (rowkey <- rowkeys) {
        rowKeyMap.remove(rowkey)
      }
      sortedMap.removeAll(sortKey)
      lastEntry = sortedMap.lastEntry()
    }
  }

  /**
    * After adjustment, there might be less than topn size elements in heap,
    * try to load additional elems from state to heap.
    */
  def fillInHeapUntilFull(partitionKey: BaseRow): Unit = {
    val (lastKeyInHeap, _) = getAdjacentElems(partitionKey)

    val iter = dataState.tailIterator(partitionKey, lastKeyInHeap)
    while (sortedMap.currentTopNum < rankEnd && iter != null && iter.hasNext) {
      val entry = iter.next()
      if (!entry.getKey.equals(lastKeyInHeap)) {
        val sortKey = entry.getKey
        val rowkeyList = entry.getValue
        for(rowkey <- rowkeyList) {
          val row = rowkeyState.get(partitionKey, rowkey)
          rowKeyMap.put(rowkey, row)
        }
        sortedMap.putAll(sortKey, rowkeyList)
      }
    }
  }

  /**
    * get last sort key in sorted map and adjacent sort key in state
    */
  def getAdjacentElems(currentKey: BaseRow): (K, K) = {
    val lastEntry = sortedMap.lastEntry()
    val lastKeyInHeap = lastEntry.getKey()
    val adjKeyInState = if (lastKeyInHeap != null.asInstanceOf[K]) {
      val iter = dataState.tailIterator(currentKey, lastKeyInHeap)
      if(iter != null && iter.hasNext) {

        iter.next()   // jump to next (ignore position which have same sort key)
        if (iter.hasNext) {
          iter.next().getKey
        } else {
          null.asInstanceOf[K]
        }
      } else {
        null.asInstanceOf[K]
      }
    } else {
      null.asInstanceOf[K]
    }

    (lastKeyInHeap, adjKeyInState)
  }

  /**
   * return true if record should be put into sort map.
   */
  def checkSortKeyInBufferRange(currentKey: BaseRow, sortKey: K): Boolean = {
    val worstEntry = sortedMap.lastEntry()
    if (worstEntry == null) {
      //sort map is empty
      true
    } else {
      val worstKey = worstEntry.getKey
      val compare = sortKeyComparator.compare(sortKey, worstKey)
      if (compare <= 0) {
        //elem should be inserted among elements in sorted map
        true
      } else if (sortedMap.currentTopNum < rankEnd) {
        //sorted map is not full
        val (_, adjKeyInState) = getAdjacentElems(currentKey)
        if (adjKeyInState != null.asInstanceOf[K]) {
          //todo for debug check purpose
          throw new TableException("This shouldn't happen! Please contact developer for details")
        } else {
          //no adjacent elem in state, meaning that the last key in sorted map is the true last one
          true
        }
      } else {
        //sorted map is full
        false
      }
    }
  }

  /**
    * update sorted map and state for element removal
    */
  def refreshHeapAndStateForRemoval(
    partitionKey: BaseRow,
    rowKey: BaseRow,
    sortKey: K,
    inHeap: Boolean,
    updateRowkeyState: Boolean = true): Unit = {

    if (inHeap) {
      /* update in-memory content */
      rowKeyMap.remove(rowKey)
      sortedMap.remove(sortKey, rowKey)
    }

    val rowkeyList = if (inHeap) {
      sortedMap.get(sortKey)
    } else {
      val tempRowkeyList = dataState.get(partitionKey, sortKey)
      if (tempRowkeyList != null) {
        tempRowkeyList.remove(rowKey)
        tempRowkeyList
      } else {
        // Remove the partitionKey and sortKey from data state if cannot find the row data of the
        // the row because the data state is cleared.
        LOG.warn(StateUtil.STATE_CLEARED_WARN_MSG)
        null
      }
    }

    /* update data state */
    if (rowkeyList == null || rowkeyList.isEmpty) {
      dataState.remove(partitionKey, sortKey)
    } else {
      dataState.add(partitionKey, sortKey, rowkeyList.asInstanceOf[JList[BaseRow]])
    }

    if (updateRowkeyState) {
      /* update row key state */
      rowkeyState.remove(partitionKey, rowKey)
    }
  }

  /**
    * update sorted map and state for element insert
    */
  def refreshHeapAndStateForInsert(
    partitionKey: BaseRow,
    inputRow: BaseRow,
    rowKey: BaseRow,
    sortKey: K,
    inHeap: Boolean,
    updateRowkeyState: Boolean = true): Unit = {

    if (inHeap) {
      /* update in-memory content */
      sortedMap.put(sortKey, rowKey)
      rowKeyMap.put(rowKey, inputRowSer.copy(inputRow))
    }

    val rowkeyList = if (inHeap) {
      sortedMap.get(sortKey)
    } else {
      var keyList = dataState.get(partitionKey, sortKey)
      if (keyList == null) {
        keyList = new util.ArrayList[BaseRow]()
      }
      keyList.add(rowKey)
      keyList
    }

    /* update data state */
    dataState.add(partitionKey, sortKey, rowkeyList.asInstanceOf[JList[BaseRow]])

    if (updateRowkeyState) {
      /* update row key state */
      rowkeyState.add(partitionKey, rowKey, inputRow)
    }
  }


  private object ArrayListSupplier extends Supplier[JCollection[BaseRow]] {
    override def get(): JCollection[BaseRow] = new JArrayList[BaseRow]()
  }

  // just let it go, unary sort rank may have "a little" more than topn size elems in heap,
  // and has no interest in this method.
  override def getMaxSortMapSize: Long = 0L

}
