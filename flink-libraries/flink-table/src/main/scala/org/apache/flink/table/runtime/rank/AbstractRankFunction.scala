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

import java.lang.{Long => JLong}
import java.util.Comparator
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.metrics.{Counter, Gauge}
import org.apache.flink.runtime.state.keyed.KeyedValueState
import org.apache.flink.table.api.types.TypeConverters
import org.apache.flink.table.api.{TableConfig, Types}
import org.apache.flink.table.codegen.{EqualiserCodeGenerator, GeneratedRecordEqualiser}
import org.apache.flink.table.dataformat.util.BaseRowUtil
import org.apache.flink.table.dataformat.{BaseRow, GenericRow, JoinedRow}
import org.apache.flink.table.plan.util.{ConstantRankRange, RankRange, VariableRankRange}
import org.apache.flink.table.runtime.aggregate.ProcessFunctionWithCleanupState
import org.apache.flink.table.runtime.functions.ExecutionContext
import org.apache.flink.table.runtime.sort.RecordEqualiser
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.util.Collector

abstract class AbstractRankFunction(
    tableConfig: TableConfig,
    rankRange: RankRange,
    inputRowType: BaseRowTypeInfo,
    inputArity: Int,
    outputArity: Int,
    generateRetraction: Boolean)
  extends ProcessFunctionWithCleanupState[BaseRow, BaseRow](tableConfig) {

  protected var isConstantRankEnd: Boolean = _
  protected var rankEnd: Long = -1
  protected var rankStart: Long = -1

  private var rankEndIndex: Int = _
  private var rankEndState: KeyedValueState[BaseRow, JLong] = _
  private var invalidCounter: Counter = _

  private var outputRow: JoinedRow = _

  protected val isRowNumberAppend: Boolean = inputArity + 1 == outputArity

  protected var equaliser: RecordEqualiser = _

  // metrics
  protected var hitCount: Long = 0L
  protected var requestCount: Long = 0L

  override def open(ctx: ExecutionContext): Unit = {
    super.open(ctx)
    initCleanupTimeState("RankFunctionCleanupTime")
    outputRow = new JoinedRow()

    // variable rank limit
    rankRange match {
      case c: ConstantRankRange =>
        isConstantRankEnd = true
        rankEnd = c.rankEnd
        rankStart = c.rankStart
      case v: VariableRankRange =>
        isConstantRankEnd = false
        rankEndIndex = v.rankEndIndex
        val rankStateDesc = new ValueStateDescriptor[JLong]("rankEnd", Types.LONG)
        rankEndState = ctx.getKeyedValueState(rankStateDesc)
    }

    val generatedEqualiser = createEqualiser(inputRowType)
    equaliser = generatedEqualiser.newInstance(getRuntimeContext.getUserCodeClassLoader)

    invalidCounter = ctx.getRuntimeContext.getMetricGroup.counter(
      "topn.invalidTopSize")
  }

  protected def getDefaultTopSize: Long = {
    if (isConstantRankEnd) {
      rankEnd
    } else {
      100 // we set default topn size to 100
    }
  }

  protected def initRankEnd(row: BaseRow): Long = {
    if (isConstantRankEnd) {
      rankEnd
    } else {
      val currentKey = executionContext.currentKey()
      val rankEndValue = rankEndState.get(currentKey)
      val curRankEnd = row.getLong(rankEndIndex)
      if (rankEndValue == null) {
        rankEnd = curRankEnd
        rankEndState.put(currentKey, rankEnd)
        rankEnd
      } else {
        rankEnd = rankEndValue
        if (rankEnd != curRankEnd) {
          // increment the invalid counter when the current rank end
          // not equal to previous rank end
          invalidCounter.inc()
        }
        rankEnd
      }
    }
  }

  /**
    * Returns the rank number and inner rank of the record row.
    * Note that this method is used in scenarios where row have row keys and
    * sort map is of type [sort key, List[row keys]]
    */
  def rowNumber[K](
    sortKey: K,
    rowKey: BaseRow,
    sortedMap: SortedMap[K]): (Int, Int) = {

    val iterator = sortedMap.entrySet().iterator()
    var curRank = 1
    while (iterator.hasNext) {
      val entry = iterator.next()
      val curKey = entry.getKey
      val rowKeys = entry.getValue
      if (curKey.equals(sortKey)) {
        val rowKeysIter = rowKeys.iterator()
        var innerRank = 1
        while (rowKeysIter.hasNext) {
          if (rowKey.equals(rowKeysIter.next())) {
            return (curRank, innerRank)
          } else {
            innerRank += 1
            curRank += 1
          }
        }
      } else {
        curRank += rowKeys.size()
      }
    }
    throw new RuntimeException(
      s"Failed to find the sortKey: $sortKey, rowkey: $rowKey in SortedMap. " +
        s"This should never happen")
  }

  /**
    * return true if record should be put into sort map.
    */
  def checkSortKeyInBufferRange[K](
    sortKey: K, sortedMap: SortedMap[K],
    sortKeyComparator: Comparator[K]): Boolean = {
    val worstEntry = sortedMap.lastEntry()
    if (worstEntry == null) {
      // sort map is empty
      true
    } else {
      val worstKey = worstEntry.getKey
      val compare = sortKeyComparator.compare(sortKey, worstKey)
      if (compare < 0) {
        true
      } else if (sortedMap.currentTopNum < getMaxSortMapSize) {
        true
      } else {
        false
      }
    }
  }

  protected def registerMetric(heapSize: Long): Unit = {
    executionContext.getRuntimeContext.getMetricGroup
      .gauge[Double, Gauge[Double]]("topn.cache.hitRate", new Gauge[Double]() {
      override def getValue: Double =
        if (requestCount == 0) 1.0 else hitCount.toDouble / requestCount
    })
    executionContext.getRuntimeContext.getMetricGroup
      .gauge[Long, Gauge[Long]]("topn.cache.size", new Gauge[Long]() {
      override def getValue: Long = heapSize
    })
  }

  protected def collect(out: Collector[BaseRow], inputRow: BaseRow): Unit = {
    BaseRowUtil.setAccumulate(inputRow)
    out.collect(inputRow)
  }

  /**
    * This is similar to [[retract()]] but always send retraction message regardless of
    * generateRetraction is true or not
    */
  protected def delete(out: Collector[BaseRow], inputRow: BaseRow): Unit = {
    BaseRowUtil.setRetract(inputRow)
    out.collect(inputRow)
  }

  /**
    * This is with-row-number version of above delete() method
    */
  protected def delete(out: Collector[BaseRow], inputRow: BaseRow, rank: Long): Unit = {
    if (isInRankRange(rank)) {
      out.collect(createOutputRow(inputRow, rank, BaseRowUtil.RETRACT_MSG))
    }
  }

  protected def collect(out: Collector[BaseRow], inputRow: BaseRow, rank: Long): Unit = {
    if (isInRankRange(rank)) {
      out.collect(createOutputRow(inputRow, rank, BaseRowUtil.ACCUMULATE_MSG))
    }
  }

  protected def retract(out: Collector[BaseRow], inputRow: BaseRow, rank: Long): Unit = {
    if (generateRetraction && isInRankRange(rank)) {
      out.collect(createOutputRow(inputRow, rank, BaseRowUtil.RETRACT_MSG))
    }
  }

  protected def isInRankEnd(rank: Long): Boolean = {
    rank <= rankEnd
  }

  protected def isInRankRange(rank: Long): Boolean = {
    rank <= rankEnd && rank >= rankStart
  }

  protected def hasOffset: Boolean = {
    // rank start is 1-based
    rankStart > 1
  }

  protected def createOutputRow(inputRow: BaseRow, rank: Long, header: Byte): BaseRow = {
    if (isRowNumberAppend) {
      val rankRow = new GenericRow(1)
      rankRow.update(0, rank)

      outputRow.replace(inputRow, rankRow)
      outputRow.setHeader(header)
      outputRow
    } else {
      inputRow.setHeader(header)
      inputRow
    }
  }

  /**
    * get sorted map size limit
    * Implementations may vary depending on each rank who has in-memory sort map.
    * @return
    */
  protected def getMaxSortMapSize: Long

  private def createEqualiser(inputRowType: BaseRowTypeInfo): GeneratedRecordEqualiser = {
    val inputTypes = inputRowType.getFieldTypes.map(TypeConverters.createInternalTypeFromTypeInfo)
    val generator = new EqualiserCodeGenerator(inputTypes)
    generator.generateRecordEqualiser("RankValueEqualiser")
  }
}
