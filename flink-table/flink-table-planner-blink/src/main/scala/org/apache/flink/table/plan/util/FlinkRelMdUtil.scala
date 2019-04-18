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

package org.apache.flink.table.plan.util

import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.BinaryRow
import org.apache.flink.table.runtime.rank.{ConstantRankRange, RankRange}
import org.apache.flink.table.runtime.sort.BinaryIndexedSortable
import org.apache.flink.table.typeutils.BinaryRowSerializer

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataQuery

import java.lang.Double

import scala.collection.JavaConversions._

/**
  * FlinkRelMdUtil provides utility methods used by the metadata provider methods.
  */
object FlinkRelMdUtil {

  def getRankRangeNdv(rankRange: RankRange): Double = rankRange match {
    case r: ConstantRankRange => (r.getRankEnd - r.getRankStart + 1).toDouble
    case _ => 100D // default value now
  }

  def binaryRowAverageSize(rel: RelNode): Double = {
    val binaryType = FlinkTypeFactory.toInternalRowType(rel.getRowType)
    // TODO reuse FlinkRelMetadataQuery here
    val mq = rel.getCluster.getMetadataQuery
    val columnSizes = mq.getAverageColumnSizes(rel)
    var length = 0d
    columnSizes.zip(binaryType.getFieldTypes).foreach {
      case (columnSize, internalType) =>
        if (BinaryRow.isInFixedLengthPart(internalType)) {
          length += 8
        } else {
          if (columnSize == null) {
            // find a better way of computing generic type field variable-length
            // right now we use a small value assumption
            length += 16
          } else {
            // the 8 bytes is used store the length and offset of variable-length part.
            length += columnSize + 8
          }
        }
    }
    length += BinaryRow.calculateBitSetWidthInBytes(columnSizes.size())
    length
  }

  def computeSortMemory(mq: RelMetadataQuery, inputOfSort: RelNode): Double = {
    //TODO It's hard to make sure that the normalized key's length is accurate in optimized stage.
    // use SortCodeGenerator.MAX_NORMALIZED_KEY_LEN instead of 16
    val normalizedKeyBytes = 16
    val rowCount = mq.getRowCount(inputOfSort)
    val averageRowSize = binaryRowAverageSize(inputOfSort)
    val recordAreaInBytes = rowCount * (averageRowSize + BinaryRowSerializer.LENGTH_SIZE_IN_BYTES)
    val indexAreaInBytes = rowCount * (normalizedKeyBytes + BinaryIndexedSortable.OFFSET_LEN)
    recordAreaInBytes + indexAreaInBytes
  }

}
