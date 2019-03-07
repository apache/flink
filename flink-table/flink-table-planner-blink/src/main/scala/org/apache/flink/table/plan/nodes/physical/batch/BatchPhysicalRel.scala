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

package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.BinaryRow
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel

import org.apache.calcite.rel.RelNode

import scala.collection.JavaConversions._

/**
  * Base class for batch physical relational expression.
  */
trait BatchPhysicalRel extends FlinkPhysicalRel {

}

object BatchPhysicalRel {

  private[flink] def binaryRowAverageSize(rel: RelNode): Double = {
    val binaryType = FlinkTypeFactory.toInternalRowType(rel.getRowType)
    // TODO reuse FlinkRelMetadataQuery here
    val mq = rel.getCluster.getMetadataQuery
    val columnSizes = mq.getAverageColumnSizes(rel)
    var length = 0d
    columnSizes.zip(binaryType.getFieldTypes).foreach {
      case (columnSize, internalType) =>
        if (BinaryRow.isFixedLength(internalType)) {
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
}
