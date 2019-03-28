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

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._

import scala.collection.JavaConversions._

/**
  * Utility methods concerning [[RexNode]].
  */
object FlinkRexUtil {

  /**
    * Adjust the condition's field indices according to mapOldToNewIndex.
    *
    * @param c The condition to be adjusted.
    * @param fieldsOldToNewIndexMapping A map containing the mapping the old field indices to new
    *   field indices.
    * @param rowType The row type of the new output.
    * @return Return new condition with new field indices.
    */
  private[flink] def adjustInputRefs(
      c: RexNode,
      fieldsOldToNewIndexMapping: Map[Int, Int],
      rowType: RelDataType) = c.accept(
    new RexShuttle() {

      override def visitInputRef(inputRef: RexInputRef): RexNode = {
        assert(fieldsOldToNewIndexMapping.containsKey(inputRef.getIndex))
        val newIndex = fieldsOldToNewIndexMapping(inputRef.getIndex)
        val ref = RexInputRef.of(newIndex, rowType)
        if (ref.getIndex == inputRef.getIndex && (ref.getType eq inputRef.getType)) {
          inputRef
        } else {
           // re-use old object, to prevent needless expr cloning
          ref
        }
      }
    })
}
