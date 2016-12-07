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

package org.apache.flink.table.runtime

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConverters._


class LimitFilterFunction[T](
    limitStart: Long,
    limitEnd: Long,
    broadcastName: String)
  extends RichFilterFunction[T] {

  var partitionIndex: Int = _
  var elementCount: Long = _
  var countList: Array[Long] = _

  override def open(config: Configuration) {
    partitionIndex = getRuntimeContext.getIndexOfThisSubtask

    val countPartitionResult = getRuntimeContext
      .getBroadcastVariable[(Int, Long)](broadcastName)
      .asScala

    // sort by partition index, extract number per partition, sum with intermediate results
    countList = countPartitionResult.sortWith(_._1 < _._1).map(_._2).scanLeft(0L) { case (a, b) =>
        val sum = a + b
        if (sum < 0L) { // prevent overflow
          Long.MaxValue
        }
        sum
    }.toArray

    elementCount = 0
  }

  override def filter(value: T): Boolean = {
    if (elementCount != Long.MaxValue) { // prevent overflow
      elementCount += 1L
    }
    // we filter out records that are not within the limit (Long.MaxValue is unlimited)
    limitStart - countList(partitionIndex) < elementCount &&
      (limitEnd == Long.MaxValue || limitEnd - countList(partitionIndex) >= elementCount)
  }
}
