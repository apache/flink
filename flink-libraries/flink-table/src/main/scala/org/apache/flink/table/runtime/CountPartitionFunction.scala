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

import java.lang.Iterable

import org.apache.flink.api.common.functions.RichMapPartitionFunction
import org.apache.flink.util.Collector

class CountPartitionFunction[IN] extends RichMapPartitionFunction[IN, (Int, Long)] {

  override def mapPartition(value: Iterable[IN], out: Collector[(Int, Long)]): Unit = {
    val partitionIndex = getRuntimeContext.getIndexOfThisSubtask
    var elementCount = 0L
    val iterator = value.iterator()
    while (iterator.hasNext) {
      if (elementCount != Long.MaxValue) { // prevent overflow
        elementCount += 1L
      }
      iterator.next()
    }
    out.collect(partitionIndex, elementCount)
  }
}
