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

package org.apache.flink.table.planner.utils

import java.util.{Iterator => JIterator}

import org.apache.flink.util.SplittableIterator

import scala.collection.mutable

class ParallelCollectionWrapper[T](
    val list: mutable.MutableList[T],
    var from: Integer,
    val to : Integer) extends SplittableIterator[T] with Serializable {

  override def split(numPartitions: Int): Array[JIterator[T]] = {
    var numPerPartition = (to - from) / numPartitions
    if ((to - from) % numPartitions != 0) {
      numPerPartition += 1
    }

    val result: Array[JIterator[T]] = new Array[JIterator[T]](numPartitions)
    var cur = from
    for (i <- 0 until numPartitions) {
      result(i) = new ParallelCollectionWrapper[T](list, cur, math.min(cur + numPerPartition, to))
      cur += numPerPartition
    }
    result
  }

  override def getMaximumNumberOfSplits: Int = {
    to - from
  }

  override def hasNext: Boolean = {
    from < to
  }

  override def next(): T = {
    from += 1
    list.get(from - 1).get
  }
}
