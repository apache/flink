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

import java.lang.{Iterable => JIterable}

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.util.Collector

class IntersectCoGroupFunction[T](all: Boolean) extends CoGroupFunction[T, T, T]{
  override def coGroup(first: JIterable[T], second: JIterable[T], out: Collector[T]): Unit = {
    if (first == null || second == null) return
    val leftIter = first.iterator()
    val rightIter = second.iterator()
    if (all) {
      while (leftIter.hasNext && rightIter.hasNext) {
        out.collect(leftIter.next)
        rightIter.next
      }
    } else {
      if (leftIter.hasNext && rightIter.hasNext) {
        out.collect(leftIter.next)
      }
    }
  }
}
