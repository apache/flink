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

package org.apache.flink.api.table.runtime

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.configuration.Configuration

import scala.collection.mutable
import scala.collection.JavaConverters._

class LimitFilterFunction[T](limitStart: Int,
                             limitEnd: Int,
                             broadcast: String) extends RichFilterFunction[T] {
  var elementCount = 0
  var countList = mutable.Buffer[Int]()

  override def open(config: Configuration) {
    countList = getRuntimeContext.getBroadcastVariable[(Int, Int)](broadcast).asScala
      .sortWith(_._1 < _._1).map(_._2).scanLeft(0) (_ + _)
  }

  override def filter(value: T): Boolean = {
    val partitionIndex = getRuntimeContext.getIndexOfThisSubtask
    elementCount += 1
    limitStart - countList(partitionIndex) < elementCount &&
      limitEnd - countList(partitionIndex) >= elementCount
  }
}
