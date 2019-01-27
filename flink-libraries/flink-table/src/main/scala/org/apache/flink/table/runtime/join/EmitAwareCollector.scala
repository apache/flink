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

package org.apache.flink.table.runtime.join

import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.util.Collector

/**
  * Collector to wrap a Row into a [[org.apache.flink.table.dataformat.BaseRow]]
  * and to track whether a row has been emitted by
  * the inner collector
  **/
class EmitAwareCollector extends Collector[BaseRow] {

  var emitted = false
  var innerCollector: Collector[BaseRow] = _

  def reset(): Unit = {
    emitted = false
  }

  /**
    * Emits a record.
    *
    * @param record The record to collect.
    */
  override def collect(record: BaseRow): Unit = {
    emitted = true
    innerCollector.collect(record)
  }

  /**
    * Closes the collector. If any data was buffered, that data will be flushed.
    */
  override def close(): Unit = {
    innerCollector.close()
  }
}

object EmitAwareCollector {

  val JOINED = 1.asInstanceOf[Byte]
  val NONJOINED = 0.asInstanceOf[Byte]

  def isJoined(baseRow: BaseRow): Boolean = {
    baseRow.getHeader == JOINED
  }
}
