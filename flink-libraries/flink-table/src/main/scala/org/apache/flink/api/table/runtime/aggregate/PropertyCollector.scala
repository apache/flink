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

package org.apache.flink.api.table.runtime.aggregate

import org.apache.flink.api.table.Row
import org.apache.flink.util.Collector

/**
  * Adds properties to the end of a row before it emits it to the final collector.
  * The collector assumes that the row has placeholders at the end that can be filled.
  */
class PropertyCollector(properties: Array[WindowPropertyRead[_ <: Any]]) extends Collector[Row] {
  var finalCollector: Collector[Row] = _

  override def collect(record: Row): Unit = {
    var i: Int = 0
    while (i < properties.length) {
      val idx = record.productArity - properties.length + i
      record.setField(idx, properties(i).get())
      i = i + 1
    }
    finalCollector.collect(record)
  }

  override def close(): Unit = finalCollector.close()
}
