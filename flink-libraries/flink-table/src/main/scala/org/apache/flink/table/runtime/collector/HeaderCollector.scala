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

package org.apache.flink.table.runtime.collector

import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.dataformat.util.BaseRowUtil
import org.apache.flink.util.Collector

/**
  * The collector is used to set [[BaseRow]] with the change flag.
  */
class HeaderCollector[T <: BaseRow] extends Collector[T] {

  var out: Collector[T] = _
  var header: Byte = 0

  def setHeader(header: Byte): Unit = this.header = header

  def setAccumulate(): Unit = this.header = BaseRowUtil.ACCUMULATE_MSG

  def setRetract(): Unit = this.header = BaseRowUtil.RETRACT_MSG

  override def collect(record: T): Unit = {
    record.setHeader(header)
    out.collect(record)
  }

  override def close(): Unit = out.close()
}

class CachedCollector[T <: BaseRow] extends Collector[T] {
  private var joinCnt: Int = _

  override def collect(record: T): Unit = {
    joinCnt += 1
  }

  def reset(): Unit = {
    joinCnt = 0
  }

  def getJoinCnt(): Int = joinCnt

  override def close(): Unit = {}
}
