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

import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * The collector to wrap a [[Row]] into a [[CRow]] and collect it multiple times. This collector
  * can also used to count output record number and do lazy output.
  */
class CRowWrappingMultiOutputCollector() extends Collector[Row] {

  private var out: Collector[CRow] = _
  private val outCRow: CRow = new CRow(null, true)
  // times for collect
  private var times: Long = 0L
  // count how many records have been emitted
  private var emitCnt: Long = 0L
  // don't collect to downstream if set lazyOutput to true
  private var lazyOutput: Boolean = false

  def setCollector(collector: Collector[CRow]): Unit = this.out = collector

  def setChange(change: Boolean): Unit = this.outCRow.change = change

  def setRow(row: Row): Unit = this.outCRow.row = row

  def getRow(): Row = this.outCRow.row

  def setTimes(times: Long): Unit = this.times = times

  def setEmitCnt(emitted: Long): Unit = this.emitCnt = emitted

  def getEmitCnt(): Long = emitCnt

  def setLazyOutput(lazyOutput: Boolean): Unit = this.lazyOutput = lazyOutput

  override def collect(record: Row): Unit = {
    outCRow.row = record
    if (!lazyOutput) {
      emitCnt += times
      var i: Long = 0L
      while (i < times) {
        out.collect(outCRow)
        i += 1
      }
    }
  }

  def reset(): Unit = {
    this.outCRow.change = true
    this.times = 0
    this.emitCnt = 0
    this.lazyOutput = false
  }

  override def close(): Unit = out.close()
}
