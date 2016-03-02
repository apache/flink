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

import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.api.table.Row

class CountAggregate extends Aggregate[Long] {

  override def initiate(intermediate: Row): Unit = {
    intermediate.setField(aggOffsetInRow, 0L)
  }

  override def merge(intermediate: Row, buffer: Row): Unit = {
    val partialCount = intermediate.productElement(aggOffsetInRow).asInstanceOf[Long]
    val bufferCount = buffer.productElement(aggOffsetInRow).asInstanceOf[Long]
    buffer.setField(aggOffsetInRow, partialCount + bufferCount)
  }

  override def evaluate(buffer: Row): Long = {
    buffer.productElement(aggOffsetInRow).asInstanceOf[Long]
  }

  override def prepare(value: Any, intermediate: Row): Unit = {
    intermediate.setField(aggOffsetInRow, 1L)
  }

  override def intermediateDataType: Array[SqlTypeName] = {
    Array(SqlTypeName.BIGINT)
  }

  override def supportPartial: Boolean = true
}
