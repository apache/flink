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

package org.apache.flink.table.plan.schema

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.plan.stats.{FlinkStatistic, TableStats}

/**
  * Intermediate bounded stream is a intermediate result table of segmentation optimization.
  */
class IntermediateBoundedStreamTable[T](
    val rowType: RelDataType,
    dataStream: DataStream[T],
    fieldIndexes: Array[Int],
    fieldNames: Array[String],
    statistic: FlinkStatistic = FlinkStatistic.builder.tableStats((TableStats(1000L))).build())
  extends DataStreamTable[T](dataStream, false, false, fieldIndexes, fieldNames, statistic) {

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    // the row type of intermediate bounded stream should keep the same as logical plan
    // eg: Varchar(2000) -> String will lose type information.
    rowType
  }

  override def copy(statistic: FlinkStatistic): DataStreamTable[T] =
    new IntermediateBoundedStreamTable(rowType, dataStream, fieldIndexes, fieldNames, statistic)
}
