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
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.plan.stats.FlinkStatistic

class IntermediateDataStreamTable[T](
    val rowType: RelDataType,
    dataStream: DataStream[T],
    producesUpdates: Boolean,
    isAccRetract: Boolean,
    fieldIndexes: Array[Int],
    fieldNames: Array[String],
    statistic: FlinkStatistic = FlinkStatistic.UNKNOWN)
  extends DataStreamTable[T](
    dataStream,
    producesUpdates,
    isAccRetract,
    fieldIndexes,
    fieldNames,
    statistic){

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    // the row type of intermediate data stream should keep the same as optimized plan
    rowType
  }

  override def copy(statistic: FlinkStatistic): FlinkTable = {
    new IntermediateDataStreamTable[T](
      rowType, dataStream, producesUpdates, isAccRetract, fieldIndexes, fieldNames, statistic)
  }
}
