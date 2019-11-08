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

package org.apache.flink.table.planner.plan.schema

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}

/**
  * The class that wraps [[DataStream]] as a Calcite Table.
  */
class DataStreamTable[T](
    val dataStream: DataStream[T],
    val producesUpdates: Boolean,
    val isAccRetract: Boolean,
    override val fieldIndexes: Array[Int],
    override val fieldNames: Array[String],
    statistic: FlinkStatistic = FlinkStatistic.UNKNOWN,
    fieldNullables: Option[Array[Boolean]] = None)
  extends InlineTable[T](
    fromLegacyInfoToDataType(dataStream.getType), fieldIndexes, fieldNames, statistic) {

  // This is only used for bounded stream now, we supply default statistic.
  def this(
      stream: DataStream[T],
      fieldIndexes: Array[Int],
      fieldNames: Array[String]) {
    this(stream, false, false, fieldIndexes, fieldNames)
  }

  def this(
      stream: DataStream[T],
      fieldIndexes: Array[Int],
      fieldNames: Array[String],
      fieldNullables: Option[Array[Boolean]]) {
    this(stream, false, false, fieldIndexes, fieldNames, fieldNullables = fieldNullables)
  }

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    fieldNullables match {
      case Some(nulls) => typeFactory.asInstanceOf[FlinkTypeFactory]
          .buildRelNodeRowType(fieldNames, fieldTypes.zip(nulls).map {
            case (t, nullable) => t.copy(nullable)
          })
      case _ => super.getRowType(typeFactory)
    }
  }

  /**
    * Creates a copy of this table, changing statistic.
    *
    * @param statistic A new FlinkStatistic.
    * @return Copy of this table, substituting statistic.
    */
  override def copy(statistic: FlinkStatistic): FlinkTable =
    new DataStreamTable(
      dataStream, producesUpdates, isAccRetract, fieldIndexes, fieldNames, statistic)
}
