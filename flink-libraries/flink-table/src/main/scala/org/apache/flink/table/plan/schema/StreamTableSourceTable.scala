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
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.{DefinedProctimeAttribute, DefinedRowtimeAttribute, TableSource}
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

class StreamTableSourceTable[T](
    override val tableSource: TableSource[T],
    override val statistic: FlinkStatistic = FlinkStatistic.UNKNOWN)
  extends TableSourceTable[T](tableSource, statistic) {


  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val flinkTypeFactory = typeFactory.asInstanceOf[FlinkTypeFactory]

    val fieldNames = TableEnvironment.getFieldNames(tableSource).toList
    val fieldTypes = TableEnvironment.getFieldTypes(tableSource.getReturnType).toList

    val fields = fieldNames.zip(fieldTypes)

    val withRowtime = tableSource match {
      case timeSource: DefinedRowtimeAttribute if timeSource.getRowtimeAttribute == null =>
        fields
      case timeSource: DefinedRowtimeAttribute if timeSource.getRowtimeAttribute.trim.equals("") =>
        throw TableException("The name of the rowtime attribute must not be empty.")
      case timeSource: DefinedRowtimeAttribute =>
        val rowtimeAttribute = timeSource.getRowtimeAttribute
        fields :+ (rowtimeAttribute, TimeIndicatorTypeInfo.ROWTIME_INDICATOR)
      case _ =>
        fields
    }

    val withProctime = tableSource match {
      case timeSource : DefinedProctimeAttribute if timeSource.getProctimeAttribute == null =>
        withRowtime
      case timeSource: DefinedProctimeAttribute
        if timeSource.getProctimeAttribute.trim.equals("") =>
        throw TableException("The name of the rowtime attribute must not be empty.")
      case timeSource: DefinedProctimeAttribute =>
        val proctimeAttribute = timeSource.getProctimeAttribute
        withRowtime :+ (proctimeAttribute, TimeIndicatorTypeInfo.PROCTIME_INDICATOR)
      case _ =>
        withRowtime
    }

    val (fieldNamesWithIndicators, fieldTypesWithIndicators) = withProctime.unzip

    flinkTypeFactory.buildLogicalRowType(
      fieldNamesWithIndicators,
      fieldTypesWithIndicators)

  }

}
