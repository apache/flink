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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.{DefinedFieldMapping, TableSource, TableSourceValidation}
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks
import org.apache.flink.table.types.utils.{DataTypeUtils, TypeConversions}
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.table.utils.TypeMappingUtils

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.Statistic
import org.apache.calcite.schema.impl.AbstractTable

import java.util.function.{Function => JFunction}

/**
  * Abstract class which define the interfaces required to convert a [[TableSource]] to
  * a Calcite Table.
  *
  * @param tableSource The [[TableSource]] for which is converted to a Calcite Table.
  * @param isStreamingMode A flag that tells if the current table is in stream mode.
  * @param statistic The table statistics.
  */
class TableSourceTable[T](
    val tableSchema: TableSchema,
    val tableSource: TableSource[T],
    val isStreamingMode: Boolean,
    val statistic: FlinkStatistic)
  extends AbstractTable {

  TableSourceValidation.validateTableSource(tableSource, tableSchema)

  /**
    * Returns statistics of current table
    *
    * @return statistics of current table
    */
  override def getStatistic: Statistic = statistic

  // We must enrich logical schema from catalog table with physical type coming from table source.
  // Schema coming from catalog table might not have proper conversion classes. Those must be
  // extracted from produced type, before converting to RelDataType
  def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {

    val flinkTypeFactory = typeFactory.asInstanceOf[FlinkTypeFactory]

    val fieldNames = tableSchema.getFieldNames

    val nameMapping: JFunction[String, String] = tableSource match {
      case mapping: DefinedFieldMapping if mapping.getFieldMapping != null =>
        new JFunction[String, String] {
          override def apply(t: String): String = mapping.getFieldMapping.get(t)
        }
      case _ => JFunction.identity()
    }

    val producedDataType = tableSource.getProducedDataType
    val fieldIndexes = TypeMappingUtils.computePhysicalIndicesOrTimeAttributeMarkers(
      tableSource,
      tableSchema.getTableColumns,
      isStreamingMode,
      nameMapping
    )

    val typeInfos = if (LogicalTypeChecks.isCompositeType(producedDataType.getLogicalType)) {
      val physicalSchema = DataTypeUtils.expandCompositeTypeToSchema(producedDataType)
      fieldIndexes.map(mapIndex(_,
        idx =>
          TypeConversions.fromDataTypeToLegacyInfo(physicalSchema.getFieldDataType(idx).get()))
      )
    } else {
      fieldIndexes.map(mapIndex(_, _ => TypeConversions.fromDataTypeToLegacyInfo(producedDataType)))
    }

    flinkTypeFactory.buildLogicalRowType(fieldNames, typeInfos)
  }

  def mapIndex(idx: Int, mapNonMarker: Int => TypeInformation[_]): TypeInformation[_] = {
    idx match {
      case TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER => Types.SQL_TIMESTAMP()
      case TimeIndicatorTypeInfo.PROCTIME_BATCH_MARKER => Types.SQL_TIMESTAMP()
      case TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER =>
        TimeIndicatorTypeInfo.PROCTIME_INDICATOR
      case TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER => TimeIndicatorTypeInfo.ROWTIME_INDICATOR
      case _ =>
       mapNonMarker(idx)
    }
  }
}
