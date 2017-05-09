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

package org.apache.flink.table.plan.nodes.datastream

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.plan.nodes.CommonScan
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.types.Row
import org.apache.flink.table.plan.schema.FlinkTable
import org.apache.flink.table.runtime.CRowOutputMapRunner
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}

import scala.collection.JavaConverters._

trait StreamScan extends CommonScan[CRow] with DataStreamRel {

  protected def convertToInternalRow(
      schema: RowSchema,
      input: DataStream[Any],
      flinkTable: FlinkTable[_],
      config: TableConfig)
    : DataStream[CRow] = {

    val inputType = input.getType
    val internalType = CRowTypeInfo(schema.physicalTypeInfo)

    // conversion
    if (needsConversion(input.getType, internalType)) {

      val function = generatedConversionFunction(
        config,
        classOf[MapFunction[Any, Row]],
        inputType,
        schema.physicalTypeInfo,
        "DataStreamSourceConversion",
        schema.physicalFieldNames,
        Some(flinkTable.fieldIndexes))

      val mapFunc = new CRowOutputMapRunner(
        function.name,
        function.code,
        internalType)

      val opName = s"from: (${getRowType.getFieldNames.asScala.toList.mkString(", ")})"

      // TODO we need a ProcessFunction here
      input.map(mapFunc).name(opName).returns(internalType)
    }
    // no conversion necessary, forward
    else {
      input.asInstanceOf[DataStream[CRow]]
    }
  }
}
