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

package org.apache.flink.table.plan.nodes.dataset

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.CommonScan
import org.apache.flink.table.plan.schema.FlinkTable
import org.apache.flink.table.runtime.MapRunner
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

trait BatchScan extends CommonScan[Row] with DataSetRel {

  protected def convertToInternalRow(
      input: DataSet[Any],
      flinkTable: FlinkTable[_],
      config: TableConfig)
    : DataSet[Row] = {

    val inputType = input.getType

    val internalType = FlinkTypeFactory.toInternalRowTypeInfo(getRowType)

    // conversion
    if (needsConversion(inputType, internalType)) {

      val function = generatedConversionFunction(
        config,
        classOf[MapFunction[Any, Row]],
        inputType,
        internalType,
        "DataSetSourceConversion",
        getRowType.getFieldNames,
        Some(flinkTable.fieldIndexes))

      val runner = new MapRunner[Any, Row](
        function.name,
        function.code,
        function.returnType)

      val opName = s"from: (${getRowType.getFieldNames.asScala.toList.mkString(", ")})"

      input.map(runner).name(opName)
    }
    // no conversion necessary, forward
    else {
      input.asInstanceOf[DataSet[Row]]
    }
  }
}
