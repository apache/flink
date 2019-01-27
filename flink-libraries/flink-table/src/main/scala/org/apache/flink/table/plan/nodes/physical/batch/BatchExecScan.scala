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

package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.types.DataType
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfig}
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.plan.nodes.common.CommonScan
import org.apache.flink.table.plan.nodes.exec.RowBatchExecNode
import org.apache.flink.table.util.Logging

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import java.util.{List => JList}

import scala.collection.JavaConversions._

trait BatchExecScan extends CommonScan[BinaryRow] with RowBatchExecNode with Logging {

   /**
    * Assign source for transformation.
    *
    */
  def assignSourceResourceAndParallelism(
      tableEnv: BatchTableEnvironment,
      input: StreamTransformation[Any]): Unit = {

    input.setParallelism(getResource.getParallelism)
    input.setResources(sourceResSpec, sourceResSpec)
    input.getTransitivePredecessors.foreach(t => tableEnv.getRUKeeper.addTransformation(this, t))
  }

  def convertToInternalRow(
      tableEnv: BatchTableEnvironment,
      input: StreamTransformation[Any],
      fieldIdxs: Array[Int],
      outRowType: RelDataType,
      dataType: DataType,
      qualifiedName: JList[String],
      config: TableConfig,
      rowtimeExpr: Option[RexNode]): StreamTransformation[BaseRow] = {
    if (needInternalConversion) {
      val ctx = CodeGeneratorContext(config, supportReference = true)
      val convertTransform = convertToInternalRow(
        ctx, input, fieldIdxs, dataType, outRowType, qualifiedName, config, rowtimeExpr)
      convertTransform.setResources(conversionResSpec, conversionResSpec)
      tableEnv.getRUKeeper.addTransformation(this, convertTransform)
      convertTransform
    } else {
      input.asInstanceOf[StreamTransformation[BaseRow]]
    }
  }

}
