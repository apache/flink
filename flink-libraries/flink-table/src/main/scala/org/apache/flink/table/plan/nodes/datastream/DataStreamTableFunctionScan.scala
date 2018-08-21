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

import java.lang.reflect.Type
import java.util.{List => JList, Set => JSet}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableFunctionScan
import org.apache.calcite.rel.metadata.RelColumnMapping
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment}
import org.apache.flink.table.codegen.SourceFunctionCodeGenerator
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.io.TableFunctionSource
import org.apache.flink.table.runtime.types.CRow

import scala.collection.JavaConverters._


/**
  * DataStream RelNode for LogicalTableFunctionScanc.
  */
class DataStreamTableFunctionScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputs: JList[RelNode],
    rexCall: RexNode,
    elementType: Type,
    rowType: RelDataType,
    columnMappings: JSet[RelColumnMapping],
    ruleDescription: String)
  extends TableFunctionScan(
    cluster,
    traitSet,
    inputs,
    rexCall,
    elementType,
    rowType,
    columnMappings)
  with DataStreamRel
  with StreamScan {

  override def deriveRowType(): RelDataType = rowType

  override def toString: String = {
    val call = rexCall.asInstanceOf[RexCall]
    val sqlFunction = call.getOperator.asInstanceOf[TableSqlFunction]
    val udtfName = sqlFunction.toString
    val operands = call.getOperands.asScala
      .map(getExpressionString(_, List(), None)).mkString(", ")
    s"table($udtfName($operands))"
  }

  override def copy(
    traitSet: RelTraitSet,
    inputs: JList[RelNode],
    rexCall: RexNode,
    elementType: Type,
    rowType: RelDataType,
    columnMappings: JSet[RelColumnMapping]): TableFunctionScan = {
      new DataStreamTableFunctionScan(
        cluster,
        traitSet,
        inputs,
        rexCall,
        elementType,
        rowType,
        columnMappings,
        ruleDescription
      )
  }

  override def translateToPlan(tableEnv: StreamTableEnvironment,
                               queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val sqlFunction = rexCall.asInstanceOf[RexCall].getOperator.asInstanceOf[TableSqlFunction]

    val udtfTypeInfo = sqlFunction.getRowTypeInfo.asInstanceOf[TypeInformation[Any]]

    val generator = new SourceFunctionCodeGenerator(tableEnv.getConfig, udtfTypeInfo)

    val result = generator.generateExpression(rexCall)

    val generatedSource = generator.generateSourceFunction(ruleDescription, result)

    val tableFunctionSource: SourceFunction[Any] =
      new TableFunctionSource(generatedSource.name, generatedSource.code, udtfTypeInfo)

    val dataStreamSource: DataStream[Any] =
      tableEnv
        .execEnv
        .addSource(tableFunctionSource)
        .setMaxParallelism(1)
        .setParallelism(1)
        .returns(udtfTypeInfo)

    val outputSchema = new RowSchema(rowType)

    convertToInternalRow(
      outputSchema,
      dataStreamSource,
      outputSchema.fieldNames.indices.toArray,
      tableEnv.getConfig,
      None)

  }
}
