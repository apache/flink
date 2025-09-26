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
package org.apache.flink.table.planner.codegen

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.streaming.api.functions.async.AsyncFunction
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.data.RowData
import org.apache.flink.table.functions.{AsyncTableFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.table.planner.codegen.FunctionCallCodeGenerator.GeneratedTableFunctionWithDataType
import org.apache.flink.table.planner.codegen.calls.BridgingFunctionGenUtil
import org.apache.flink.table.planner.functions.inference.FunctionCallContext
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil.FunctionParam
import org.apache.flink.table.runtime.collector.ListenableCollector
import org.apache.flink.table.runtime.generated.{GeneratedCollector, GeneratedFunction}
import org.apache.flink.table.types.inference.{TypeInference, TypeStrategies, TypeTransformations}
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.flink.table.types.utils.DataTypeUtils.transform

import java.util

import scala.collection.JavaConverters._

object MLPredictCodeGenerator {

  /** Generates a predict function ([[TableFunction]]) */
  def generateSyncPredictFunction(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      dataTypeFactory: DataTypeFactory,
      inputType: LogicalType,
      predictFunctionOutputType: LogicalType,
      collectorOutputType: LogicalType,
      features: util.List[FunctionParam],
      syncPredictFunction: TableFunction[_],
      functionName: String,
      fieldCopy: Boolean
  ): GeneratedFunction[FlatMapFunction[RowData, RowData]] = {
    FunctionCallCodeGenerator
      .generateSyncFunctionCall(
        tableConfig,
        classLoader,
        dataTypeFactory,
        inputType,
        predictFunctionOutputType,
        collectorOutputType,
        features,
        syncPredictFunction,
        generateCallWithDataType(functionName, predictFunctionOutputType),
        functionName,
        "PredictFunction",
        fieldCopy
      )
      .tableFunc
  }

  /** Generates a async predict function ([[AsyncTableFunction]]) */
  def generateAsyncPredictFunction(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      dataTypeFactory: DataTypeFactory,
      inputType: LogicalType,
      predictFunctionOutputType: LogicalType,
      collectorOutputType: LogicalType,
      features: util.List[FunctionParam],
      asyncPredictFunction: AsyncTableFunction[_],
      functionName: String): GeneratedTableFunctionWithDataType[AsyncFunction[RowData, AnyRef]] = {
    FunctionCallCodeGenerator.generateAsyncFunctionCall(
      tableConfig,
      classLoader,
      dataTypeFactory,
      inputType,
      predictFunctionOutputType,
      collectorOutputType,
      features,
      asyncPredictFunction,
      generateCallWithDataType(functionName, predictFunctionOutputType),
      functionName,
      "AsyncPredictFunction"
    )
  }

  /** Generate a collector to collect to join the input row and predicted results. */
  def generateCollector(
      ctx: CodeGeneratorContext,
      inputRowType: RowType,
      predictFunctionOutputType: RowType,
      collectorOutputType: RowType
  ): GeneratedCollector[ListenableCollector[RowData]] = {
    FunctionCallCodeGenerator.generateCollector(
      ctx,
      inputRowType,
      predictFunctionOutputType,
      collectorOutputType,
      Option.empty,
      Option.empty
    )
  }

  private def generateCallWithDataType(
      functionName: String,
      modelOutputType: LogicalType
  ) = (
      ctx: CodeGeneratorContext,
      callContext: FunctionCallContext,
      udf: UserDefinedFunction,
      operands: Seq[GeneratedExpression]) => {
    val inference = TypeInference
      .newBuilder()
      .typedArguments(
        callContext.getArgumentDataTypes.asScala
          .map(dt => transform(dt, TypeTransformations.TO_INTERNAL_CLASS))
          .asJava)
      .outputTypeStrategy(TypeStrategies.explicit(
        transform(callContext.getOutputDataType.get(), TypeTransformations.TO_INTERNAL_CLASS)))
      .build()
    BridgingFunctionGenUtil.generateFunctionAwareCallWithDataType(
      ctx,
      operands,
      modelOutputType,
      inference,
      callContext,
      udf,
      functionName,
      skipIfArgsNull = false
    )
  }
}
