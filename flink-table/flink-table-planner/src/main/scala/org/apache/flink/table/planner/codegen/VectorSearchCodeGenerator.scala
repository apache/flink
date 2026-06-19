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
import org.apache.flink.table.functions._
import org.apache.flink.table.planner.codegen.FunctionCallCodeGenerator.GeneratedTableFunctionWithDataType
import org.apache.flink.table.planner.codegen.calls.BridgingFunctionGenUtil
import org.apache.flink.table.planner.functions.inference.FunctionCallContext
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil.FunctionParam
import org.apache.flink.table.runtime.collector.ListenableCollector
import org.apache.flink.table.runtime.generated.{GeneratedCollector, GeneratedFunction}
import org.apache.flink.table.types.inference.{TypeInference, TypeStrategies, TypeTransformations}
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.flink.table.types.utils.DataTypeUtils.transform
import org.apache.flink.util.Collector

import java.util

import scala.collection.JavaConverters._

object VectorSearchCodeGenerator {

  /** Generates a sync vector search function ([[TableFunction]]) */
  def generateSyncVectorSearchFunction(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      dataTypeFactory: DataTypeFactory,
      inputType: LogicalType,
      searchOutputType: LogicalType,
      outputType: LogicalType,
      searchColumns: util.List[FunctionParam],
      syncVectorSearchFunction: TableFunction[_],
      functionName: String,
      fieldCopy: Boolean): GeneratedFunction[FlatMapFunction[RowData, RowData]] = {
    FunctionCallCodeGenerator
      .generateSyncFunctionCall(
        tableConfig,
        classLoader,
        dataTypeFactory,
        inputType,
        searchOutputType,
        outputType,
        searchColumns,
        syncVectorSearchFunction,
        generateCallWithDataType(functionName, searchOutputType),
        functionName,
        "VectorSearchFunction",
        fieldCopy
      )
      .tableFunc
  }

  /** Generates a async vector search function ([[AsyncTableFunction]]) */
  def generateAsyncVectorSearchFunction(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      dataTypeFactory: DataTypeFactory,
      inputType: LogicalType,
      searchOutputType: LogicalType,
      outputType: LogicalType,
      searchColumns: util.List[FunctionParam],
      asyncVectorSearchFunction: AsyncTableFunction[_],
      functionName: String): GeneratedTableFunctionWithDataType[AsyncFunction[RowData, AnyRef]] = {
    FunctionCallCodeGenerator.generateAsyncFunctionCall(
      tableConfig,
      classLoader,
      dataTypeFactory,
      inputType,
      searchOutputType,
      outputType,
      searchColumns,
      asyncVectorSearchFunction,
      generateCallWithDataType(functionName, searchOutputType),
      functionName,
      "AsyncVectorSearchFunction"
    )
  }

  private def generateCallWithDataType(
      functionName: String,
      searchOutputType: LogicalType
  ) = (
      ctx: CodeGeneratorContext,
      callContext: FunctionCallContext,
      udf: UserDefinedFunction,
      operands: Seq[GeneratedExpression]) => {
    val inference = createVectorSearchTypeInference(callContext)
    BridgingFunctionGenUtil.generateFunctionAwareCallWithDataType(
      ctx,
      operands,
      searchOutputType,
      inference,
      callContext,
      udf,
      functionName,
      skipIfArgsNull = true
    )
  }

  private def createVectorSearchTypeInference(callContext: FunctionCallContext): TypeInference = {
    // for convenience, we assume internal or default external data structures
    // of expected logical types
    val defaultArgDataTypes = callContext.getArgumentDataTypes.asScala
    val defaultOutputDataType = callContext.getOutputDataType.get()

    val internalArgDataTypes = defaultArgDataTypes
      .map(dt => transform(dt, TypeTransformations.TO_INTERNAL_CLASS))
    val internalOutputDataType =
      transform(defaultOutputDataType, TypeTransformations.TO_INTERNAL_CLASS)
    TypeInference
      .newBuilder()
      .typedArguments(internalArgDataTypes.asJava)
      .outputTypeStrategy(TypeStrategies.explicit(internalOutputDataType))
      .build()
  }

  /** Generates collector for vector search ([[Collector]]) */
  def generateCollector(
      ctx: CodeGeneratorContext,
      inputType: RowType,
      searchOutputType: RowType,
      outputType: RowType): GeneratedCollector[ListenableCollector[RowData]] = {
    FunctionCallCodeGenerator.generateCollector(
      ctx,
      inputType,
      searchOutputType,
      outputType,
      Option.empty,
      Option.empty)
  }
}
