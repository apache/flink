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
package org.apache.flink.table.codegen

import org.apache.calcite.rex.RexLiteral
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.dataview._
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.functions.UserDefinedAggregateFunction
import org.apache.flink.table.runtime.aggregate.GeneratedAggregations

/**
  * A code generator for generating [[GeneratedAggregations]].
  *
  * @param config                 configuration that determines runtime behavior
  * @param nullableInput          input(s) can be null.
  * @param input                  type information about the input of the Function
  * @param constants              constant expressions that act like a second input in the
  *                               parameter indices.
  * @param name                   Class name of the function.
  *                               Does not need to be unique but has to be a valid Java class
  *                               identifier.
  * @param physicalInputTypes     Physical input row types
  * @param aggregates             All aggregate functions
  * @param aggFields              Indexes of the input fields for all aggregate functions
  * @param aggMapping             The mapping of aggregates to output fields
  * @param isDistinctAggs         The flag array indicating whether it is distinct aggregate.
  * @param isStateBackedDataViews a flag to indicate if distinct filter uses state backend.
  * @param partialResults         A flag defining whether final or partial results (accumulators)
  *                               are set
  *                               to the output row.
  * @param fwdMapping             The mapping of input fields to output fields
  * @param mergeMapping           An optional mapping to specify the accumulators to merge. If not
  *                               set, we
  *                               assume that both rows have the accumulators at the same position.
  * @param outputArity            The number of fields in the output row.
  * @param needRetract            a flag to indicate if the aggregate needs the retract method
  * @param needMerge              a flag to indicate if the aggregate needs the merge method
  * @param needReset              a flag to indicate if the aggregate needs the resetAccumulator
  *                               method
  * @param accConfig              Data view specification for accumulators
  */
class AggregationCodeGenerator(
    config: TableConfig,
    nullableInput: Boolean,
    input: TypeInformation[_ <: Any],
    constants: Option[Seq[RexLiteral]],
    name: String,
    physicalInputTypes: Seq[TypeInformation[_]],
    aggregates: Array[UserDefinedAggregateFunction[_ <: Any, _ <: Any]],
    aggFields: Array[Array[Int]],
    aggMapping: Array[Int],
    isDistinctAggs: Array[Boolean],
    isStateBackedDataViews: Boolean,
    partialResults: Boolean,
    fwdMapping: Array[Int],
    mergeMapping: Option[Array[Int]],
    outputArity: Int,
    needRetract: Boolean,
    needMerge: Boolean,
    needReset: Boolean,
    accConfig: Option[Array[Seq[DataViewSpec[_]]]])
  extends BaseAggregationCodeGenerator(
    config,
    nullableInput,
    input,
    constants,
    name,
    physicalInputTypes,
    aggregates,
    aggFields,
    aggMapping,
    isDistinctAggs,
    isStateBackedDataViews,
    partialResults,
    fwdMapping,
    mergeMapping,
    outputArity,
    needRetract,
    needMerge,
    needReset,
    accConfig) {

  /**
    * Generates a [[GeneratedAggregations]] that can be passed to a Java compiler.
    *
    * @return A GeneratedAggregationsFunction
    */
  def generateAggregations: GeneratedAggregationsFunction = {

    init()
    val aggFuncCode = Seq(
      genSetAggregationResults,
      genAccumulate,
      genRetract,
      genCreateAccumulators,
      genSetForwardedFields,
      genCreateOutputRow,
      genMergeAccumulatorsPair,
      genResetAccumulator).mkString("\n")

    val generatedAggregationsClass = classOf[GeneratedAggregations].getCanonicalName
    val funcCode =
      j"""
         |public final class $funcName extends $generatedAggregationsClass {
         |
         |  ${reuseMemberCode()}
         |  $genMergeList
         |  public $funcName() throws Exception {
         |    ${reuseInitCode()}
         |  }
         |  ${reuseConstructorCode(funcName)}
         |
         |  public final void open(
         |    org.apache.flink.api.common.functions.RuntimeContext $contextTerm) throws Exception {
         |    ${reuseOpenCode()}
         |  }
         |
         |  $aggFuncCode
         |
         |  public final void cleanup() throws Exception {
         |    ${reuseCleanupCode()}
         |  }
         |
         |  public final void close() throws Exception {
         |    ${reuseCloseCode()}
         |  }
         |}
         """.stripMargin

    GeneratedAggregationsFunction(funcName, funcCode)
  }
}
