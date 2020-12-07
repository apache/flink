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

package org.apache.flink.table.planner.codegen.calls

import org.apache.flink.table.functions.{ScalarFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.table.planner.codegen._
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext
import org.apache.flink.table.runtime.collector.WrappingCollector
import org.apache.flink.table.types.logical.LogicalType

import org.apache.calcite.rex.{RexCall, RexCallBinding}

import java.util.Collections

/**
 * Generates a call to a user-defined [[ScalarFunction]] or [[TableFunction]].
 *
 * Table functions are a special case because they are using a collector. Thus, the result of this
 * generator will be a reference to a [[WrappingCollector]]. Furthermore, atomic types are wrapped
 * into a row by the collector.
 */
class BridgingSqlFunctionCallGen(call: RexCall) extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType)
    : GeneratedExpression = {

    val function: BridgingSqlFunction = call.getOperator.asInstanceOf[BridgingSqlFunction]
    val udf: UserDefinedFunction = function.getDefinition.asInstanceOf[UserDefinedFunction]

    val inference = function.getTypeInference

    // we could have implemented a dedicated code generation context but the closer we are to
    // Calcite the more consistent is the type inference during the data type enrichment
    val callContext = new OperatorBindingCallContext(
      function.getDataTypeFactory,
      udf,
      RexCallBinding.create(
        function.getTypeFactory,
        call,
        Collections.emptyList()))

    BridgingFunctionGenUtil.generateFunctionAwareCall(
      ctx,
      operands,
      returnType,
      inference,
      callContext,
      udf,
      function.toString,
      skipIfArgsNull = false)
  }
}
