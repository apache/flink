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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.planner.codegen.calls.BridgingFunctionGenUtil;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import scala.Option;
import scala.Tuple3;

/**
 * Generates an {@link AsyncFunction} which can be used to evaluate correlate invocations from an
 * async table function.
 */
public class AsyncCorrelateCodeGenerator {

    public static GeneratedFunction<AsyncFunction<RowData, Object>> generateFunction(
            String name,
            RowType inputType,
            RowType returnType,
            RexCall invocation,
            ReadableConfig tableConfig,
            ClassLoader classLoader) {
        CodeGeneratorContext ctx = new CodeGeneratorContext(tableConfig, classLoader);
        Tuple2<String, Optional<String>> processCode =
                generateProcessCode(
                        ctx, inputType, returnType, invocation, CodeGenUtils.DEFAULT_INPUT1_TERM());
        return FunctionCodeGenerator.generateFunction(
                ctx,
                name,
                getFunctionClass(),
                processCode.f0,
                returnType,
                inputType,
                CodeGenUtils.DEFAULT_INPUT1_TERM(),
                JavaScalaConversionUtil.toScala(Optional.empty()),
                JavaScalaConversionUtil.toScala(Optional.empty()),
                CodeGenUtils.DEFAULT_COLLECTOR_TERM(),
                CodeGenUtils.DEFAULT_CONTEXT_TERM(),
                JavaScalaConversionUtil.toScala(processCode.f1));
    }

    @SuppressWarnings("unchecked")
    private static Class<AsyncFunction<RowData, Object>> getFunctionClass() {
        return (Class<AsyncFunction<RowData, Object>>) (Object) AsyncFunction.class;
    }

    private static Tuple2<String, Optional<String>> generateProcessCode(
            CodeGeneratorContext ctx,
            RowType inputType,
            RowType returnType,
            RexCall invocation,
            String inputTerm) {
        invocation.accept(new AsyncCorrelateFunctionsValidator());

        ExprCodeGenerator exprGenerator =
                new ExprCodeGenerator(ctx, false)
                        .bindInput(
                                inputType,
                                inputTerm,
                                JavaScalaConversionUtil.toScala(Optional.empty()));

        // Generate operand expressions explicitly so we can call the timeout-aware helper below.
        // Letting `generateExpression(invocation)` walk the call instead would route through
        // BridgingSqlFunctionCallGen, which discards the optional `timeout(...)` call we need to
        // render into the generated AsyncFunction subclass.
        List<GeneratedExpression> operands = new ArrayList<>();
        for (RexNode operand : invocation.getOperands()) {
            operands.add(exprGenerator.generateExpression(operand));
        }

        Tuple3<GeneratedExpression, Option<GeneratedExpression>, DataType> result =
                BridgingFunctionGenUtil.generateBridgingFunctionCallWithTimeout(
                        ctx,
                        invocation,
                        // rexProgram is null: correlate calls operate on direct RexCall operands
                        // without a RexProgram indirection (no Calc on top of the correlate).
                        null,
                        JavaScalaConversionUtil.toScala(operands),
                        returnType,
                        false);

        Option<GeneratedExpression> timeoutCall = result._2();
        Optional<String> timeoutBodyCode =
                timeoutCall.isDefined() ? Optional.of(timeoutCall.get().code()) : Optional.empty();
        return Tuple2.of(result._1().code(), timeoutBodyCode);
    }

    private static class AsyncCorrelateFunctionsValidator extends RexVisitorImpl<Void> {
        public AsyncCorrelateFunctionsValidator() {
            super(true);
        }

        @Override
        public Void visitCall(RexCall call) {
            super.visitCall(call);

            if (call.getOperator() instanceof BridgingSqlFunction
                    && ((BridgingSqlFunction) call.getOperator()).getDefinition().getKind()
                            != FunctionKind.ASYNC_TABLE) {
                throw new CodeGenException(
                        "Invalid use of function "
                                + call.getOperator()
                                + "."
                                + "Code generation should only be done with async table calls");
            }
            return null;
        }
    }
}
