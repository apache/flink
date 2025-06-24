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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.calc.async.DelegatingAsyncResultFuture;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.commons.text.StringSubstitutor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Generates an {@link AsyncFunction} which can be used to evaluate calc projections from an async
 * scalar function.
 */
public class AsyncCodeGenerator {

    public static final String DEFAULT_EXCEPTION_TERM = "e";
    public static final String DEFAULT_DELEGATING_FUTURE_TERM = "f";

    /**
     * Creates a generated function which produces an {@link AsyncFunction} which executes the calc
     * projections.
     *
     * @param name The name used to generate the underlying function name
     * @param inputType The RowType of the input RowData
     * @param returnType The RowType of the resulting RowData
     * @param retainHeader If the header of the row should be retained
     * @param calcProjection The list of projections to be executed by this function
     * @param tableConfig The table configuration
     * @param classLoader The classloader to use while resolving classes
     * @return A {@link GeneratedFunction} returning an {@link AsyncFunction} executing the given
     *     list of projections
     */
    public static GeneratedFunction<AsyncFunction<RowData, RowData>> generateFunction(
            String name,
            RowType inputType,
            RowType returnType,
            List<RexNode> calcProjection,
            boolean retainHeader,
            ReadableConfig tableConfig,
            ClassLoader classLoader) {
        CodeGeneratorContext ctx = new CodeGeneratorContext(tableConfig, classLoader);
        String processCode =
                generateProcessCode(
                        ctx,
                        inputType,
                        returnType,
                        calcProjection,
                        retainHeader,
                        CodeGenUtils.DEFAULT_INPUT1_TERM(),
                        CodeGenUtils.DEFAULT_COLLECTOR_TERM(),
                        DEFAULT_EXCEPTION_TERM,
                        CodeGenUtils.DEFAULT_OUT_RECORD_TERM(),
                        DEFAULT_DELEGATING_FUTURE_TERM);
        return FunctionCodeGenerator.generateFunction(
                ctx,
                name,
                getFunctionClass(),
                processCode,
                returnType,
                inputType,
                CodeGenUtils.DEFAULT_INPUT1_TERM(),
                JavaScalaConversionUtil.toScala(Optional.empty()),
                JavaScalaConversionUtil.toScala(Optional.empty()),
                CodeGenUtils.DEFAULT_COLLECTOR_TERM(),
                CodeGenUtils.DEFAULT_CONTEXT_TERM());
    }

    @SuppressWarnings("unchecked")
    private static Class<AsyncFunction<RowData, RowData>> getFunctionClass() {
        return (Class<AsyncFunction<RowData, RowData>>) (Object) AsyncFunction.class;
    }

    private static String generateProcessCode(
            CodeGeneratorContext ctx,
            RowType inputType,
            RowType outRowType,
            List<RexNode> projection,
            boolean retainHeader,
            String inputTerm,
            String collectorTerm,
            String errorTerm,
            String recordTerm,
            String delegatingFutureTerm) {

        projection.forEach(n -> n.accept(new AsyncScalarFunctionsValidator()));

        ExprCodeGenerator exprGenerator =
                new ExprCodeGenerator(ctx, false)
                        .bindInput(
                                inputType,
                                inputTerm,
                                JavaScalaConversionUtil.toScala(Optional.empty()));

        List<GeneratedExpression> projectionExprs =
                projection.stream()
                        .map(exprGenerator::generateExpression)
                        .collect(Collectors.toList());
        int index = 0;
        StringBuilder metadataInvocations = new StringBuilder();
        StringBuilder asyncInvocation = new StringBuilder();
        if (retainHeader) {
            metadataInvocations.append(
                    String.format("%s.setRowKind(rowKind);\n", delegatingFutureTerm));
        }
        for (GeneratedExpression fieldExpr : projectionExprs) {
            if (fieldExpr.resultTerm().isEmpty()) {
                asyncInvocation.append(fieldExpr.code());
                metadataInvocations.append(
                        String.format("%s.addAsyncIndex(%d);\n", delegatingFutureTerm, index));
            } else {
                metadataInvocations.append(fieldExpr.code());
                metadataInvocations.append(
                        String.format(
                                "%s.addSynchronousResult(%d, %s);\n",
                                delegatingFutureTerm, index, fieldExpr.resultTerm()));
            }
            index++;
        }

        Map<String, String> values = new HashMap<>();
        values.put("delegatingFutureTerm", delegatingFutureTerm);
        values.put("delegatingFutureType", DelegatingAsyncResultFuture.class.getCanonicalName());
        values.put("collectorTerm", collectorTerm);
        values.put("typeTerm", GenericRowData.class.getCanonicalName());
        values.put("recordTerm", recordTerm);
        values.put("inputTerm", inputTerm);
        values.put("fieldCount", Integer.toString(LogicalTypeChecks.getFieldCount(outRowType)));
        values.put("metadataInvocations", metadataInvocations.toString());
        values.put("asyncInvocation", asyncInvocation.toString());
        values.put("errorTerm", errorTerm);

        return StringSubstitutor.replace(
                String.join(
                        "\n",
                        new String[] {
                            "final ${delegatingFutureType} ${delegatingFutureTerm} ",
                            "    = new ${delegatingFutureType}(${collectorTerm}, ${fieldCount});",
                            "final org.apache.flink.types.RowKind rowKind = ${inputTerm}.getRowKind();\n",
                            "try {",
                            // Ensure that metadata setup come first so that we know that they're
                            // available when the async callback occurs.
                            "  ${metadataInvocations}",
                            "  ${asyncInvocation}",
                            "",
                            "} catch (Throwable ${errorTerm}) {",
                            "  ${collectorTerm}.completeExceptionally(${errorTerm});",
                            "}"
                        }),
                values);
    }

    private static class AsyncScalarFunctionsValidator extends RexVisitorImpl<Void> {
        public AsyncScalarFunctionsValidator() {
            super(true);
        }

        @Override
        public Void visitCall(RexCall call) {
            super.visitCall(call);

            if (call.getOperator() instanceof BridgingSqlFunction
                    && ((BridgingSqlFunction) call.getOperator()).getDefinition().getKind()
                            != FunctionKind.ASYNC_SCALAR) {
                throw new CodeGenException(
                        "Invalid use of function "
                                + call.getOperator()
                                + "."
                                + "Code generation should only be done with async calls");
            }
            return null;
        }
    }
}
