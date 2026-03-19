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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.inference.utils.CallContextMock;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link UrlDecodeFunction}. */
class UrlDecodeFunctionTest {

    private UrlDecodeFunction createFunction(boolean recursive) {
        CallContextMock callContextMock = new CallContextMock();
        callContextMock.functionDefinition =
                recursive
                        ? BuiltInFunctionDefinitions.URL_DECODE_RECURSIVE
                        : BuiltInFunctionDefinitions.URL_DECODE;
        callContextMock.argumentDataTypes = Collections.singletonList(DataTypes.STRING());
        callContextMock.argumentLiterals = Collections.singletonList(false);
        callContextMock.argumentNulls = Collections.singletonList(false);
        callContextMock.argumentValues = Collections.singletonList(Optional.empty());
        callContextMock.outputDataType = Optional.of(DataTypes.STRING().nullable());
        callContextMock.name =
                recursive
                        ? BuiltInFunctionDefinitions.URL_DECODE_RECURSIVE.getName()
                        : BuiltInFunctionDefinitions.URL_DECODE.getName();
        callContextMock.isGroupedAggregation = false;

        SpecializedFunction.SpecializedContext context =
                new SpecializedFunction.SpecializedContext() {
                    @Override
                    public CallContextMock getCallContext() {
                        return callContextMock;
                    }

                    @Override
                    public Configuration getConfiguration() {
                        return new Configuration();
                    }

                    @Override
                    public ClassLoader getBuiltInClassLoader() {
                        return Thread.currentThread().getContextClassLoader();
                    }

                    @Override
                    public SpecializedFunction.ExpressionEvaluator createEvaluator(
                            org.apache.flink.table.expressions.Expression expression,
                            org.apache.flink.table.types.DataType outputDataType,
                            DataTypes.Field... args) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public SpecializedFunction.ExpressionEvaluator createEvaluator(
                            String sqlExpression,
                            org.apache.flink.table.types.DataType outputDataType,
                            DataTypes.Field... args) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public SpecializedFunction.ExpressionEvaluator createEvaluator(
                            org.apache.flink.table.functions.BuiltInFunctionDefinition function,
                            org.apache.flink.table.types.DataType outputDataType,
                            org.apache.flink.table.types.DataType... args) {
                        throw new UnsupportedOperationException();
                    }
                };
        return new UrlDecodeFunction(context);
    }

    // === URL_DECODE_RECURSIVE with maxDepth edge cases ===

    @Test
    void testUrlDecodeRecursiveNullWithMaxDepth() {
        UrlDecodeFunction func = createFunction(true);
        // null input with maxDepth specified should return null
        StringData result = func.eval(null, 5);
        assertThat(result).isNull();
    }

    @Test
    void testUrlDecodeRecursiveIterationSucceedsThenFails() {
        UrlDecodeFunction func = createFunction(true);
        // First iteration succeeds (decodes %253A -> %3A), second iteration has illegal escape
        // Should return last successful result after at least one successful iteration
        StringData result = func.eval(StringData.fromString("test%253A%25"), 10);
        assertThat(result).isNotNull();
        // After first decode: test%3A% — second decode fails on %, so returns last successful
        assertThat(result.toString()).isEqualTo("test%3A%");
    }
}
