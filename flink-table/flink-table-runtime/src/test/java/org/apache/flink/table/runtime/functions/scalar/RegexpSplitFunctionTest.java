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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.ExpressionEvaluator;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.utils.CallContextMock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RegexpSplitFunction}. */
class RegexpSplitFunctionTest {

    @ParameterizedTest
    @ValueSource(strings = {"(", "["})
    void testInvalidLiteralSkipsInputConversion(String regex) throws Exception {
        final RegexpSplitFunction function = createFunction(regex);
        function.open(new FunctionContext(null));
        final StringData input =
                new StringData() {
                    @Override
                    public int compareTo(StringData other) {
                        throw new AssertionError("Invalid literal must skip input conversion");
                    }

                    @Override
                    public byte[] toBytes() {
                        throw new AssertionError("Invalid literal must skip input conversion");
                    }

                    @Override
                    public String toString() {
                        throw new AssertionError("Invalid literal must skip input conversion");
                    }
                };
        for (int i = 0; i < 3; i++) {
            assertThat(function.eval(input, StringData.fromString(regex))).isNull();
        }
    }

    @Test
    void testValidLiteralCanBeEvaluatedRepeatedly() throws Exception {
        final RegexpSplitFunction function = createFunction(",");
        function.open(new FunctionContext(null));
        for (int i = 0; i < 3; i++) {
            final ArrayData result =
                    function.eval(StringData.fromString("a,b,"), StringData.fromString(","));
            assertThat(result.size()).isEqualTo(3);
            assertThat(result.getString(0).toString()).isEqualTo("a");
            assertThat(result.getString(1).toString()).isEqualTo("b");
            assertThat(result.getString(2).toString()).isEmpty();
        }
    }

    @Test
    void testDynamicPatternCanChangeAfterFailure() throws Exception {
        final RegexpSplitFunction function = createFunction(null);
        function.open(new FunctionContext(null));
        final StringData input = StringData.fromString("a,b");
        assertThat(function.eval(input, StringData.fromString("("))).isNull();
        final ArrayData result = function.eval(input, StringData.fromString(","));
        assertThat(result.size()).isEqualTo(2);
        assertThat(result.getString(0).toString()).isEqualTo("a");
        assertThat(result.getString(1).toString()).isEqualTo("b");
        assertThat(function.eval(null, StringData.fromString(","))).isNull();
        assertThat(function.eval(input, null)).isNull();
    }

    @Test
    void testEmptyLiteralPreservesCodePointsAndEmptyInput() throws Exception {
        final RegexpSplitFunction function = createFunction("");
        function.open(new FunctionContext(null));
        final StringData regex = StringData.fromString("");
        final ArrayData codePoints = function.eval(StringData.fromString("\uD801\uDC37a"), regex);
        assertThat(codePoints.size()).isEqualTo(2);
        assertThat(codePoints.getString(0).toString()).isEqualTo("\uD801\uDC37");
        assertThat(codePoints.getString(1).toString()).isEqualTo("a");
        final ArrayData emptyInput = function.eval(StringData.fromString(""), regex);
        assertThat(emptyInput.size()).isEqualTo(1);
        assertThat(emptyInput.getString(0).toString()).isEmpty();
    }

    private static RegexpSplitFunction createFunction(String literalRegex) {
        final CallContextMock callContext = new CallContextMock();
        callContext.argumentDataTypes = List.of(DataTypes.STRING(), DataTypes.STRING());
        callContext.outputDataType = Optional.of(DataTypes.ARRAY(DataTypes.STRING()));
        callContext.argumentLiterals = List.of(false, literalRegex != null);
        callContext.argumentValues = List.of(Optional.empty(), Optional.ofNullable(literalRegex));
        callContext.functionDefinition = BuiltInFunctionDefinitions.REGEXP_SPLIT;
        final SpecializedContext context =
                new SpecializedContext() {
                    @Override
                    public CallContext getCallContext() {
                        return callContext;
                    }

                    @Override
                    public ReadableConfig getConfiguration() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public ClassLoader getBuiltInClassLoader() {
                        return getClass().getClassLoader();
                    }

                    @Override
                    public ExpressionEvaluator createEvaluator(
                            Expression expression, DataType outputType, DataTypes.Field... args) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public ExpressionEvaluator createEvaluator(
                            String expression, DataType outputType, DataTypes.Field... args) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public ExpressionEvaluator createEvaluator(
                            BuiltInFunctionDefinition function,
                            DataType outputType,
                            DataType... args) {
                        throw new UnsupportedOperationException();
                    }
                };
        return new RegexpSplitFunction(context);
    }
}
