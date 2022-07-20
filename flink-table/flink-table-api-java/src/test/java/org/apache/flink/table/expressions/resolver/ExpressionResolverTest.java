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

package org.apache.flink.table.expressions.resolver;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SourceQueryOperation;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.table.utils.FunctionLookupMock;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.col;
import static org.apache.flink.table.api.Expressions.range;
import static org.apache.flink.table.api.Expressions.withColumns;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for resolving expressions with {@link ExpressionResolver} created with Expression DSL. See
 * also {@link FunctionLookupMock} for a set of supported functions.
 */
class ExpressionResolverTest {

    static Stream<Arguments> parameters() {
        return Stream.of(
                Arguments.of(
                        TestSpec.test("Columns range")
                                .inputSchemas(
                                        TableSchema.builder()
                                                .field("f0", DataTypes.BIGINT())
                                                .field("f1", DataTypes.STRING())
                                                .field("f2", DataTypes.SMALLINT())
                                                .build())
                                .select(withColumns(range("f1", "f2")), withColumns(range(1, 2)))
                                .equalTo(
                                        new FieldReferenceExpression(
                                                "f1", DataTypes.STRING(), 0, 1),
                                        new FieldReferenceExpression(
                                                "f2", DataTypes.SMALLINT(), 0, 2),
                                        new FieldReferenceExpression(
                                                "f0", DataTypes.BIGINT(), 0, 0),
                                        new FieldReferenceExpression(
                                                "f1", DataTypes.STRING(), 0, 1))),
                Arguments.of(
                        TestSpec.test("Flatten call")
                                .inputSchemas(
                                        TableSchema.builder()
                                                .field(
                                                        "f0",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "n0", DataTypes.BIGINT()),
                                                                DataTypes.FIELD(
                                                                        "n1", DataTypes.STRING())))
                                                .build())
                                .select($("f0").flatten())
                                .equalTo(
                                        CallExpression.permanent(
                                                BuiltInFunctionDefinitions.GET,
                                                Arrays.asList(
                                                        new FieldReferenceExpression(
                                                                "f0",
                                                                DataTypes.ROW(
                                                                        DataTypes.FIELD(
                                                                                "n0",
                                                                                DataTypes.BIGINT()),
                                                                        DataTypes.FIELD(
                                                                                "n1",
                                                                                DataTypes
                                                                                        .STRING())),
                                                                0,
                                                                0),
                                                        new ValueLiteralExpression("n0")),
                                                DataTypes.BIGINT()),
                                        CallExpression.permanent(
                                                BuiltInFunctionDefinitions.GET,
                                                Arrays.asList(
                                                        new FieldReferenceExpression(
                                                                "f0",
                                                                DataTypes.ROW(
                                                                        DataTypes.FIELD(
                                                                                "n0",
                                                                                DataTypes.BIGINT()),
                                                                        DataTypes.FIELD(
                                                                                "n1",
                                                                                DataTypes
                                                                                        .STRING())),
                                                                0,
                                                                0),
                                                        new ValueLiteralExpression("n1")),
                                                DataTypes.STRING()))),
                Arguments.of(
                        TestSpec.test("Builtin function calls")
                                .inputSchemas(
                                        TableSchema.builder()
                                                .field("f0", DataTypes.INT())
                                                .field("f1", DataTypes.DOUBLE())
                                                .build())
                                .select($("f0").isEqual($("f1")))
                                .equalTo(
                                        CallExpression.permanent(
                                                BuiltInFunctionDefinitions.EQUALS,
                                                Arrays.asList(
                                                        new FieldReferenceExpression(
                                                                "f0", DataTypes.INT(), 0, 0),
                                                        new FieldReferenceExpression(
                                                                "f1", DataTypes.DOUBLE(), 0, 1)),
                                                DataTypes.BOOLEAN()))),
                Arguments.of(
                        TestSpec.test("Lookup legacy scalar function call")
                                .inputSchemas(
                                        TableSchema.builder().field("f0", DataTypes.INT()).build())
                                .lookupFunction(
                                        "func",
                                        new ScalarFunctionDefinition(
                                                "func", new LegacyScalarFunc()))
                                .select(call("func", 1, $("f0")))
                                .equalTo(
                                        CallExpression.permanent(
                                                FunctionIdentifier.of("func"),
                                                new ScalarFunctionDefinition(
                                                        "func", new LegacyScalarFunc()),
                                                Arrays.asList(
                                                        valueLiteral(1),
                                                        new FieldReferenceExpression(
                                                                "f0", DataTypes.INT(), 0, 0)),
                                                DataTypes.INT().bridgedTo(Integer.class)))),
                Arguments.of(
                        TestSpec.test("Lookup system function call")
                                .inputSchemas(
                                        TableSchema.builder().field("f0", DataTypes.INT()).build())
                                .lookupFunction("func", new ScalarFunc())
                                .select(call("func", 1, $("f0")))
                                .equalTo(
                                        CallExpression.permanent(
                                                FunctionIdentifier.of("func"),
                                                new ScalarFunc(),
                                                Arrays.asList(
                                                        valueLiteral(1),
                                                        new FieldReferenceExpression(
                                                                "f0", DataTypes.INT(), 0, 0)),
                                                DataTypes.INT().notNull().bridgedTo(int.class)))),
                Arguments.of(
                        TestSpec.test("Inline function call via a class")
                                .inputSchemas(
                                        TableSchema.builder().field("f0", DataTypes.INT()).build())
                                .select(call(ScalarFunc.class, 1, $("f0")))
                                .equalTo(
                                        CallExpression.anonymous(
                                                new ScalarFunc(),
                                                Arrays.asList(
                                                        valueLiteral(1),
                                                        new FieldReferenceExpression(
                                                                "f0", DataTypes.INT(), 0, 0)),
                                                DataTypes.INT().notNull().bridgedTo(int.class)))),
                Arguments.of(
                        TestSpec.test("Lookup catalog function call")
                                .inputSchemas(
                                        TableSchema.builder().field("f0", DataTypes.INT()).build())
                                .lookupFunction(
                                        ObjectIdentifier.of("cat", "db", "func"), new ScalarFunc())
                                .select(call("cat.db.func", 1, $("f0")))
                                .equalTo(
                                        CallExpression.permanent(
                                                FunctionIdentifier.of(
                                                        ObjectIdentifier.of("cat", "db", "func")),
                                                new ScalarFunc(),
                                                Arrays.asList(
                                                        valueLiteral(1),
                                                        new FieldReferenceExpression(
                                                                "f0", DataTypes.INT(), 0, 0)),
                                                DataTypes.INT().notNull().bridgedTo(int.class)))),
                Arguments.of(
                        TestSpec.test("Deeply nested user-defined inline calls")
                                .inputSchemas(
                                        TableSchema.builder().field("f0", DataTypes.INT()).build())
                                .lookupFunction("func", new ScalarFunc())
                                .select(
                                        call(
                                                "func",
                                                call(new ScalarFunc(), call("func", 1, $("f0")))))
                                .equalTo(
                                        CallExpression.permanent(
                                                FunctionIdentifier.of("func"),
                                                new ScalarFunc(),
                                                Collections.singletonList(
                                                        CallExpression.anonymous(
                                                                new ScalarFunc(),
                                                                Collections.singletonList(
                                                                        CallExpression.permanent(
                                                                                FunctionIdentifier
                                                                                        .of("func"),
                                                                                new ScalarFunc(),
                                                                                Arrays.asList(
                                                                                        valueLiteral(
                                                                                                1),
                                                                                        new FieldReferenceExpression(
                                                                                                "f0",
                                                                                                DataTypes
                                                                                                        .INT(),
                                                                                                0,
                                                                                                0)),
                                                                                DataTypes.INT()
                                                                                        .notNull()
                                                                                        .bridgedTo(
                                                                                                int
                                                                                                        .class))),
                                                                DataTypes.INT()
                                                                        .notNull()
                                                                        .bridgedTo(int.class))),
                                                DataTypes.INT().notNull().bridgedTo(int.class)))),
                Arguments.of(
                        TestSpec.test("Star expression as parameter of user-defined func")
                                .inputSchemas(
                                        TableSchema.builder()
                                                .field("f0", DataTypes.INT())
                                                .field("f1", DataTypes.STRING())
                                                .build())
                                .lookupFunction("func", new ScalarFunc())
                                .select(call("func", $("*")))
                                .equalTo(
                                        CallExpression.permanent(
                                                FunctionIdentifier.of("func"),
                                                new ScalarFunc(),
                                                Arrays.asList(
                                                        new FieldReferenceExpression(
                                                                "f0", DataTypes.INT(), 0, 0),
                                                        new FieldReferenceExpression(
                                                                "f1", DataTypes.STRING(), 0, 1)),
                                                DataTypes.INT().notNull().bridgedTo(int.class)))),
                Arguments.of(
                        TestSpec.test("Test field reference with col()")
                                .inputSchemas(
                                        TableSchema.builder().field("i", DataTypes.INT()).build())
                                .select(col("i"))
                                .equalTo(
                                        new FieldReferenceExpression("i", DataTypes.INT(), 0, 0))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void testResolvingExpressions(TestSpec testSpec) {
        List<ResolvedExpression> resolvedExpressions =
                testSpec.getResolver().resolve(Arrays.asList(testSpec.expressions));
        assertThat(resolvedExpressions).isEqualTo(testSpec.expectedExpressions);
    }

    /** Test scalar function. */
    @FunctionHint(
            input = @DataTypeHint(inputGroup = InputGroup.ANY),
            isVarArgs = true,
            output = @DataTypeHint(value = "INTEGER NOT NULL", bridgedTo = int.class))
    public static class ScalarFunc extends ScalarFunction {
        public int eval(Object... any) {
            return 0;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ScalarFunc;
        }
    }

    /** Legacy scalar function. */
    public static class LegacyScalarFunc extends ScalarFunction {
        public int eval(Object... any) {
            return 0;
        }

        @Override
        public TypeInformation<?> getResultType(Class<?>[] signature) {
            return Types.INT;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ScalarFunc;
        }
    }

    private static class TestSpec {
        private final String description;
        private TableSchema[] schemas;
        private Expression[] expressions;
        private List<ResolvedExpression> expectedExpressions;
        private Map<FunctionIdentifier, FunctionDefinition> functions = new HashMap<>();

        private TestSpec(String description) {
            this.description = description;
        }

        public static TestSpec test(String description) {
            return new TestSpec(description);
        }

        public TestSpec inputSchemas(TableSchema... schemas) {
            this.schemas = schemas;
            return this;
        }

        public TestSpec lookupFunction(String name, FunctionDefinition functionDefinition) {
            functions.put(FunctionIdentifier.of(name), functionDefinition);
            return this;
        }

        public TestSpec lookupFunction(
                ObjectIdentifier identifier, FunctionDefinition functionDefinition) {
            functions.put(FunctionIdentifier.of(identifier), functionDefinition);
            return this;
        }

        public TestSpec select(Expression... expressions) {
            this.expressions = expressions;
            return this;
        }

        public TestSpec equalTo(ResolvedExpression... resolvedExpressions) {
            this.expectedExpressions = Arrays.asList(resolvedExpressions);
            return this;
        }

        public ExpressionResolver getResolver() {
            return ExpressionResolver.resolverFor(
                            TableConfig.getDefault(),
                            Thread.currentThread().getContextClassLoader(),
                            name -> Optional.empty(),
                            new FunctionLookupMock(functions),
                            new DataTypeFactoryMock(),
                            (sqlExpression, inputRowType, outputType) -> {
                                throw new UnsupportedOperationException();
                            },
                            Arrays.stream(schemas)
                                    .map(
                                            schema ->
                                                    (QueryOperation)
                                                            new SourceQueryOperation(
                                                                    ContextResolvedTable.anonymous(
                                                                            new ResolvedCatalogTable(
                                                                                    CatalogTable.of(
                                                                                            schema
                                                                                                    .toSchema(),
                                                                                            null,
                                                                                            Collections
                                                                                                    .emptyList(),
                                                                                            Collections
                                                                                                    .emptyMap()),
                                                                                    ResolvedSchema
                                                                                            .physical(
                                                                                                    schema
                                                                                                            .getFieldNames(),
                                                                                                    schema
                                                                                                            .getFieldDataTypes())))))
                                    .toArray(QueryOperation[]::new))
                    .build();
        }

        @Override
        public String toString() {
            return description;
        }
    }
}
