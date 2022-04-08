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

package org.apache.flink.table.planner.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Test interface implementing the logic to execute tests for {@link BuiltInFunctionDefinition}.
 *
 * <p>To create a new set of test cases, just create a subclass and implement the method {@link
 * #getTestSetSpecs()}.
 *
 * <p>Note: This test base is not the most efficient one. It currently checks the full pipeline
 * end-to-end. If the testing time is too long, we can change the underlying implementation easily
 * without touching the defined {@link TestSetSpec}s.
 */
@Execution(ExecutionMode.CONCURRENT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(MiniClusterExtension.class)
abstract class BuiltInFunctionTestBase {

    Configuration getConfiguration() {
        return new Configuration();
    }

    abstract Stream<TestSetSpec> getTestSetSpecs();

    private Stream<TestCase> getTestCases() {
        return this.getTestSetSpecs()
                .flatMap(testSpec -> testSpec.getTestCases(this.getConfiguration()));
    }

    @ParameterizedTest
    @MethodSource("getTestCases")
    final void test(TestCase testCase) throws Throwable {
        testCase.execute();
    }

    // --------------------------------------------------------------------------------------------
    // Test model
    // --------------------------------------------------------------------------------------------

    /** Single test case. */
    static class TestCase implements Executable {

        private final String name;
        private final Executable executable;

        TestCase(String name, Executable executable) {
            this.name = name;
            this.executable = executable;
        }

        @Override
        public void execute() throws Throwable {
            this.executable.execute();
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Test specification for executing a {@link BuiltInFunctionDefinition} with different
     * parameters on a set of fields.
     */
    static class TestSetSpec {

        private final @Nullable BuiltInFunctionDefinition definition;

        private final @Nullable String description;

        private final List<Class<? extends UserDefinedFunction>> functions;

        private final List<TestItem> testItems;

        private Object[] fieldData;

        private @Nullable AbstractDataType<?>[] fieldDataTypes;

        private TestSetSpec(BuiltInFunctionDefinition definition, @Nullable String description) {
            this.definition = definition;
            this.description = description;
            this.functions = new ArrayList<>();
            this.testItems = new ArrayList<>();
        }

        static TestSetSpec forFunction(BuiltInFunctionDefinition definition) {
            return forFunction(definition, null);
        }

        static TestSetSpec forFunction(BuiltInFunctionDefinition definition, String description) {
            return new TestSetSpec(Preconditions.checkNotNull(definition), description);
        }

        static TestSetSpec forExpression(String description) {
            return new TestSetSpec(null, Preconditions.checkNotNull(description));
        }

        TestSetSpec onFieldsWithData(Object... fieldData) {
            this.fieldData = fieldData;
            return this;
        }

        TestSetSpec andDataTypes(AbstractDataType<?>... fieldDataType) {
            this.fieldDataTypes = fieldDataType;
            return this;
        }

        TestSetSpec withFunction(Class<? extends UserDefinedFunction> functionClass) {
            // the function will be registered under the class simple name
            this.functions.add(functionClass);
            return this;
        }

        TestSetSpec testTableApiResult(
                Expression expression, Object result, AbstractDataType<?> dataType) {
            return testTableApiResult(
                    singletonList(expression), singletonList(result), singletonList(dataType));
        }

        TestSetSpec testTableApiResult(
                List<Expression> expression,
                List<Object> result,
                List<AbstractDataType<?>> dataType) {
            testItems.add(new TableApiResultTestItem(expression, result, dataType));
            return this;
        }

        TestSetSpec testTableApiValidationError(Expression expression, String errorMessage) {
            testItems.add(
                    new TableApiErrorTestItem(
                            expression, ValidationException.class, errorMessage, true));
            return this;
        }

        TestSetSpec testTableApiRuntimeError(Expression expression, String errorMessage) {
            testItems.add(
                    new TableApiErrorTestItem(expression, Throwable.class, errorMessage, false));
            return this;
        }

        TestSetSpec testTableApiRuntimeError(
                Expression expression, Class<? extends Throwable> exceptionError) {
            testItems.add(new TableApiErrorTestItem(expression, exceptionError, null, false));
            return this;
        }

        TestSetSpec testSqlResult(String expression, Object result, AbstractDataType<?> dataType) {
            return testSqlResult(expression, singletonList(result), singletonList(dataType));
        }

        TestSetSpec testSqlResult(
                String expression, List<Object> result, List<AbstractDataType<?>> dataType) {
            testItems.add(new SqlResultTestItem(expression, result, dataType));
            return this;
        }

        TestSetSpec testSqlValidationError(String expression, String errorMessage) {
            testItems.add(
                    new SqlErrorTestItem(
                            expression, ValidationException.class, errorMessage, true));
            return this;
        }

        TestSetSpec testSqlRuntimeError(String expression, String errorMessage) {
            testItems.add(new SqlErrorTestItem(expression, Throwable.class, errorMessage, false));
            return this;
        }

        TestSetSpec testSqlRuntimeError(
                String expression, Class<? extends Throwable> exceptionError) {
            testItems.add(new SqlErrorTestItem(expression, exceptionError, null, false));
            return this;
        }

        TestSetSpec testResult(
                Expression expression,
                String sqlExpression,
                Object result,
                AbstractDataType<?> dataType) {
            return testResult(expression, sqlExpression, result, dataType, dataType);
        }

        TestSetSpec testResult(ResultSpec... resultSpecs) {
            final int cols = resultSpecs.length;
            final List<Expression> expressions = new ArrayList<>(cols);
            final List<String> sqlExpressions = new ArrayList<>(cols);
            final List<Object> results = new ArrayList<>(cols);
            final List<AbstractDataType<?>> tableApiDataTypes = new ArrayList<>(cols);
            final List<AbstractDataType<?>> sqlDataTypes = new ArrayList<>(cols);

            for (ResultSpec resultSpec : resultSpecs) {
                expressions.add(resultSpec.tableApiExpression);
                sqlExpressions.add(resultSpec.sqlExpression);
                results.add(resultSpec.result);
                tableApiDataTypes.add(resultSpec.tableApiDataType);
                sqlDataTypes.add(resultSpec.sqlDataType);
            }
            return testResult(
                    expressions, sqlExpressions, results, tableApiDataTypes, sqlDataTypes);
        }

        TestSetSpec testResult(
                Expression expression,
                String sqlExpression,
                Object result,
                AbstractDataType<?> tableApiDataType,
                AbstractDataType<?> sqlDataType) {
            return testResult(
                    singletonList(expression),
                    singletonList(sqlExpression),
                    singletonList(result),
                    singletonList(tableApiDataType),
                    singletonList(sqlDataType));
        }

        TestSetSpec testResult(
                List<Expression> expression,
                List<String> sqlExpression,
                List<Object> result,
                List<AbstractDataType<?>> tableApiDataType,
                List<AbstractDataType<?>> sqlDataType) {
            testItems.add(new TableApiResultTestItem(expression, result, tableApiDataType));
            testItems.add(
                    new SqlResultTestItem(String.join(",", sqlExpression), result, sqlDataType));
            return this;
        }

        Stream<TestCase> getTestCases(Configuration configuration) {
            return testItems.stream().map(testItem -> getTestCase(configuration, testItem));
        }

        private TestCase getTestCase(Configuration configuration, TestItem testItem) {
            return new TestCase(
                    testItem.toString(),
                    () -> {
                        final TableEnvironmentInternal env =
                                (TableEnvironmentInternal)
                                        TableEnvironment.create(
                                                EnvironmentSettings.newInstance().build());
                        env.getConfig().addConfiguration(configuration);

                        functions.forEach(
                                f -> env.createTemporarySystemFunction(f.getSimpleName(), f));

                        final Table inputTable;
                        if (fieldDataTypes == null) {
                            inputTable = env.fromValues(Row.of(fieldData));
                        } else {
                            final DataTypes.UnresolvedField[] fields =
                                    IntStream.range(0, fieldDataTypes.length)
                                            .mapToObj(
                                                    i ->
                                                            DataTypes.FIELD(
                                                                    "f" + i, fieldDataTypes[i]))
                                            .toArray(DataTypes.UnresolvedField[]::new);
                            inputTable = env.fromValues(DataTypes.ROW(fields), Row.of(fieldData));
                        }

                        testItem.test(env, inputTable);
                    });
        }

        @Override
        public String toString() {
            return (definition != null ? definition.getName() : "Expression")
                    + (description != null ? " : " + description : "");
        }
    }

    private interface TestItem {
        void test(TableEnvironmentInternal env, Table inputTable) throws Exception;
    }

    private abstract static class ResultTestItem<T> implements TestItem {
        final T expression;
        final List<Object> results;
        final List<AbstractDataType<?>> dataTypes;

        ResultTestItem(T expression, List<Object> results, List<AbstractDataType<?>> dataTypes) {
            this.expression = expression;
            this.results = results;
            this.dataTypes = dataTypes;
        }

        abstract Table query(TableEnvironment env, Table inputTable);

        @Override
        public void test(TableEnvironmentInternal env, Table inputTable) throws Exception {
            final Table resultTable = this.query(env, inputTable);

            final List<DataType> expectedDataTypes =
                    createDataTypes(env.getCatalogManager().getDataTypeFactory(), this.dataTypes);
            final TableResult result = resultTable.execute();
            try (final CloseableIterator<Row> iterator = result.collect()) {
                assertThat(iterator).hasNext();

                final Row row = iterator.next();

                assertThat(iterator).as("No more rows expected.").isExhausted();

                for (int i = 0; i < row.getArity(); i++) {
                    assertThat(
                                    result.getResolvedSchema()
                                            .getColumnDataTypes()
                                            .get(i)
                                            .getLogicalType())
                            .as("Logical type for spec [%d] of test [%s] doesn't match.", i, this)
                            .isEqualTo(expectedDataTypes.get(i).getLogicalType());

                    assertThat(Row.of(row.getField(i)))
                            .as("Result for spec [%d] of test [%s] doesn't match.", i, this)
                            .isEqualTo(
                                    // Use Row.equals() to enable equality for complex structure,
                                    // i.e. byte[]
                                    Row.of(this.results.get(i)));
                }
            }
        }
    }

    private abstract static class ErrorTestItem<T> implements TestItem {
        final T expression;
        final Class<? extends Throwable> errorClass;
        final String errorMessage;
        final boolean expectedDuringValidation;

        ErrorTestItem(
                T expression,
                Class<? extends Throwable> errorClass,
                String errorMessage,
                boolean expectedDuringValidation) {
            Preconditions.checkState(errorClass != null || errorMessage != null);
            this.expression = expression;
            this.errorClass = errorClass;
            this.errorMessage = errorMessage;
            this.expectedDuringValidation = expectedDuringValidation;
        }

        abstract Table query(TableEnvironment env, Table inputTable);

        Consumer<? super Throwable> errorMatcher() {
            if (errorClass != null && errorMessage != null) {
                return anyCauseMatches(errorClass, errorMessage);
            }
            if (errorMessage != null) {
                return anyCauseMatches(errorMessage);
            }
            return anyCauseMatches(errorClass);
        }

        @Override
        public void test(TableEnvironmentInternal env, Table inputTable) {
            AtomicReference<TableResult> tableResult = new AtomicReference<>();

            Throwable t =
                    catchThrowable(() -> tableResult.set(this.query(env, inputTable).execute()));

            if (this.expectedDuringValidation) {
                assertThat(t)
                        .as("Expected a validation exception")
                        .isNotNull()
                        .satisfies(this.errorMatcher());
                return;
            } else {
                assertThat(t).as("Error while validating the query").isNull();
            }

            assertThatThrownBy(() -> tableResult.get().await())
                    .isNotNull()
                    .satisfies(this.errorMatcher());
        }
    }

    private static class TableApiResultTestItem extends ResultTestItem<List<Expression>> {

        TableApiResultTestItem(
                List<Expression> expressions,
                List<Object> results,
                List<AbstractDataType<?>> dataTypes) {
            super(expressions, results, dataTypes);
        }

        @Override
        Table query(TableEnvironment env, Table inputTable) {
            return inputTable.select(expression.toArray(new Expression[] {}));
        }

        @Override
        public String toString() {
            return "[API] "
                    + expression.stream()
                            .map(Expression::asSummaryString)
                            .collect(Collectors.joining(", "));
        }
    }

    private static class TableApiErrorTestItem extends ErrorTestItem<Expression> {

        TableApiErrorTestItem(
                Expression expression,
                Class<? extends Throwable> errorClass,
                String errorMessage,
                boolean expectedDuringValidation) {
            super(expression, errorClass, errorMessage, expectedDuringValidation);
        }

        @Override
        Table query(TableEnvironment env, Table inputTable) {
            return inputTable.select(expression);
        }

        @Override
        public String toString() {
            return "[API] " + expression.asSummaryString();
        }
    }

    private static class SqlResultTestItem extends ResultTestItem<String> {

        SqlResultTestItem(
                String sqlExpression, List<Object> result, List<AbstractDataType<?>> dataType) {
            super(sqlExpression, result, dataType);
        }

        @Override
        Table query(TableEnvironment env, Table inputTable) {
            return env.sqlQuery("SELECT " + expression + " FROM " + inputTable);
        }

        @Override
        public String toString() {
            return "[SQL] " + expression;
        }
    }

    private static class SqlErrorTestItem extends ErrorTestItem<String> {

        private SqlErrorTestItem(
                String expression,
                Class<? extends Throwable> errorClass,
                String errorMessage,
                boolean expectedDuringValidation) {
            super(expression, errorClass, errorMessage, expectedDuringValidation);
        }

        @Override
        Table query(TableEnvironment env, Table inputTable) {
            return env.sqlQuery("SELECT " + expression + " FROM " + inputTable);
        }

        @Override
        public String toString() {
            return "[SQL] " + expression;
        }
    }

    static List<DataType> createDataTypes(
            DataTypeFactory dataTypeFactory, List<AbstractDataType<?>> dataTypes) {
        return dataTypes.stream().map(dataTypeFactory::createDataType).collect(Collectors.toList());
    }

    /** Helper POJO to store test parameters. */
    static class ResultSpec {

        final Expression tableApiExpression;
        final String sqlExpression;
        final Object result;
        final AbstractDataType<?> tableApiDataType;
        final AbstractDataType<?> sqlDataType;

        private ResultSpec(
                Expression tableApiExpression,
                String sqlExpression,
                Object result,
                AbstractDataType<?> tableApiDataType,
                AbstractDataType<?> sqlQueryDataType) {
            this.tableApiExpression = tableApiExpression;
            this.sqlExpression = sqlExpression;
            this.result = result;
            this.tableApiDataType = tableApiDataType;
            this.sqlDataType = sqlQueryDataType;
        }
    }

    public static ResultSpec resultSpec(
            Expression tableApiExpression,
            String sqlExpression,
            Object result,
            AbstractDataType<?> dataType) {
        return resultSpec(tableApiExpression, sqlExpression, result, dataType, dataType);
    }

    public static ResultSpec resultSpec(
            Expression tableApiExpression,
            String sqlExpression,
            Object result,
            AbstractDataType<?> tableApiDataType,
            AbstractDataType<?> sqlQueryDataType) {
        return new ResultSpec(
                tableApiExpression, sqlExpression, result, tableApiDataType, sqlQueryDataType);
    }
}
