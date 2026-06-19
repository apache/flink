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

import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.expressions.DefaultSqlFactory;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.operations.ProjectQueryOperation;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedThrowable;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.row;
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
abstract class BuiltInFunctionTestBase {

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_EXTENSION = new MiniClusterExtension();

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
    final void test(TestCase testCase, @InjectMiniCluster MiniCluster miniCluster)
            throws Throwable {
        testCase.execute(new MiniClusterClient(miniCluster.getConfiguration(), miniCluster));
    }

    // --------------------------------------------------------------------------------------------
    // Test model
    // --------------------------------------------------------------------------------------------

    interface TestCaseWithClusterClient {
        void execute(MiniClusterClient clusterClient) throws Throwable;
    }

    /** Single test case. */
    static class TestCase implements TestCaseWithClusterClient {

        private final String name;
        private final TestCaseWithClusterClient executable;

        TestCase(String name, TestCaseWithClusterClient executable) {
            this.name = name;
            this.executable = executable;
        }

        public void execute(MiniClusterClient clusterClient) throws Throwable {
            this.executable.execute(clusterClient);
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

        private @Nullable Object[] fieldData;

        private @Nullable AbstractDataType<?>[] fieldDataTypes;

        private boolean supportsConstantFolding = false;

        private TestSetSpec(
                @Nullable BuiltInFunctionDefinition definition, @Nullable String description) {
            this.definition = definition;
            this.description = description;
            this.functions = new ArrayList<>();
            this.testItems = new ArrayList<>();
        }

        /**
         * Creates a new test specification for a built-in function.
         *
         * <p>The function definition is used for test organization and readability only. It does
         * not affect test execution behavior, which is determined by the actual test methods like
         * {@link #testSqlResult} or {@link #testTableApiResult}.
         */
        static TestSetSpec forFunction(BuiltInFunctionDefinition definition) {
            return forFunction(definition, null);
        }

        /**
         * Creates a new test specification for a built-in function with description.
         *
         * <p>Both the function definition and description are used for test organization and
         * readability only. They help identify what is being tested but do not affect the actual
         * test execution behavior.
         */
        static TestSetSpec forFunction(BuiltInFunctionDefinition definition, String description) {
            return new TestSetSpec(Preconditions.checkNotNull(definition), description);
        }

        /**
         * Creates a new test specification for arbitrary expressions.
         *
         * <p>The description is used for test organization and readability only. It helps identify
         * what is being tested but does not affect the actual test execution behavior.
         */
        static TestSetSpec forExpression(String description) {
            return new TestSetSpec(null, Preconditions.checkNotNull(description));
        }

        /**
         * Sets the field data for creating an input table.
         *
         * <p>If called with arguments, creates an input table with the provided data as a single
         * row. SQL queries will include {@code FROM <inputTable>}. If called with no arguments or
         * not called, no input table is created and SQL queries run as standalone expressions.
         */
        TestSetSpec onFieldsWithData(Object... fieldData) {
            this.fieldData = fieldData;
            return this;
        }

        /**
         * Sets the data types for the input table fields.
         *
         * <p>Must be used together with {@link #onFieldsWithData(Object...)}. The number of data
         * types should match the number of field data values. When used, constant folding is
         * disabled to force runtime code generation paths.
         */
        TestSetSpec andDataTypes(AbstractDataType<?>... fieldDataType) {
            this.fieldDataTypes = fieldDataType;
            return this;
        }

        /**
         * Registers a user-defined function under the class simple name for use in test
         * expressions.
         */
        TestSetSpec withFunction(Class<? extends UserDefinedFunction> functionClass) {
            this.functions.add(functionClass);
            return this;
        }

        /**
         * Enables constant folding for this test set.
         *
         * <p>When enabled, expressions can be optimized by the optimizer at compile time, allowing
         * constants to be folded. This is useful for testing the optimizer's behavior with constant
         * expressions.
         *
         * <p>When disabled (default), field accesses are wrapped with {@link IdentityFunction} to
         * force runtime code generation and prevent constant folding.
         *
         * @see IdentityFunction
         */
        TestSetSpec withConstantFoldingEnabled() {
            this.supportsConstantFolding = true;
            return this;
        }

        TestSetSpec testTableApiResult(
                Expression expression, Object result, AbstractDataType<?> dataType) {
            return testTableApiResult(
                    singletonList(expression), singletonList(result), singletonList(dataType));
        }

        TestSetSpec testTableApiResult(Expression expression, AbstractDataType<?> dataType) {
            return testTableApiResult(
                    singletonList(expression), emptyList(), singletonList(dataType));
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

        TestSetSpec testTableApiRuntimeError(
                Expression expression,
                Class<? extends Throwable> exceptionError,
                String errorMessage) {
            testItems.add(
                    new TableApiErrorTestItem(expression, exceptionError, errorMessage, false));
            return this;
        }

        TestSetSpec testSqlResult(String expression, Object result, AbstractDataType<?> dataType) {
            return testSqlResult(expression, singletonList(result), singletonList(dataType));
        }

        TestSetSpec testSqlResult(String expression, AbstractDataType<?> dataType) {
            return testSqlResult(expression, emptyList(), singletonList(dataType));
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

        TestSetSpec testSqlRuntimeError(
                String expression, Class<? extends Throwable> exceptionError, String errorMessage) {
            testItems.add(new SqlErrorTestItem(expression, exceptionError, errorMessage, false));
            return this;
        }

        /** Tests both Table API and SQL expressions expecting successful results. */
        TestSetSpec testResult(
                Expression expression,
                String sqlExpression,
                Object result,
                AbstractDataType<?> dataType) {
            return testResult(expression, sqlExpression, result, dataType, dataType);
        }

        /** Tests both Table API and SQL expressions expecting successful results. */
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

        /** Tests both Table API and SQL expressions expecting successful results. */
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

        /** Tests both Table API and SQL expressions expecting successful results. */
        TestSetSpec testResult(
                List<Expression> expression,
                List<String> sqlExpression,
                List<Object> result,
                List<AbstractDataType<?>> tableApiDataType,
                List<AbstractDataType<?>> sqlDataType) {
            testItems.add(new TableApiResultTestItem(expression, result, tableApiDataType));
            testItems.add(new TableApiSqlResultTestItem(expression, result, tableApiDataType));
            testItems.add(
                    new SqlResultTestItem(String.join(",", sqlExpression), result, sqlDataType));
            return this;
        }

        /** Generates test cases from this specification. */
        Stream<TestCase> getTestCases(Configuration configuration) {
            return testItems.stream().map(testItem -> getTestCase(configuration, testItem));
        }

        private TestCase getTestCase(Configuration configuration, TestItem testItem) {
            return new TestCase(
                    testItem.toString(),
                    (clusterClient) -> {
                        final TableEnvironmentInternal env =
                                (TableEnvironmentInternal)
                                        TableEnvironment.create(
                                                EnvironmentSettings.newInstance().build());
                        env.getConfig().addConfiguration(configuration);

                        functions.forEach(
                                f -> env.createTemporarySystemFunction(f.getSimpleName(), f));

                        Preconditions.checkArgument(
                                !(fieldData == null && fieldDataTypes != null),
                                "The field data type is set but the field data is not.");
                        final Table inputTable;
                        if (fieldData == null) {
                            inputTable = null;
                        } else if (fieldDataTypes == null) {
                            inputTable = env.fromValues(Row.of(fieldData));
                        } else {
                            final DataTypes.UnresolvedField[] fields =
                                    IntStream.range(0, fieldDataTypes.length)
                                            .mapToObj(
                                                    i ->
                                                            DataTypes.FIELD(
                                                                    "f" + i, fieldDataTypes[i]))
                                            .toArray(DataTypes.UnresolvedField[]::new);

                            final Expression[] expressions =
                                    IntStream.range(0, fieldDataTypes.length)
                                            .mapToObj(i -> $(fields[i].getName()))
                                            .toArray(Expression[]::new);

                            final Expression[] aliasedExpressions =
                                    IntStream.range(0, expressions.length)
                                            .mapToObj(
                                                    i ->
                                                            call(
                                                                            IdentityFunction.class,
                                                                            expressions[i])
                                                                    .as(fields[i].getName()))
                                            .toArray(Expression[]::new);

                            final Table table =
                                    env.fromValues(DataTypes.ROW(fields), Row.of(fieldData));
                            inputTable =
                                    this.supportsConstantFolding
                                            ? table
                                            : table.select(aliasedExpressions);
                        }

                        testItem.test(env, inputTable, clusterClient);
                    });
        }

        @Override
        public String toString() {
            return (definition != null ? definition.getName() : "Expression")
                    + (description != null ? " : " + description : "");
        }
    }

    private interface TestItem {
        /**
         * @param env The table environment for test to execute.
         * @param inputTable The input table of this test that contains input data and data type. If
         *     it is null, the test is not dependent on the input data.
         */
        void test(
                TableEnvironmentInternal env,
                @Nullable Table inputTable,
                MiniClusterClient clusterClient)
                throws Exception;
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

        abstract Table query(TableEnvironment env, @Nullable Table inputTable);

        @Override
        public void test(
                TableEnvironmentInternal env,
                @Nullable Table inputTable,
                MiniClusterClient clusterClient)
                throws Exception {
            final Table resultTable = this.query(env, inputTable);

            final List<DataType> expectedDataTypes =
                    createDataTypes(env.getCatalogManager().getDataTypeFactory(), this.dataTypes);
            final TableResult result = resultTable.execute();
            try (final CloseableIterator<Row> iterator = result.collect()) {
                assertThat(iterator).hasNext();

                final Row row = iterator.next();

                assertThat(iterator).as("No more rows expected.").isExhausted();

                for (int i = 0; i < row.getArity(); i++) {
                    if (!expectedDataTypes.isEmpty()) {
                        assertThat(
                                        result.getResolvedSchema()
                                                .getColumnDataTypes()
                                                .get(i)
                                                .getLogicalType())
                                .as(
                                        "Logical type for spec [%d] of test [%s] doesn't match.",
                                        i, this)
                                .isEqualTo(expectedDataTypes.get(i).getLogicalType());
                    }

                    if (!this.results.isEmpty()) {
                        assertThat(Row.of(row.getField(i)))
                                .as("Result for spec [%d] of test [%s] doesn't match.", i, this)
                                .isEqualTo(
                                        // Use Row.equals() to enable equality for complex
                                        // structure,
                                        // i.e. byte[]
                                        Row.of(this.results.get(i)));
                    }
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

        abstract Table query(TableEnvironment env, @Nullable Table inputTable);

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
        public void test(
                TableEnvironmentInternal env,
                @Nullable Table inputTable,
                MiniClusterClient clusterClient) {
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

            assertThatThrownBy(
                            () -> {
                                final TableResult result = tableResult.get();
                                result.await();
                                final Optional<SerializedThrowable> serializedThrowable =
                                        clusterClient
                                                .requestJobResult(
                                                        result.getJobClient().get().getJobID())
                                                .get()
                                                .getSerializedThrowable();
                                if (serializedThrowable.isPresent()) {
                                    throw serializedThrowable
                                            .get()
                                            .deserializeError(getClass().getClassLoader());
                                }
                            })
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
        Table query(TableEnvironment env, @Nullable Table inputTable) {
            if (inputTable != null) {
                return inputTable.select(expression.toArray(new Expression[] {}));
            } else {
                // use a mock collection table with row "0" to avoid pruning the project
                // node with expression by PruneEmptyRules.PROJECT_INSTANCE
                return env.fromValues(row(0)).select(expression.toArray(new Expression[] {}));
            }
        }

        @Override
        public String toString() {
            return "[API] "
                    + expression.stream()
                            .map(Expression::asSummaryString)
                            .collect(Collectors.joining(", "));
        }
    }

    private static class TableApiSqlResultTestItem extends ResultTestItem<List<Expression>> {

        TableApiSqlResultTestItem(
                List<Expression> expressions,
                List<Object> results,
                List<AbstractDataType<?>> dataTypes) {
            super(expressions, results, dataTypes);
        }

        @Override
        Table query(TableEnvironment env, Table inputTable) {
            final Table select = inputTable.select(expression.toArray(new Expression[] {}));
            final ProjectQueryOperation projectQueryOperation =
                    (ProjectQueryOperation) select.getQueryOperation();
            final String exprAsSerializableString =
                    projectQueryOperation.getProjectList().stream()
                            .map(
                                    resolvedExpression ->
                                            resolvedExpression.asSerializableString(
                                                    DefaultSqlFactory.INSTANCE))
                            .collect(Collectors.joining(", "));
            return env.sqlQuery("SELECT " + exprAsSerializableString + " FROM " + inputTable);
        }

        @Override
        public String toString() {
            return "[API as SQL] "
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
        Table query(TableEnvironment env, @Nullable Table inputTable) {
            if (inputTable != null) {
                return env.sqlQuery("SELECT " + expression + " FROM " + inputTable);
            } else {
                return env.sqlQuery("SELECT " + expression);
            }
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

    /** Identity function that forces the planner to skip constant folding. */
    public static class IdentityFunction extends ScalarFunction {
        public Object eval(Object input) {
            return input;
        }

        @Override
        public TypeInference getTypeInference(final DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .outputTypeStrategy(c -> Optional.of(c.getArgumentDataTypes().get(0)))
                    .build();
        }

        @Override
        public boolean supportsConstantFolding() {
            return false;
        }
    }
}
