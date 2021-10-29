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
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
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
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.apache.flink.core.testutils.FlinkMatchers.containsMessage;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test base for testing {@link BuiltInFunctionDefinition}.
 *
 * <p>Note: This test base is not the most efficient one. It currently checks the full pipeline
 * end-to-end. If the testing time is too long, we can change the underlying implementation easily
 * without touching the defined {@link TestSpec}s.
 */
@RunWith(Parameterized.class)
public abstract class BuiltInFunctionTestBase {

    @ClassRule
    public static MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @Parameter public TestSpec testSpec;

    protected Configuration configuration() {
        return new Configuration();
    }

    @Test
    public void testFunction() {
        final TableEnvironment env =
                TableEnvironment.create(EnvironmentSettings.newInstance().build());
        env.getConfig().addConfiguration(configuration());

        testSpec.functions.forEach(f -> env.createTemporarySystemFunction(f.getSimpleName(), f));

        final DataTypeFactory dataTypeFactory =
                ((TableEnvironmentInternal) env).getCatalogManager().getDataTypeFactory();

        final Table inputTable;
        if (testSpec.fieldDataTypes == null) {
            inputTable = env.fromValues(Row.of(testSpec.fieldData));
        } else {
            final DataTypes.UnresolvedField[] fields =
                    IntStream.range(0, testSpec.fieldDataTypes.length)
                            .mapToObj(i -> DataTypes.FIELD("f" + i, testSpec.fieldDataTypes[i]))
                            .toArray(DataTypes.UnresolvedField[]::new);
            inputTable = env.fromValues(DataTypes.ROW(fields), Row.of(testSpec.fieldData));
        }

        for (TestItem testItem : testSpec.testItems) {
            try {
                if (testItem instanceof ResultTestItem<?>) {
                    testResult(dataTypeFactory, env, inputTable, (ResultTestItem<?>) testItem);
                } else if (testItem instanceof ErrorTestItem<?>) {
                    testError(env, inputTable, (ErrorTestItem<?>) testItem);
                }
            } catch (Throwable t) {
                throw new AssertionError("Failing test item: " + testItem, t);
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Test utilities
    // --------------------------------------------------------------------------------------------
    private static void testResult(
            DataTypeFactory dataTypeFactory,
            TableEnvironment env,
            Table inputTable,
            ResultTestItem<?> testItem) {

        final Table resultTable;
        if (testItem instanceof TableApiResultTestItem) {
            resultTable =
                    inputTable.select(
                            ((TableApiResultTestItem) testItem)
                                    .expression()
                                    .toArray(new Expression[] {}));
        } else {
            resultTable = env.sqlQuery("SELECT " + testItem.expression() + " FROM " + inputTable);
        }

        final List<DataType> expectedDataTypes =
                createDataTypes(dataTypeFactory, testItem.dataTypes);
        final TableResult result = resultTable.execute();
        final Iterator<Row> iterator = result.collect();

        assertTrue(iterator.hasNext());

        final Row row = iterator.next();

        assertFalse("No more rows expected.", iterator.hasNext());

        for (int i = 0; i < row.getArity(); i++) {
            assertEquals(
                    "Logical type for spec [" + i + "] of test [" + testItem + "] doesn't match.",
                    expectedDataTypes.get(i).getLogicalType(),
                    result.getResolvedSchema().getColumnDataTypes().get(i).getLogicalType());

            assertEquals(
                    "Result for spec [" + i + "] of test [" + testItem + "] doesn't match.",
                    // Use Row.equals() to enable equality for complex structure, i.e. byte[]
                    Row.of(testItem.results.get(i)),
                    Row.of(row.getField(i)));
        }
    }

    private static void testError(
            TableEnvironment env, Table inputTable, ErrorTestItem<?> testItem) {
        try {
            final TableResult tableResult;
            if (testItem instanceof TableApiErrorTestItem) {
                tableResult =
                        inputTable
                                .select(((TableApiErrorTestItem) testItem).expression())
                                .execute();
            } else {
                tableResult =
                        env.sqlQuery("SELECT " + testItem.expression() + " FROM " + inputTable)
                                .execute();
            }
            if (testItem.expectedDuringValidation) {
                fail("Error expected: " + testItem.errorMessage);
            }

            try {
                tableResult.await();
                fail("Error expected: " + testItem.errorMessage);
            } catch (AssertionError e) {
                throw e;
            } catch (Throwable t) {
                assertThat("Wrong error message", t, containsMessage(testItem.errorMessage));
            }
        } catch (AssertionError e) {
            throw e;
        } catch (Throwable t) {
            assertTrue(t instanceof ValidationException);
            assertThat(t.getMessage(), containsString(testItem.errorMessage));
        }
    }

    /**
     * Test specification for executing a {@link BuiltInFunctionDefinition} with different
     * parameters on a set of fields.
     */
    protected static class TestSpec {

        private final @Nullable BuiltInFunctionDefinition definition;

        private final @Nullable String description;

        private final List<Class<? extends UserDefinedFunction>> functions;

        private final List<TestItem> testItems;

        private Object[] fieldData;

        private @Nullable AbstractDataType<?>[] fieldDataTypes;

        private TestSpec(BuiltInFunctionDefinition definition, @Nullable String description) {
            this.definition = definition;
            this.description = description;
            this.functions = new ArrayList<>();
            this.testItems = new ArrayList<>();
        }

        static TestSpec forFunction(BuiltInFunctionDefinition definition) {
            return forFunction(definition, null);
        }

        static TestSpec forFunction(BuiltInFunctionDefinition definition, String description) {
            return new TestSpec(Preconditions.checkNotNull(definition), description);
        }

        static TestSpec forExpression(String description) {
            return new TestSpec(null, Preconditions.checkNotNull(description));
        }

        TestSpec onFieldsWithData(Object... fieldData) {
            this.fieldData = fieldData;
            return this;
        }

        TestSpec andDataTypes(AbstractDataType<?>... fieldDataType) {
            this.fieldDataTypes = fieldDataType;
            return this;
        }

        TestSpec withFunction(Class<? extends UserDefinedFunction> functionClass) {
            // the function will be registered under the class simple name
            this.functions.add(functionClass);
            return this;
        }

        TestSpec testTableApiResult(
                Expression expression, Object result, AbstractDataType<?> dataType) {
            return testTableApiResult(
                    singletonList(expression), singletonList(result), singletonList(dataType));
        }

        TestSpec testTableApiResult(
                List<Expression> expression,
                List<Object> result,
                List<AbstractDataType<?>> dataType) {
            testItems.add(new TableApiResultTestItem(expression, result, dataType));
            return this;
        }

        TestSpec testTableApiValidationError(Expression expression, String errorMessage) {
            testItems.add(new TableApiErrorTestItem(expression, errorMessage, true));
            return this;
        }

        TestSpec testTableApiRuntimeError(Expression expression, String errorMessage) {
            testItems.add(new TableApiErrorTestItem(expression, errorMessage, false));
            return this;
        }

        TestSpec testSqlResult(String expression, Object result, AbstractDataType<?> dataType) {
            return testSqlResult(expression, singletonList(result), singletonList(dataType));
        }

        TestSpec testSqlResult(
                String expression, List<Object> result, List<AbstractDataType<?>> dataType) {
            testItems.add(new SqlResultTestItem(expression, result, dataType));
            return this;
        }

        TestSpec testSqlValidationError(String expression, String errorMessage) {
            testItems.add(new SqlErrorTestItem(expression, errorMessage, true));
            return this;
        }

        TestSpec testSqlRuntimeError(String expression, String errorMessage) {
            testItems.add(new SqlErrorTestItem(expression, errorMessage, false));
            return this;
        }

        TestSpec testResult(
                Expression expression,
                String sqlExpression,
                Object result,
                AbstractDataType<?> dataType) {
            return testResult(expression, sqlExpression, result, dataType, dataType);
        }

        TestSpec testResult(ResultSpec... resultSpecs) {
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

        TestSpec testResult(
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

        TestSpec testResult(
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

        @Override
        public String toString() {
            return (definition != null ? definition.getName() : "Expression")
                    + (description != null ? " : " + description : "");
        }
    }

    private interface TestItem {
        // marker interface
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

        abstract T expression();
    }

    private abstract static class ErrorTestItem<T> implements TestItem {
        final T expression;
        final String errorMessage;
        final boolean expectedDuringValidation;

        ErrorTestItem(T expression, String errorMessage, boolean expectedDuringValidation) {
            this.expression = expression;
            this.errorMessage = errorMessage;
            this.expectedDuringValidation = expectedDuringValidation;
        }

        abstract T expression();
    }

    private static class TableApiResultTestItem extends ResultTestItem<List<Expression>> {

        TableApiResultTestItem(
                List<Expression> expressions,
                List<Object> results,
                List<AbstractDataType<?>> dataTypes) {
            super(expressions, results, dataTypes);
        }

        @Override
        List<Expression> expression() {
            return expression;
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
                Expression expression, String errorMessage, boolean expectedDuringValidation) {
            super(expression, errorMessage, expectedDuringValidation);
        }

        @Override
        Expression expression() {
            return expression;
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
        String expression() {
            return expression;
        }

        @Override
        public String toString() {
            return "[SQL] " + expression;
        }
    }

    private static class SqlErrorTestItem extends ErrorTestItem<String> {

        private SqlErrorTestItem(
                String expression, String errorMessage, boolean expectedDuringValidation) {
            super(expression, errorMessage, expectedDuringValidation);
        }

        @Override
        String expression() {
            return expression;
        }

        @Override
        public String toString() {
            return "[SQL] " + expression;
        }
    }

    private static List<DataType> createDataTypes(
            DataTypeFactory dataTypeFactory, List<AbstractDataType<?>> dataTypes) {
        return dataTypes.stream().map(dataTypeFactory::createDataType).collect(Collectors.toList());
    }

    /** Helper POJO to store test parameters. */
    public static class ResultSpec {

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
