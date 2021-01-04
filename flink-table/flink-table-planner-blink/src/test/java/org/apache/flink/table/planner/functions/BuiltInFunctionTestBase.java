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

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
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

    @Test
    public void testFunction() {
        final TableEnvironment env =
                TableEnvironment.create(EnvironmentSettings.newInstance().build());

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
                if (testItem instanceof TableApiResultTestItem) {
                    testTableApiResult(
                            dataTypeFactory, inputTable, ((TableApiResultTestItem) testItem));
                } else if (testItem instanceof TableApiErrorTestItem) {
                    testTableApiError(inputTable, ((TableApiErrorTestItem) testItem));
                } else if (testItem instanceof SqlResultTestItem) {
                    testSqlResult(dataTypeFactory, env, inputTable, ((SqlResultTestItem) testItem));
                } else if (testItem instanceof SqlErrorTestItem) {
                    testSqlError(env, inputTable, ((SqlErrorTestItem) testItem));
                }
            } catch (Throwable t) {
                throw new AssertionError("Failing test item: " + testItem.toString(), t);
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Test utilities
    // --------------------------------------------------------------------------------------------

    private static void testTableApiResult(
            DataTypeFactory dataTypeFactory, Table inputTable, TableApiResultTestItem testItem) {
        testResult(dataTypeFactory, inputTable.select(testItem.expression), testItem);
    }

    private static void testTableApiError(Table inputTable, TableApiErrorTestItem testItem) {
        try {
            inputTable.select(testItem.expression).execute();
            fail("Error expected: " + testItem.errorMessage);
        } catch (Throwable t) {
            assertThat(t, containsCause(new ValidationException(testItem.errorMessage)));
        }
    }

    private static void testSqlResult(
            DataTypeFactory dataTypeFactory,
            TableEnvironment env,
            Table inputTable,
            SqlResultTestItem testItem) {
        testResult(
                dataTypeFactory,
                env.sqlQuery("SELECT " + testItem.expression + " FROM " + inputTable),
                testItem);
    }

    private static void testSqlError(
            TableEnvironment env, Table inputTable, SqlErrorTestItem testItem) {
        try {
            env.sqlQuery("SELECT " + testItem.expression + " FROM " + inputTable).execute();
            fail("Error expected: " + testItem.errorMessage);
        } catch (Throwable t) {
            assertTrue(t instanceof ValidationException);
            assertThat(t.getMessage(), containsString(testItem.errorMessage));
        }
    }

    private static void testResult(
            DataTypeFactory dataTypeFactory, Table resultTable, ResultTestItem testItem) {
        final DataType expectedDataType = dataTypeFactory.createDataType(testItem.dataType);
        final TableResult result = resultTable.execute();
        final Iterator<Row> iterator = result.collect();

        assertTrue(iterator.hasNext());

        final Row row = iterator.next();

        assertFalse("No more rows expected.", iterator.hasNext());

        assertEquals("Only 1 column expected.", 1, row.getArity());

        assertEquals(
                "Logical type doesn't match.",
                expectedDataType.getLogicalType(),
                result.getTableSchema().getFieldDataTypes()[0].getLogicalType());

        assertEquals("Result doesn't match.", testItem.result, row.getField(0));
    }

    /**
     * Test specification for executing a {@link BuiltInFunctionDefinition} with different
     * parameters on a set of fields.
     */
    protected static class TestSpec {

        private final BuiltInFunctionDefinition definition;

        private final @Nullable String description;

        private Object[] fieldData;

        private @Nullable AbstractDataType<?>[] fieldDataTypes;

        private List<Class<? extends UserDefinedFunction>> functions;

        private List<TestItem> testItems;

        private TestSpec(BuiltInFunctionDefinition definition, @Nullable String description) {
            this.definition = definition;
            this.description = description;
            this.functions = new ArrayList<>();
            this.testItems = new ArrayList<>();
        }

        static TestSpec forFunction(BuiltInFunctionDefinition definition) {
            return new TestSpec(definition, null);
        }

        static TestSpec forFunction(BuiltInFunctionDefinition definition, String description) {
            return new TestSpec(definition, description);
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
            testItems.add(new TableApiResultTestItem(expression, result, dataType));
            return this;
        }

        TestSpec testTableApiError(Expression expression, String errorMessage) {
            testItems.add(new TableApiErrorTestItem(expression, errorMessage));
            return this;
        }

        TestSpec testSqlResult(String expression, Object result, AbstractDataType<?> dataType) {
            testItems.add(new SqlResultTestItem(expression, result, dataType));
            return this;
        }

        TestSpec testSqlError(String expression, String errorMessage) {
            testItems.add(new SqlErrorTestItem(expression, errorMessage));
            return this;
        }

        TestSpec testResult(
                Expression expression,
                String sqlExpression,
                Object result,
                AbstractDataType<?> dataType) {
            testItems.add(new TableApiResultTestItem(expression, result, dataType));
            testItems.add(new SqlResultTestItem(sqlExpression, result, dataType));
            return this;
        }

        @Override
        public String toString() {
            return definition.getName() + (description != null ? " : " + description : "");
        }
    }

    private interface TestItem {
        // marker interface
    }

    private static class ResultTestItem implements TestItem {
        final Object result;
        final AbstractDataType<?> dataType;

        ResultTestItem(Object result, AbstractDataType<?> dataType) {
            this.result = result;
            this.dataType = dataType;
        }
    }

    private static class ErrorTestItem implements TestItem {
        final String errorMessage;

        ErrorTestItem(String errorMessage) {
            this.errorMessage = errorMessage;
        }
    }

    private static class TableApiResultTestItem extends ResultTestItem {
        final Expression expression;

        TableApiResultTestItem(Expression expression, Object result, AbstractDataType<?> dataType) {
            super(result, dataType);
            this.expression = expression;
        }

        @Override
        public String toString() {
            return "[API] " + expression.asSummaryString();
        }
    }

    private static class TableApiErrorTestItem extends ErrorTestItem {
        final Expression expression;

        TableApiErrorTestItem(Expression expression, String errorMessage) {
            super(errorMessage);
            this.expression = expression;
        }

        @Override
        public String toString() {
            return "[API] " + expression.asSummaryString();
        }
    }

    private static class SqlResultTestItem extends ResultTestItem {
        final String expression;

        SqlResultTestItem(String expression, Object result, AbstractDataType<?> dataType) {
            super(result, dataType);
            this.expression = expression;
        }

        @Override
        public String toString() {
            return "[SQL] " + expression;
        }
    }

    private static class SqlErrorTestItem extends ErrorTestItem {
        final String expression;

        private SqlErrorTestItem(String expression, String errorMessage) {
            super(errorMessage);
            this.expression = expression;
        }

        @Override
        public String toString() {
            return "[SQL] " + expression;
        }
    }
}
