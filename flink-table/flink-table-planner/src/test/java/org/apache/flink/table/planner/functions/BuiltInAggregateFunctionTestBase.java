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
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.planner.factories.TableFactoryHarness;
import org.apache.flink.table.types.DataType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.DataType.getFieldCount;
import static org.apache.flink.table.types.DataType.getFieldDataTypes;
import static org.junit.Assert.assertEquals;

/** Test base for testing aggregate {@link BuiltInFunctionDefinition built-in functions}. */
@RunWith(Parameterized.class)
public class BuiltInAggregateFunctionTestBase {

    @ClassRule
    public static MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @Parameter public TestSpec testSpec;

    @Test
    public void testFunction() throws Exception {
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        final Table sourceTable = asTable(tEnv, testSpec.sourceRowType, testSpec.sourceRows);

        for (final TestItem testItem : testSpec.testItems) {
            testItem.execute(tEnv, sourceTable);
        }
    }

    protected static Table asTable(TableEnvironment tEnv, DataType sourceRowType, List<Row> rows) {
        final TableDescriptor descriptor =
                TableFactoryHarness.newBuilder()
                        .schema(Schema.newBuilder().fromRowDataType(sourceRowType).build())
                        .source(asSource(rows, sourceRowType))
                        .build();

        return tEnv.from(descriptor);
    }

    protected static TableFactoryHarness.ScanSourceBase asSource(
            List<Row> rows, DataType producedDataType) {
        return new TableFactoryHarness.ScanSourceBase() {
            @Override
            public ChangelogMode getChangelogMode() {
                final Set<RowKind> rowKinds =
                        rows.stream().map(Row::getKind).collect(Collectors.toSet());
                if (rowKinds.size() == 1 && rowKinds.contains(RowKind.INSERT)) {
                    return ChangelogMode.insertOnly();
                }

                return ChangelogMode.all();
            }

            @Override
            public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
                final DataStructureConverter converter =
                        context.createDataStructureConverter(producedDataType);

                return SourceFunctionProvider.of(new Source(rows, converter), true);
            }
        };
    }

    protected static void assertRows(List<Row> expectedRows, TableResult tableResult) {
        final List<Row> actualRows =
                materializeResult(tableResult).stream()
                        .sorted(Comparator.comparing(Objects::toString))
                        .collect(Collectors.toList());
        final List<Row> sortedExpectedRows =
                expectedRows.stream()
                        .sorted(Comparator.comparing(Objects::toString))
                        .collect(Collectors.toList());

        assertEquals(
                String.format("%n%nExpected:%n%s%n%nActual:%n%s", sortedExpectedRows, actualRows),
                sortedExpectedRows,
                actualRows);
    }

    private static List<Row> materializeResult(TableResult tableResult) {
        try (final CloseableIterator<Row> iterator = tableResult.collect()) {
            final List<Row> actualRows = new ArrayList<>();
            iterator.forEachRemaining(
                    row -> {
                        final RowKind kind = row.getKind();
                        switch (kind) {
                            case INSERT:
                            case UPDATE_AFTER:
                                row.setKind(RowKind.INSERT);
                                actualRows.add(row);
                                break;
                            case UPDATE_BEFORE:
                            case DELETE:
                                row.setKind(RowKind.INSERT);
                                actualRows.remove(row);
                                break;
                        }
                    });

            return actualRows;
        } catch (Exception e) {
            throw new RuntimeException("Could not collect results", e);
        }
    }

    // ---------------------------------------------------------------------------------------------

    /** Test specification. */
    protected static class TestSpec {

        private final BuiltInFunctionDefinition definition;
        private final List<TestItem> testItems = new ArrayList<>();

        private @Nullable String description;

        private DataType sourceRowType;
        private List<Row> sourceRows;

        private TestSpec(BuiltInFunctionDefinition definition) {
            this.definition = Preconditions.checkNotNull(definition);
        }

        static TestSpec forFunction(BuiltInFunctionDefinition definition) {
            return new TestSpec(definition);
        }

        TestSpec withDescription(String description) {
            this.description = description;
            return this;
        }

        TestSpec withSource(DataType sourceRowType, List<Row> sourceRows) {
            this.sourceRowType = sourceRowType;
            this.sourceRows = sourceRows;
            return this;
        }

        TestSpec testSqlResult(
                Function<Table, String> sqlSpec, DataType expectedRowType, List<Row> expectedRows) {
            this.testItems.add(new SqlTestItem(sqlSpec, expectedRowType, expectedRows));
            return this;
        }

        TestSpec testApiResult(
                Function<Table, Table> tableApiSpec,
                DataType expectedRowType,
                List<Row> expectedRows) {
            this.testItems.add(new TableApiTestItem(tableApiSpec, expectedRowType, expectedRows));
            return this;
        }

        TestSpec testResult(
                Function<Table, String> sqlSpec,
                Function<Table, Table> tableApiSpec,
                DataType expectedRowType,
                List<Row> expectedRows) {
            return testResult(
                    sqlSpec, tableApiSpec, expectedRowType, expectedRowType, expectedRows);
        }

        TestSpec testResult(
                Function<Table, String> sqlSpec,
                Function<Table, Table> tableApiSpec,
                DataType expectedSqlRowType,
                DataType expectedTableApiRowType,
                List<Row> expectedRows) {
            testSqlResult(sqlSpec, expectedSqlRowType, expectedRows);
            testApiResult(tableApiSpec, expectedTableApiRowType, expectedRows);
            return this;
        }

        @Override
        public String toString() {
            final StringBuilder bob = new StringBuilder();
            bob.append(definition.getName());
            if (description != null) {
                bob.append(" (");
                bob.append(description);
                bob.append(")");
            }

            return bob.toString();
        }
    }

    private interface TestItem {
        void execute(TableEnvironment tEnv, Table sourceTable);
    }

    private abstract static class SuccessItem implements TestItem {
        private final @Nullable DataType expectedRowType;
        private final @Nullable List<Row> expectedRows;

        public SuccessItem(@Nullable DataType expectedRowType, @Nullable List<Row> expectedRows) {
            this.expectedRowType = expectedRowType;
            this.expectedRows = expectedRows;
        }

        @Override
        public void execute(TableEnvironment tEnv, Table sourceTable) {
            final TableResult tableResult = getResult(tEnv, sourceTable);

            if (expectedRowType != null) {
                final DataType actualRowType =
                        tableResult.getResolvedSchema().toSourceRowDataType();

                assertEquals(getFieldCount(expectedRowType), getFieldCount(actualRowType));
                assertEquals(getFieldDataTypes(expectedRowType), getFieldDataTypes(actualRowType));
            }

            if (expectedRows != null) {
                assertRows(expectedRows, tableResult);
            }
        }

        protected abstract TableResult getResult(TableEnvironment tEnv, Table sourceTable);
    }

    private static class SqlTestItem extends SuccessItem {
        private final Function<Table, String> spec;

        public SqlTestItem(
                Function<Table, String> spec,
                @Nullable DataType expectedRowType,
                @Nullable List<Row> expectedRows) {
            super(expectedRowType, expectedRows);
            this.spec = spec;
        }

        @Override
        protected TableResult getResult(TableEnvironment tEnv, Table sourceTable) {
            return tEnv.sqlQuery(spec.apply(sourceTable)).execute();
        }
    }

    private static class TableApiTestItem extends SuccessItem {
        private final Function<Table, Table> spec;

        public TableApiTestItem(
                Function<Table, Table> spec,
                @Nullable DataType expectedRowType,
                @Nullable List<Row> expectedRows) {
            super(expectedRowType, expectedRows);
            this.spec = spec;
        }

        @Override
        protected TableResult getResult(TableEnvironment tEnv, Table sourceTable) {
            return spec.apply(sourceTable).execute();
        }
    }

    // ---------------------------------------------------------------------------------------------

    private static class Source implements SourceFunction<RowData> {

        private final List<Row> rows;
        private final DynamicTableSource.DataStructureConverter converter;

        public Source(List<Row> rows, DynamicTableSource.DataStructureConverter converter) {
            this.rows = rows;
            this.converter = converter;
        }

        @Override
        public void run(SourceContext<RowData> ctx) throws Exception {
            rows.stream().map(row -> (RowData) converter.toInternal(row)).forEach(ctx::collect);
        }

        @Override
        public void cancel() {}
    }
}
