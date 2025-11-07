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

package org.apache.flink.table.test.program;

import org.apache.flink.table.api.Model;
import org.apache.flink.table.api.ModelDescriptor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.DefaultSqlFactory;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.test.program.TableApiTestStep.TableEnvAccessor;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.util.Preconditions;

import java.util.function.Function;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test step for executing Table API query that will fail eventually with either {@link
 * ValidationException} (during planning time) or {@link TableRuntimeException} (during execution
 * time).
 *
 * <p>Similar to {@link FailingSqlTestStep} but uses Table API instead of SQL.
 */
public final class FailingTableApiTestStep implements TestStep {

    private final Function<TableEnvAccessor, Table> tableQuery;
    private final String sinkName;
    public final Class<? extends Exception> expectedException;
    public final String expectedErrorMessage;

    FailingTableApiTestStep(
            Function<TableEnvAccessor, Table> tableQuery,
            String sinkName,
            Class<? extends Exception> expectedException,
            String expectedErrorMessage) {
        Preconditions.checkArgument(
                expectedException == ValidationException.class
                        || expectedException == TableRuntimeException.class,
                "Usually a Table API query should fail with either validation or runtime exception. "
                        + "Otherwise this might require an update to the exception design.");
        this.tableQuery = tableQuery;
        this.sinkName = sinkName;
        this.expectedException = expectedException;
        this.expectedErrorMessage = expectedErrorMessage;
    }

    @Override
    public TestKind getKind() {
        return TestKind.FAILING_TABLE_API;
    }

    public Table toTable(TableEnvironment env) {
        return tableQuery.apply(
                new TableEnvAccessor() {
                    @Override
                    public Table from(String path) {
                        return env.from(path);
                    }

                    @Override
                    public Table fromCall(String path, Object... arguments) {
                        return env.fromCall(path, arguments);
                    }

                    @Override
                    public Table fromCall(
                            Class<? extends UserDefinedFunction> function, Object... arguments) {
                        return env.fromCall(function, arguments);
                    }

                    @Override
                    public Table fromValues(Object... values) {
                        return env.fromValues(values);
                    }

                    @Override
                    public Table fromValues(AbstractDataType<?> dataType, Object... values) {
                        return env.fromValues(dataType, values);
                    }

                    @Override
                    public Table sqlQuery(String query) {
                        return env.sqlQuery(query);
                    }

                    @Override
                    public Model fromModel(String modelPath) {
                        return env.fromModel(modelPath);
                    }

                    @Override
                    public Model from(ModelDescriptor modelDescriptor) {
                        return env.fromModel(modelDescriptor);
                    }
                });
    }

    public void apply(TableEnvironment env) {
        assertThatThrownBy(
                        () -> {
                            final Table table = toTable(env);
                            table.executeInsert(sinkName).await();
                        })
                .satisfies(anyCauseMatches(expectedException, expectedErrorMessage));
    }

    public void applyAsSql(TableEnvironment env) {
        assertThatThrownBy(
                        () -> {
                            final Table table = toTable(env);
                            final String query =
                                    table.getQueryOperation()
                                            .asSerializableString(DefaultSqlFactory.INSTANCE);
                            env.executeSql(String.format("INSERT INTO %s %s", sinkName, query))
                                    .await();
                        })
                .satisfies(anyCauseMatches(expectedException, expectedErrorMessage));
    }
}
