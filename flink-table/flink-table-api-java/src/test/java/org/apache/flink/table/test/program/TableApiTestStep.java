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

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.types.AbstractDataType;

import java.util.function.Function;

/** Test step for execution of a Table API. Similar to {@link SqlTestStep}. */
public class TableApiTestStep implements TestStep {
    private final Function<TableEnvAccessor, Table> tableQuery;
    private final String sinkName;

    TableApiTestStep(Function<TableEnvAccessor, Table> tableQuery, String sinkName) {
        this.tableQuery = tableQuery;
        this.sinkName = sinkName;
    }

    @Override
    public TestKind getKind() {
        return TestKind.TABLE_API;
    }

    public Table toTable(TableEnvironment env) {
        return tableQuery.apply(
                new TableEnvAccessor() {
                    @Override
                    public Table from(String path) {
                        return env.from(path);
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
                });
    }

    public TableResult apply(TableEnvironment env) {
        final Table table = toTable(env);
        return table.executeInsert(sinkName);
    }

    public TableResult applyAsSql(TableEnvironment env) {
        final Table table = toTable(env);
        final String query = table.getQueryOperation().asSerializableString();
        return env.executeSql(String.format("INSERT INTO %s %s", sinkName, query));
    }

    /**
     * An interface for starting a {@link Table}. It abstracts away the {@link TableEnvironment}.
     */
    public interface TableEnvAccessor {
        /** See {@link TableEnvironment#from(String)}. */
        Table from(String path);

        /** See {@link TableEnvironment#fromValues(Object...)}. */
        Table fromValues(Object... values);

        /** See {@link TableEnvironment#fromValues(AbstractDataType, Object...)}. */
        Table fromValues(AbstractDataType<?> dataType, Object... values);

        /** See {@link TableEnvironment#sqlQuery(String)}. */
        Table sqlQuery(String query);
    }
}
