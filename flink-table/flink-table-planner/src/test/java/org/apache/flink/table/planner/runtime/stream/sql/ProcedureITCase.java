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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.planner.factories.TestProcedureCatalogFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT Case for statements related to procedure. */
public class ProcedureITCase extends StreamingTestBase {

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();
        TestProcedureCatalogFactory.CatalogWithBuiltInProcedure procedureCatalog =
                new TestProcedureCatalogFactory.CatalogWithBuiltInProcedure("procedure_catalog");
        procedureCatalog.createDatabase(
                "system", new CatalogDatabaseImpl(Collections.emptyMap(), null), true);
        tEnv().registerCatalog("test_p", procedureCatalog);
        tEnv().useCatalog("test_p");
    }

    @Test
    void testShowProcedures() {
        List<Row> rows =
                CollectionUtil.iteratorToList(tEnv().executeSql("show procedures").collect());
        assertThat(rows).isEmpty();

        // should throw exception since the database(`db1`) to show from doesn't
        // exist
        assertThatThrownBy(() -> tEnv().executeSql("show procedures in `db1`"))
                .isInstanceOf(TableException.class)
                .hasMessage(
                        "Fail to show procedures because the Database `db1` to show from/in does not exist in Catalog `test_p`.");

        // show procedure with specifying catalog & database, but the catalog haven't implemented
        // the
        // interface to list procedure
        assertThatThrownBy(
                        () ->
                                tEnv().executeSql(
                                                "show procedures in default_catalog.default_catalog"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "listProcedures is not implemented for class org.apache.flink.table.catalog.GenericInMemoryCatalog.");

        // show procedure in system database
        rows =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql("show procedures in `system`").collect());
        assertThat(rows.toString())
                .isEqualTo("[+I[generate_n], +I[generate_user], +I[get_year], +I[sum_n]]");

        // show procedure with like
        rows =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql("show procedures in `system` like 'generate%'")
                                .collect());
        assertThat(rows.toString()).isEqualTo("[+I[generate_n], +I[generate_user]]");
        rows =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql("show procedures in `system` like 'gEnerate%'")
                                .collect());
        assertThat(rows).isEmpty();

        //  show procedure with ilike
        rows =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql("show procedures in `system` ilike 'gEnerate%'")
                                .collect());
        assertThat(rows.toString()).isEqualTo("[+I[generate_n], +I[generate_user]]");

        // show procedure with not like
        rows =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql("show procedures in `system` not like 'generate%'")
                                .collect());
        assertThat(rows.toString()).isEqualTo("[+I[get_year], +I[sum_n]]");

        // show procedure with not ilike
        rows =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql("show procedures in `system` not ilike 'generaTe%'")
                                .collect());
        assertThat(rows.toString()).isEqualTo("[+I[get_year], +I[sum_n]]");
    }

    @Test
    void testCallProcedure() {
        // test call procedure can run a flink job
        TableResult tableResult = tEnv().executeSql("call `system`.generate_n(4)");
        verifyTableResult(
                tableResult,
                Arrays.asList(Row.of(0), Row.of(1), Row.of(2), Row.of(3)),
                ResolvedSchema.of(
                        Column.physical(
                                "result", DataTypes.BIGINT().notNull().bridgedTo(long.class))));

        // call a procedure which will run in batch mode
        tableResult = tEnv().executeSql("call `system`.generate_n(4, 'BATCH')");
        verifyTableResult(
                tableResult,
                Arrays.asList(Row.of(0), Row.of(1), Row.of(2), Row.of(3)),
                ResolvedSchema.of(
                        Column.physical(
                                "result", DataTypes.BIGINT().notNull().bridgedTo(long.class))));
        // check the runtime mode in current env is still streaming
        assertThat(tEnv().getConfig().get(ExecutionOptions.RUNTIME_MODE))
                .isEqualTo(RuntimeExecutionMode.STREAMING);

        // test call procedure with var-args as well as output data type hint
        tableResult = tEnv().executeSql("call `system`.sum_n(5.5, 1.2, 3.3)");
        verifyTableResult(
                tableResult,
                Collections.singletonList(Row.of("10.00", 3)),
                ResolvedSchema.of(
                        Column.physical("sum_value", DataTypes.DECIMAL(10, 2)),
                        Column.physical("count", DataTypes.INT())));

        // test call procedure with timestamp as input
        tableResult =
                tEnv().executeSql(
                                "call `system`.get_year(timestamp '2023-04-22 00:00:00', timestamp '2024-04-22 00:00:00.300')");
        verifyTableResult(
                tableResult,
                Arrays.asList(Row.of(2023), Row.of(2024)),
                ResolvedSchema.of(Column.physical("result", DataTypes.STRING())));

        // test call procedure with pojo as return type
        tableResult = tEnv().executeSql("call `system`.generate_user('yuxia', 18)");
        verifyTableResult(
                tableResult,
                Collections.singletonList(Row.of("yuxia", 18)),
                ResolvedSchema.of(
                        Column.physical("name", DataTypes.STRING()),
                        Column.physical("age", DataTypes.INT().notNull().bridgedTo(int.class))));
    }

    private void verifyTableResult(
            TableResult tableResult, List<Row> expectedResult, ResolvedSchema expectedSchema) {
        assertThat(CollectionUtil.iteratorToList(tableResult.collect()).toString())
                .isEqualTo(expectedResult.toString());
        assertThat(tableResult.getResolvedSchema()).isEqualTo(expectedSchema);
    }
}
