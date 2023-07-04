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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
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

    private static final String SYSTEM_DATABASE_NAME = "system";

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();
        CatalogWithBuildInProcedure procedureCatalog =
                new CatalogWithBuildInProcedure("procedure_catalog");
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
                .isEqualTo(
                        "[+I[generate_n], +I[generate_user], +I[get_year], +I[miss_procedure_context], +I[sum_n]]");

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
        assertThat(rows.toString())
                .isEqualTo("[+I[get_year], +I[miss_procedure_context], +I[sum_n]]");

        // show procedure with not ilike
        rows =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql("show procedures in `system` not ilike 'generaTe%'")
                                .collect());
        assertThat(rows.toString())
                .isEqualTo("[+I[get_year], +I[miss_procedure_context], +I[sum_n]]");
    }

    /** A catalog with some built-in procedures for test purpose. */
    private static class CatalogWithBuildInProcedure extends GenericInMemoryCatalog {
        public CatalogWithBuildInProcedure(String name) {
            super(name);
        }

        @Override
        public List<String> listProcedures(String dbName)
                throws DatabaseNotExistException, CatalogException {
            if (!databaseExists(dbName)) {
                throw new DatabaseNotExistException(getName(), dbName);
            }
            if (dbName.equals(SYSTEM_DATABASE_NAME)) {
                return Arrays.asList(
                        "generate_n",
                        "sum_n",
                        "get_year",
                        "generate_user",
                        "miss_procedure_context");
            } else {
                return Collections.emptyList();
            }
        }
    }
}
