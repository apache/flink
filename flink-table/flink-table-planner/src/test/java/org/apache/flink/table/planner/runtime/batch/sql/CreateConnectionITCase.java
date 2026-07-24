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

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** IT case for CREATE CONNECTION statement. */
class CreateConnectionITCase extends BatchTestBase {

    @Test
    void testCreateTemporaryConnection() {
        tEnv().executeSql(
                        "CREATE TEMPORARY CONNECTION my_conn COMMENT 'hi there' "
                                + "WITH ('k' = 'v')");

        assertThat(catalogManager().getConnection(connectionIdentifier("my_conn")))
                .hasValueSatisfying(
                        connection -> {
                            assertThat(connection.getOptions()).containsOnly(entry("k", "v"));
                            assertThat(connection.getComment()).isEqualTo("hi there");
                        });
    }

    @Test
    void testCreateTemporaryConnectionRejectsDuplicate() {
        tEnv().executeSql("CREATE TEMPORARY CONNECTION my_conn WITH ('k' = 'v1')");

        assertThatThrownBy(
                        () ->
                                tEnv().executeSql(
                                                "CREATE TEMPORARY CONNECTION my_conn WITH ('k' = 'v2')"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Temporary connection");

        tEnv().executeSql("CREATE TEMPORARY CONNECTION IF NOT EXISTS my_conn WITH ('k' = 'v2')");

        assertThat(catalogManager().getConnection(connectionIdentifier("my_conn")))
                .hasValueSatisfying(
                        connection ->
                                assertThat(connection.getOptions()).containsOnly(entry("k", "v1")));
    }

    @Test
    void testCreatePermanentConnectionRejectedWithoutSecretStore() {
        assertThatThrownBy(() -> tEnv().executeSql("CREATE CONNECTION my_conn WITH ('k' = 'v')"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("WritableSecretStore must be configured");
    }

    @Test
    void testShowCreateTemporaryConnection() {
        tEnv().executeSql(
                        "CREATE TEMPORARY CONNECTION my_conn COMMENT 'hi there' "
                                + "WITH ('type' = 'default', 'k' = 'v', 'password' = 'super-secret')");

        List<Row> rows = collectRows("SHOW CREATE CONNECTION my_conn");

        assertThat(rows).hasSize(1);
        String showCreate = (String) rows.get(0).getField(0);
        assertThat(showCreate)
                .contains("CREATE TEMPORARY CONNECTION")
                .contains("`my_conn`")
                .contains("COMMENT 'hi there'")
                .contains("'k' = 'v'")
                .contains("'type' = 'default'")
                .doesNotContain("super-secret")
                .doesNotContain("password")
                .doesNotContain("__flink.encrypted-secret-key__");
    }

    private List<Row> collectRows(String sql) {
        TableResult result = tEnv().executeSql(sql);
        return CollectionUtil.iteratorToList(result.collect());
    }

    private CatalogManager catalogManager() {
        return ((TableEnvironmentInternal) tEnv()).getCatalogManager();
    }

    private ObjectIdentifier connectionIdentifier(String connectionName) {
        CatalogManager catalogManager = catalogManager();
        return ObjectIdentifier.of(
                catalogManager.getCurrentCatalog(),
                catalogManager.getCurrentDatabase(),
                connectionName);
    }
}
